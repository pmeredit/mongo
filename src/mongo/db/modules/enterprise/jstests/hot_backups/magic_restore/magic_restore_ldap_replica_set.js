/*
 * Tests a non-PIT replica set restore with LDAP external auth mechanism.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    authAndVerify,
    defaultPwd,
    defaultRole,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

// LDAP AUTH TESTS.

function testLDAP(insertHigherTermOplogEntry) {
let configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];

    function siteRootAdminExists(node) {
        assert.soon(() => {
            const admin = node.getDB("admin");
            const res = admin.auth("siteRootAdmin", "secret");
            admin.logout();
            return res;
        }, "cannot authenticate on replica set node " + node.host);
    }

    const mongodConfig = configGenerator.generateMongodConfig();
    mongodConfig.replSet = "ldapAuthzReplset";

    const config = {
        name: "ldapAuthzReplset",
        nodes: {n0: mongodConfig},
        useHostName: true,
        waitForKeys: false
    };

    const rst = new ReplSetTest(config);
    rst.startSet();
    rst.initiate(Object.extend(rst.getReplSetConfig(), {
        writeConcernMajorityJournalDefault: true,
    }),
                 null,
                 {allNodesAuthorizedToRunRSGetStatus: false});

    let primary = rst.getPrimary();
    setupTest(primary);
    siteRootAdminExists(primary);

    const user1 = "ldapz_ldap1";
    const user2 = "ldapz_ldap2";

    const authOptions = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
    authAndVerify({conn: primary, options: {authOptions: authOptions, user: user1}});
    authOptions.user = user2;
    authAndVerify({conn: primary, options: {authOptions: authOptions, user: user2}});

    assert(primary.getDB("admin").auth("siteRootAdmin", "secret"));

    const dbName = "db";
    const coll = "coll";

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    const expectedDocs = db.getCollection(coll).find().toArray();

    // Making sure that approved documents in config.clusterParameters collection are restored
    // properly.
    assert.commandWorked(primary.getDB("admin").runCommand(
        {setClusterParameter: {defaultMaxTimeMS: {readOperations: 1}}}));
    assert.commandWorked(primary.getDB("admin").adminCommand(
        {setQuerySettings: {find: coll, $db: dbName, filter: {a: 15}}, settings: {reject: true}}));

    primary.getDB("config").getCollection("clusterParameters").insert({
        _id: "internalSearchOptions"
    });

    assert.gt(primary.getDB("config").getCollection("clusterParameters").find().toArray().length,
              2);

    const magicRestoreUtils = new MagicRestoreUtils({
        rst: rst,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreUtils.assertOplogCountForNamespace(primary, {ns: dbName + "." + coll, op: "i"}, 6);
    const {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreUtils.copyFilesAndCloseBackup();

    // Update the roles collection after taking backup. This should get truncated by magic restore.
    let admin = primary.getDB("admin");
    assert.commandWorked(admin.runCommand({updateRole: defaultRole, privileges: [], roles: []}));

    const expectedConfig = magicRestoreUtils.getExpectedConfig();
    rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": magicRestoreUtils.getCheckpointTimestamp(),
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});

    // Restart the original replica set. We need to skip stepup on restart because that will call
    // serverStatus which requires auth.
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath()},
                 true /* restart */,
                 true /* skipStepUpOnRestart */);

    primary = rst.getPrimary();

    authOptions.user = user1;
    authAndVerify({conn: primary, options: {authOptions: authOptions, user: user1}});
    authOptions.user = user2;
    authAndVerify({conn: primary, options: {authOptions: authOptions, user: user2}});

    admin = primary.getDB("admin");
    admin.auth("siteRootAdmin", "secret");

    assert.eq(primary.getDB("config").getCollection("clusterParameters").find().toArray().length,
              2);

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 3,
        opFilter: "i",
        expectedNumDocsSnapshot: 3,
    });

    rst.stopSet();
    configGenerator.stopMockupServer();
}

for (const insertHigherTermOplogEntry of [false, true]) {
    testLDAP(insertHigherTermOplogEntry);
}
