/*
 * Tests a non-PIT replica set restore with LDAP external auth mechanism.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    authAndVerify,
    defaultPwd,
    defaultRole,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

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

    // ReplSetTest.initiate() requires all nodes to be to be authorized to run replSetGetStatus.
    // TODO SERVER-14017: Remove this in favor of using initiate() everywhere.
    rst.initiateWithAnyNodeAsPrimary(
        Object.extend(rst.getReplSetConfig(), {writeConcernMajorityJournalDefault: true}));

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

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        isPit: false,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 6, "i");
    const {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreUtils.copyFilesAndCloseBackup();

    // Update the roles collection after taking backup. This should get truncated by magic restore.
    let admin = primary.getDB("admin");
    assert.commandWorked(admin.runCommand({updateRole: defaultRole, privileges: [], roles: []}));

    const expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
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

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
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
