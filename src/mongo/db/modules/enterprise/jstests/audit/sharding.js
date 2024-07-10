// Verify sharding-specific audit events.

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kDiscovery = 5;
const kDeviceConfigState = 5002;
const kDeviceConfigStateLog = 1;

const kDbName = "sharding_audit";
const kCollName = "test";
const kNamespace = `${kDbName}.${kCollName}`;
const kExtraShardName = "extraShard";
const kUser = "user1";
const kActor = `admin.${kUser}`;

const kUserTypeIdRegularUser = 1;
const kUserTypeIdSystemUser = 3;

function setupTest(audit, admin) {
    assert.commandWorked(
        admin.runCommand({createUser: kUser, pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

    assert(admin.auth({user: kUser, pwd: "pwd"}));

    audit.fastForward();
}

function runTest(st, extraShard) {
    const auditMongos = st.s0.auditSpooler();
    const auditShard0 = st.rs0.nodes[0].auditSpooler();
    const auditConfig = st.configRS.nodes[0].auditSpooler();

    function validateAuditForFunc(
        {eventType, func, mongosParams, shardsvrParams, configsvrParams}) {
        // As a general rule, configsvrs emit the audit events for sharding. For certain operations,
        // shardsvrs may emit events as well. Somewhat surprisingly, no events are currently emitted
        // on mongos but this may some day be relevant.
        jsTest.log(`Validating audit events for ${eventType}`);
        auditMongos.fastForward();
        auditConfig.fastForward();
        auditShard0.fastForward();

        func();

        function doAssert(spooler, eventType, params) {
            if (params === undefined) {
                return;
            }

            if (spooler.schema === 'ocsf') {
                const ret =
                    spooler.assertEntry(kDiscovery, kDeviceConfigState, kDeviceConfigStateLog);
                jsTest.log(ret);
                assert(spooler.deepPartialEquals(ret.unmapped,
                                                 Object.assign({atype: eventType}, params)));
                if (ret.actor.user.type_id == kUserTypeIdRegularUser) {
                    assert.eq(ret.actor.user.name, kActor);
                } else {
                    assert.eq(ret.actor.user.type_id, kUserTypeIdSystemUser);
                }
            } else {
                assert.eq(spooler.schema, 'mongo');
                const ret = spooler.assertEntryRelaxed(eventType, params);
                jsTest.log(ret);
                assert.eq(ret.users[0].user, kUser);
                spooler.assertNoNewEntries(eventType);
            }
        }

        doAssert(auditMongos, eventType, mongosParams);
        doAssert(auditConfig, eventType, configsvrParams);
        doAssert(auditShard0, eventType, shardsvrParams);
    }

    extraShard.adminCommand({replSetInitiate: {}});

    const admin = st.s0.getDB("admin");
    setupTest(auditMongos, admin);

    const test = st.s0.getDB(kDbName);

    validateAuditForFunc({
        eventType: "enableSharding",
        func: function() {
            assert.commandWorked(admin.adminCommand({enableSharding: kDbName}));
        },
        configsvrParams: {ns: kDbName}
    });

    const extraShardConnString = `${kExtraShardName}/${extraShard.host}`;
    validateAuditForFunc({
        eventType: "addShard",
        func: function() {
            assert.commandWorked(
                admin.adminCommand({addShard: extraShardConnString, name: kExtraShardName}));
        },
        configsvrParams: {shard: kExtraShardName, connectionString: extraShardConnString}
    });

    const key1 = {x: 1};

    validateAuditForFunc({
        eventType: "shardCollection",
        func: function() {
            assert.commandWorked(test.getCollection(kCollName).createIndex(key1));
            assert.commandWorked(test.getCollection(kCollName).insert({x: 1, y: 2}));
            assert.commandWorked(test.getCollection(kCollName).insert({x: 2, y: 2}));

            assert.commandWorked(admin.adminCommand({shardCollection: kNamespace, key: key1}));
        },
        // Oddly, we generally audit shardCollection on the individual shards.
        shardsvrParams: {
            ns: kNamespace,
            key: key1,
            options: {
                unique: false,
            }
        }
    });

    const key2 = {x: 1, y: 1};
    validateAuditForFunc({
        eventType: "refineCollectionShardKey",
        func: function() {
            assert.commandWorked(test.getCollection(kCollName).createIndex({x: 1, y: 1}));
            assert.commandWorked(
                admin.adminCommand({refineCollectionShardKey: kNamespace, key: key2}));
        },
        configsvrParams: {ns: kNamespace, key: key2}
    });

    validateAuditForFunc({
        eventType: "removeShard",
        func: function() {
            // Drop the db to make removing the shard easier.
            assert.commandWorked(test.dropDatabase());
            assert.commandWorked(admin.adminCommand({removeShard: kExtraShardName}));
        },
        configsvrParams: {shard: kExtraShardName}
    });
}

{
    jsTest.log('schema === mongo');
    const st = MongoRunner.runShardedClusterAuditLogger({}, {auth: null});
    const extraShard = MongoRunner.runMongodAuditLogger({shardsvr: '', replSet: kExtraShardName});
    runTest(st, extraShard);
    MongoRunner.stopMongod(extraShard);
    st.stop();
}

{
    jsTest.log('schema === ocsf');
    const st = MongoRunner.runShardedClusterAuditLogger({}, {auth: null}, 'JSON', 'ocsf');
    const extraShard =
        MongoRunner.runMongodAuditLogger({shardsvr: '', replSet: kExtraShardName}, 'JSON', 'ocsf');
    runTest(st, extraShard);
    MongoRunner.stopMongod(extraShard);
    st.stop();
}
