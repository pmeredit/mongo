// Verify sharding-specific audit events.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const kDbName = "sharding_audit";
const kCollName = "test";
const kNamespace = `${kDbName}.${kCollName}`;
const kExtraShardName = "extraShard";
const kUser = "user1";

let setupTest = function(audit, admin) {
    assert.commandWorked(
        admin.runCommand({createUser: kUser, pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

    assert(admin.auth({user: kUser, pwd: "pwd"}));

    audit.fastForward();
};

const st = MongoRunner.runShardedClusterAuditLogger({}, {auth: null});
const auditMongos = st.s0.auditSpooler();
const auditShard0 = st.rs0.nodes[0].auditSpooler();
const auditConfig = st.configRS.nodes[0].auditSpooler();

let extraShard = MongoRunner.runMongodAuditLogger({shardsvr: '', replSet: kExtraShardName});
extraShard.adminCommand({replSetInitiate: {}});

const admin = st.s0.getDB("admin");
setupTest(auditMongos, admin);

const test = st.s0.getDB(kDbName);

function validateAuditForFunc({eventType, func, mongosParams, shardsvrParams, configsvrParams}) {
    // As a general rule, configsvrs emit the audit events for sharding. For certain operations,
    // shardsvrs may emit events as well. Somewhat surprisingly, no events are currently emitted on
    // mongos but this may some day be relevant.
    jsTest.log(`Validating audit events for ${eventType}`);
    auditMongos.fastForward();
    auditConfig.fastForward();
    auditShard0.fastForward();

    func();

    if (mongosParams !== undefined) {
        const ret = auditMongos.assertEntryRelaxed(eventType, mongosParams);
        print(`Found event on mongos: ${tojson(ret)}`);
        assert.eq(ret.users[0].user, kUser);
    }

    if (configsvrParams !== undefined) {
        const ret = auditConfig.assertEntryRelaxed(eventType, configsvrParams);
        print(`Found event on configsvr: ${tojson(ret)}`);
        assert.eq(ret.users[0].user, kUser);
    }

    if (shardsvrParams !== undefined) {
        const ret = auditShard0.assertEntryRelaxed(eventType, shardsvrParams);
        print(`Found event on shardsvr: ${tojson(ret)}`);
        assert.eq(ret.users[0].user, kUser);
    }

    auditMongos.assertNoNewEntries(eventType);
    auditConfig.assertNoNewEntries(eventType);
    auditShard0.assertNoNewEntries(eventType);
}

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

const key1 = {
    x: 1
};

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

const key2 = {
    x: 1,
    y: 1
};
validateAuditForFunc({
    eventType: "refineCollectionShardKey",
    func: function() {
        assert.commandWorked(test.getCollection(kCollName).createIndex({x: 1, y: 1}));
        assert.commandWorked(admin.adminCommand({refineCollectionShardKey: kNamespace, key: key2}));
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

MongoRunner.stopMongod(extraShard);
st.stop();
})();
