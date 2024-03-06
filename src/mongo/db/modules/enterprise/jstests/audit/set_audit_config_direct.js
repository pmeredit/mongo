// Invocations of {setAuditConfig: ...}
// @tags: [requires_fcv_49, requires_persistence]

(function() {
'use strict';

load('jstests/libs/feature_flag_util.js');
load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const testParamMongod = MongoRunner.runMongod();
if (FeatureFlagUtil.isEnabled(testParamMongod, "AuditConfigClusterParameter")) {
    jsTest.log(
        "Skipping set_audit_config_direct.js when AuditConfigClusterParameter feature flag is enabled.");
    MongoRunner.stopMongod(testParamMongod);
    return;
}
MongoRunner.stopMongod(testParamMongod);

function assertSameOID(a, b) {
    if (!(a instanceof ObjectId) && (a['$oid'] === undefined)) {
        assert(false, tojson(a) + ' is not an ObjectId');
    }

    if (!(b instanceof ObjectId) && (b['$oid'] === undefined)) {
        assert(false, tojson(b) + ' is not an ObjectId');
    }

    // Normalize ObjectId or {'$oid':...} to a hex string.
    const astr = (a instanceof ObjectId) ? a.valueOf() : a['$oid'];
    const bstr = (b instanceof ObjectId) ? b.valueOf() : b['$oid'];
    assert.eq(astr, bstr, "Objects are inequal: " + tojson(a) + " != " + tojson(b));
}

const kDefaultConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    generation: ObjectId("000000000000000000000000"),
};

class SetAuditConfigFixture {
    constructor(conn) {
        // Primary connection to perform actions on.
        // Either standalone, rsSet primary, or Mongos.
        this.conn = conn;

        // Potentially wait for an action to happen.
        // On standalone there's no replication/syncing to wait for.
        // On ReplSet we need to allow replication to hit the secondaries, then test.
        // On Sharding we need to allow for non-config nodes to poll from config.
        // Make sure to invoke assertion via apply to keep the fixture bound as `this`.
        this.waitFor = (assertion) => assertion.apply(this);

        // Perform extra checks against secondaries and/or non-config nodes.
        this.checkConfig = () => undefined;

        // Assume we start with unconfigured audit filtering.
        this.config = kDefaultConfig;
    }

    /**
     * Returns the set of audit spoolers where we expect to find specific entries.
     * In sharded mode this might be config, shard, or mongos spoolers.
     * On other types, it's just the one and only spooler list.
     */
    selectSpoolers(aType) {
        if (this.spoolers !== undefined) {
            return this.spoolers;
        }

        switch (aType) {
            case 'createUser':
                return this.configSpoolers;
            case 'authenticate':
                return this.mongosSpoolers;
            case 'authCheck':
                return this.shardSpoolers;
            case 'auditConfigure':
                return []
                    .concat(this.mongosSpoolers)
                    .concat(this.shardSpoolers)
                    .concat(this.configSpoolers);
            default:
                throw "I don't know where to find " + aType + " audit entries";
        }
    }

    fastForward(aType = undefined) {
        let spoolers = [];
        if (aType === undefined) {
            // As a side-effect, this selects all spoolers since
            // 'auditConfigure' events happen on all nodes.
            spoolers = this.selectSpoolers('auditConfigure');
        } else {
            spoolers = this.selectSpoolers(aType);
        }

        this.waitFor(() => null);
        spoolers.forEach((spooler) => spooler.fastForward());
    }

    /**
     * Reset audit line on all spooler nodes.
     */
    resetAuditLineAll() {
        let spoolers;

        if (this.spoolers !== undefined) {
            spoolers = this.spoolers;
        } else {
            spoolers = [].concat(this.mongosSpoolers)
                           .concat(this.shardSpoolers)
                           .concat(this.configSpoolers);
        }

        spoolers.forEach(function(audit) {
            audit.resetAuditLine();
        });
    }

    /**
     * Check for an audit event on all spooler nodes.
     * e.g. auditConfigure observed on all replset members
     */
    assertAuditedAll(...args) {
        const spoolers = this.selectSpoolers(args[0]);
        return this.waitFor(function() {
            const matches = [];
            spoolers.forEach(function(audit) {
                const entry = audit.assertEntryRelaxed.apply(audit, args);
                assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));
                matches.push(entry);
            });
            return matches;
        });
    }

    /**
     * Check for an audit entry on any one node.
     * e.g. insert authCheck on write primary
     */
    assertAuditedAny(aType, params) {
        assert(aType !== undefined, "aType required for assertAuditedAny");
        assert.neq(aType, 'auditConfigure', "Use assertAuditedAll with 'auditConfigure'");

        // We fully expect that some nodes will never have an audit entry.
        // If we used the default timeout (5 minutes) on each node
        // then the test runner would timeout.
        //
        // Instead, assert that within enough time to visit each spooler twice plus 30 seconds
        // at least one spooler must succeed, but only block for 5 seconds at a time on
        // any given node.  It is expected to see success within one pass.
        const spoolers = this.selectSpoolers(aType);
        const kTimeoutForSingleMS = 5 * 1000;
        const kTimeoutMarginMS = 30 * 1000;
        const kTimeoutForAnyMS = (spoolers.length * 2 * kTimeoutForSingleMS) + kTimeoutMarginMS;

        this.waitFor(function() {
            assert.soon(function() {
                return spoolers.some(function(audit) {
                    try {
                        // Failure is an option...
                        const opts = {runHangAnalyzer: false};
                        const entry = audit.assertEntryRelaxed(
                            aType, params, kTimeoutForSingleMS, undefined, opts);
                        assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));
                        return true;
                    } catch (e) {
                        return false;
                    }
                });
            }, "Could not find audit entry on any node", kTimeoutForAnyMS, undefined);
        });
    }

    /**
     * Check that none of our nodes have a given audit entry.
     */
    assertAuditedNone(...args) {
        const spoolers = this.selectSpoolers(args[0]);
        this.waitFor(function() {
            spoolers.forEach(function(audit) {
                audit.assertNoNewEntries.apply(audit, args);
            });
        });
    }

    /**
     * Update the audit config and assert that the correct audit entry is generated
     * with the next larger generation value.
     */
    setAuditConfig(filter, success) {
        jsTest.log('Updating configuration: ' + tojson(filter) + ', ' + tojson(success));
        assert.commandWorked(this.conn.getDB('admin').runCommand(
            {setAuditConfig: 1, filter: filter, auditAuthorizationSuccess: success}));
        const expect = {
            filter: filter,
            auditAuthorizationSuccess: success,
        };
        const next =
            this.assertAuditedAll('auditConfigure', {previous: this.config, config: expect})[0]
                .param.config;
        assert.neq(
            bsonWoCompare(next.generation, this.config.generation), 0, "Generation did not change");
        this.config = next;
        delete this.config._id;

        this.checkConfig();
    }

    /**
     * Worker test.
     */
    runTest() {
        assert(this.conn, "No connection has been set up");

        jsTest.log('BEGIN runTest');
        let admin = this.conn.getDB('admin');
        let test = this.conn.getDB('test');

        // Setup auth db and check for unfiltered authenticate.
        admin.createUser({user: 'admin', pwd: 'admin', roles: ['root']});
        this.assertAuditedAny('createUser', {user: 'admin', db: 'admin'});
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedAny('authenticate', {user: 'admin', db: 'admin'});

        // Filter everything.
        this.setAuditConfig({'atype': 'does-not-exist'}, false);

        // No new entries for auths.
        admin.logout();
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedNone('authenticate', {user: 'admin', db: 'admin'});

        // Filter some entries, but not all.
        this.setAuditConfig({'atype': 'createUser'}, false);
        admin.logout();
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedNone('authenticate', {user: 'admin', db: 'admin'});
        test.createUser({user: 'alice', pwd: 'pwd', roles: []});
        this.assertAuditedAny('createUser', {user: 'alice', db: 'test'});

        // Audit auth checks only.
        // Leave this filter config in place for restart leading into runRestartTest()
        this.setAuditConfig({atype: 'authCheck'}, true);
        assert.writeOK(test.coll.insert({x: 1}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 1}]}});
        assert.writeOK(test.coll.insert({x: 2}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 2}]}});

        jsTest.log('Restarting');
        this.restart(this);
        assert(this.conn, "Failed to restart");
        jsTest.log('Restarted');

        // Reset audit line on all spoolers
        this.resetAuditLineAll();

        // Rebind collections since we have a new connection.
        admin = this.conn.getDB('admin');
        test = this.conn.getDB('test');

        // We should still be in a state without any auditing but auth checks.
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedNone('authenticate');
        assert.writeOK(test.coll.insert({x: 3}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 3}]}});

        // Stop auditing success by deleting the config directly.
        const settings = this.conn.getDB('config').settings;
        assert.writeOK(settings.remove({_id: 'audit'}));
        this.waitFor(function() {
            this.assertAuditedAll('auditConfigure',
                                  {previous: this.config, config: kDefaultConfig});
        });
        this.config = kDefaultConfig;

        assert.writeOK(test.coll.insert({x: 4}));
        this.assertAuditedNone('authCheck', {command: 'insert', args: {documents: [{x: 4}]}});

        // Start filtering by manually inserting a document to the config db.
        const manualAuthSuccessOnlyCheckConfig = {
            filter: {atype: 'authCheck'},
            auditAuthorizationSuccess: true,
            generation: ObjectId(),
        };
        assert.writeOK(
            settings.insert(Object.assign({_id: 'audit'}, manualAuthSuccessOnlyCheckConfig)));
        this.waitFor(function() {
            this.assertAuditedAll(
                'auditConfigure',
                {previous: this.config, config: manualAuthSuccessOnlyCheckConfig});
        });
        this.config = manualAuthSuccessOnlyCheckConfig;

        assert.writeOK(test.coll.insert({x: 5}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 5}]}});

        this.fastForward('authenticate');
        admin.logout();
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedNone('authenticate');

        jsTest.log('END runTest');
    }
}

{
    // Standalone
    jsTest.log('Begin standalone');
    const opts = {
        auth: '',
        auditRuntimeConfiguration: true,
        noCleanData: true,
        dbpath: MongoRunner.dataPath + "standalone",
        port: allocatePort()
    };
    const standalone = new SetAuditConfigFixture(MongoRunner.runMongodAuditLogger(opts));
    standalone.restart = function(fixture) {
        jsTest.log('Restarting standalone');
        MongoRunner.stopMongod(this.conn);
        fixture.conn = MongoRunner.runMongodAuditLogger(opts);
        fixture.spoolers = [standalone.conn.auditSpooler()];
    };
    standalone.spoolers = [standalone.conn.auditSpooler()];

    standalone.runTest();
    MongoRunner.stopMongod(standalone.conn);
    jsTest.log('End standalone');
}

const kKeyFile = 'jstests/libs/key1';
const kKeyData = cat(kKeyFile).replace(/\s/g, '');

// Expects caller to have authenticated.
// Since it might need admin.admin auth or local.__system auth depending on node type.
function checkConfigOnNode(test, node) {
    const admin = node.getDB('admin');
    const generation =
        assert.commandWorked(admin.runCommand({_getAuditConfigGeneration: 1})).generation;
    assertSameOID(generation, test.config.generation);

    const config = assert.commandWorked(admin.runCommand({getAuditConfig: 1}));
    assertSameOID(config.generation, test.config.generation);
    assert.eq(bsonWoCompare(config.filter, test.config.filter),
              0,
              tojson(config.filter) + ' != ' + tojson(test.config.filter));
    assert.eq(config.auditAuthorizationSuccess, test.config.auditAuthorizationSuccess);
}

{
    // ReplicaSets
    jsTest.log('Begin ReplSet');

    const rsOpts = {
        nodes: 3,
        keyFile: kKeyFile,
        nodeOptions: {auditRuntimeConfiguration: true},
    };

    const rs = ReplSetTest.runReplSetAuditLogger(rsOpts);
    rs.awaitSecondaryNodes();
    const replset = new SetAuditConfigFixture(rs.getPrimary());
    replset.waitFor = function(assertion) {
        // Allow replication to complete, then expect success immediately.
        rs.awaitReplication();
        return assertion.apply(replset);
    };
    replset.spoolers = rs.nodes.map((node) => node.auditSpooler());
    replset.checkConfig = function() {
        rs.awaitReplication();
        rs.getSecondaries().forEach(function(node) {
            assert(node.getDB('local').auth('__system', kKeyData));
            checkConfigOnNode(replset, node);
        });
    };
    replset.restart = function() {
        jsTest.log('Restarting ReplSet');
        rs.awaitReplication();
        rs.nodes.forEach((node) => rs.restart(node));
        rs.awaitSecondaryNodes();
        replset.conn = rs.getPrimary();
    };

    replset.runTest();
    rs.stopSet();
    jsTest.log('End ReplSet');
}

{
    // Sharding
    jsTest.log('Begin sharding');
    const kPollingFrequencySecs = 1;

    const opts = {
        other: {
            keyFile: kKeyFile,
        },
    };
    const nodeOpts = {
        auditRuntimeConfiguration: true,
        setParameter: {
            auditConfigPollingFrequencySecs: kPollingFrequencySecs,
        },
    };
    const st = MongoRunner.runShardedClusterAuditLogger(opts, nodeOpts);
    const sharding = new SetAuditConfigFixture(st.s);
    sharding.waitFor = function(assertion) {
        const kInterval = kPollingFrequencySecs * 1000;
        const kTimeout = (kPollingFrequencySecs + 3) * 1000;

        // Use a UUID in error messages to make associating them together unambiguous.
        const kAssertionUUID = UUID();
        let attempt = 1;

        let retval;
        assert.soon(
            function() {
                try {
                    retval = assertion.apply(sharding);
                    return true;
                } catch (e) {
                    print("Assertion " + kAssertionUUID + ", attempt #" + attempt +
                          " failed: " + e);
                    ++attempt;
                    return false;
                }
            },
            "Failing waiting for sharding assertion to succeed, see 'Assertion failed' messages above",
            kTimeout,
            kInterval);
        return retval;
    };
    sharding.configSpoolers = st.configRS.nodes.map((node) => node.auditSpooler());
    sharding.shardSpoolers = st._connections.map((conn) => conn.rs.nodes[0].auditSpooler());
    sharding.mongosSpoolers = st._mongos.map((node) => node.auditSpooler());
    sharding.checkConfig = function() {
        sharding.waitFor(function() {
            st.forEachMongos(function(node) {
                assert(node.getDB('admin').auth('admin', 'admin'));
                checkConfigOnNode(sharding, node);
            });
            st.forEachConnection(function(node) {
                // Shards don't have login DB, so use cluster auth.
                assert(node.getDB('local').auth('__system', kKeyData));
                checkConfigOnNode(sharding, node);
            });
        });
    };
    sharding.restart = function() {
        jsTest.log('Restarting sharding');
        st.configRS.nodes.forEach(n => st.restartConfigServer(n));
        Object.keys(st._connections).forEach((n) => st.restartShardRS(n));
        Object.keys(st._mongos).forEach((n) => st.restartMongos(n));
        sharding.conn = st.s;
    };

    sharding.runTest();
    st.stop();
    jsTest.log('End sharding');
}
})();
