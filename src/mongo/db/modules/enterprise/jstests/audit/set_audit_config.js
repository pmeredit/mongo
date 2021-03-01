// Invocations of {setAuditConfig: ...}
// @tags: [requires_fcv_49]

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

if (!TestData.setParameters.featureFlagRuntimeAuditConfig) {
    jsTest.log('Skipping, feature flag is not enabled');
    return;
}

const kDefaultConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    generation: ObjectId("000000000000000000000000"),
};

class SetAuditConfigFixture {
    constructor(conn) {
        this.conn = conn;
        this.waitfunc = () => undefined;
        this.config = kDefaultConfig;
    }

    /**
     * Check for an audit event on all spooler nodes.
     * e.g. auditConfigure observed on all replset members
     */
    assertAuditedAll(...args) {
        this.waitfunc();
        this.spoolers.forEach(function(audit) {
            const entry = audit.assertEntryRelaxed.apply(audit, args);
            assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));
        });
    }

    /**
     * Check for an audit entry on any one node.
     * e.g. insert authCheck on write primary
     */
    assertAuditedAny(aType, params) {
        assert(aType !== undefined, "One arg required for assertAuditedAny");
        assert.neq(aType, 'auditConfigure', "Use assertAuditedAll with 'auditConfigure");

        this.waitfunc();
        const opts = {runHangAnalyzer: false};
        assert(this.spoolers.some(function(audit) {
            try {
                // Failure is an option...
                const entry = audit.assertEntryRelaxed(aType, params, undefined, undefined, opts);
                assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));
                return true;
            } catch (e) {
                return false;
            }
        }),
               "Could not find audit entry on any node");
    }

    /**
     * Check that none of our nodes have a given audit entry.
     */
    assertAuditedNone(...args) {
        this.waitfunc();
        this.spoolers.forEach(function(audit) {
            audit.assertNoNewEntries.apply(audit, args);
        });
    }

    /**
     * Update the audit config and assert that the correct audit entry is generated
     * with the next larger generation value.
     */
    setAuditConfig(filter, success) {
        assert.commandWorked(this.conn.getDB('admin').runCommand(
            {setAuditConfig: 1, filter: filter, auditAuthorizationSuccess: success}));
        const next = {
            filter: filter,
            auditAuthorizationSuccess: success,
        };
        this.assertAuditedAll('auditConfigure', {previous: this.config, config: next});
        this.config = next;
    }

    /**
     * Worker test.
     */
    runTest() {
        assert(this.conn, "No connection has been set up");
        assert(this.spoolers, "No auditing spoolers defined");

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

        // Rebind collections since we have a new connection.
        admin = this.conn.getDB('admin');
        test = this.conn.getDB('test');
        this.spoolers.forEach((spooler) => spooler.fastForward());

        // We should still be in a state without any auditing but auth checks.
        assert(admin.auth('admin', 'admin'));
        this.assertAuditedNone('authenticate');
        assert.writeOK(test.coll.insert({x: 3}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 3}]}});

        // Stop auditing success by deleting the config directly.
        const settings = this.conn.getDB('config').settings;
        assert.writeOK(settings.remove({_id: 'audit'}));
        this.waitfunc();
        this.assertAuditedAll('auditConfigure', {previous: this.config, config: kDefaultConfig});
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
        this.waitfunc();
        this.assertAuditedAll('auditConfigure',
                              {previous: this.config, config: manualAuthSuccessOnlyCheckConfig});
        this.config = manualAuthSuccessOnlyCheckConfig;

        assert.writeOK(test.coll.insert({x: 5}));
        this.assertAuditedAny('authCheck', {command: 'insert', args: {documents: [{x: 5}]}});

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

{
    // ReplicaSets
    jsTest.log('Begin ReplSet');

    const rsOpts = {
        nodes: 3,
        keyFile: 'jstests/libs/key1',
        nodeOptions: {auditRuntimeConfiguration: true},
    };

    const rs = ReplSetTest.runReplSetAuditLogger(rsOpts);
    rs.awaitSecondaryNodes();
    const replset = new SetAuditConfigFixture(rs.getPrimary());
    replset.waitfunc = () => rs.awaitReplication;
    replset.spoolers = rs.nodes.map((node) => node.auditSpooler());
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
    // TODO(SERVER-54974): Add Periodic Job for polling config servers.
}
})();
