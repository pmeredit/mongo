// Verify that DDL operations are correctly attributed to the initiating user.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

function runTests(mode, mongo, audit, improvedAuditingEnabled) {
    /**
     * Checks that an audit entry was created for the given `atype` and `param`.
     *
     * Ideally, these entries should all be attributed to `admin.admin`,
     * however createIndex is currently unattributed.
     */
    audit.assertEntryForAdmin = function(atype, param, opts = {}, expectedResult) {
        const entry = this.assertEntryRelaxed(atype, param);

        assert.eq(Object.keys(param).length,
                  Object.keys(entry.param).length,
                  'Audit entry has more keys than expected');

        if (opts.expectAttribution === false) {
            assert.eq(entry.users.length, 0);
            return;
        }

        assert.eq(entry.users.length, 1);
        const user = entry.users[0];
        if ((atype === 'dropIndex') && entry.param.ns.startsWith('test.system.drop.')) {
            // Indirect drops (via db.system.drop) are handled out of band
            // from the client connection and thus do not have context of who requested it.
            // In this test, we see this during calls to dropDatabase and dropCollection,
            // however the initiating drop is proprerly attributed, and we do test for this.
            if ((user.db + '.' + user.user) === 'local.__system') {
                // Make just a tiny bit of noise in the log.
                print('WARNING: Misattribution in indirect dropIndex: ' + entry.param.ns);
                return;
            }
        }
        assert.eq(user.db, 'admin');
        assert.eq(user.user, 'admin');

        // If the expected result code is specified, check this as well.
        if (expectedResult) {
            assert.eq(expectedResult, entry.result);
        }
    };

    /**
     * Returns a RegExp instance for a drop* namespace.
     * Will match test.collname or test.system.drop.1234i56t78.collname
     * This covers both single-phase and two-phase drop strageties.
     */
    function dropTestCollNSS(collname) {
        return new RegExp('test\\.(system\\.drop\\.\\d+i\\d+t\\d+\\.)?' + collname);
    }

    jsTest.log('START audit/ddl-ops.js ' + mode);

    const admin = mongo.getDB("admin");
    const test = mongo.getDB('test');
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "admin", roles: ["root"]}));
    assert(admin.auth('admin', 'admin'));
    audit.fastForward();

    //// Create Collection (and implicitly Database)
    assert.writeOK(test.implicitCollection.insert({x: 1}));
    audit.assertEntryForAdmin('createDatabase', {ns: 'test'});
    audit.assertEntryForAdmin('createCollection', {ns: 'test.implicitCollection'});

    assert.commandWorked(test.createCollection('explicitCollection'));
    audit.assertEntryForAdmin('createCollection', {ns: 'test.explicitCollection'});

    //// Create Index
    assert.commandWorked(test.implicitCollection.createIndex({x: 1}));
    // TODO: SERVER-50990 Wire user attribute when auditing createIndex
    const kMissingAttribution = {expectAttribution: false};
    audit.assertEntryForAdmin('createIndex',
                              {
                                  ns: 'test.implicitCollection',
                                  indexName: 'x_1',
                                  indexSpec: {v: 2, key: {x: 1}, name: 'x_1'}
                              },
                              kMissingAttribution);

    // TODO: SERVER-50991 createIndex will not get audited if the collection is empty.
    assert.writeOK(test.explicitCollection.insert({y: 1}));
    assert.commandWorked(test.explicitCollection.createIndex({y: 1}));
    audit.assertEntryForAdmin('createIndex',
                              {
                                  ns: 'test.explicitCollection',
                                  indexName: 'y_1',
                                  indexSpec: {v: 2, key: {y: 1}, name: 'y_1'}
                              },
                              kMissingAttribution);

    assert.commandWorked(test.explicitCollection.dropIndex({y: 1}));
    audit.assertEntryForAdmin('dropIndex', {ns: 'test.explicitCollection', indexName: 'y_1'});

    //// Create View
    assert.commandWorked(test.createView('implicitView', 'implicitCollection', []));
    const expectImplicitView = {ns: 'test.implicitView'};
    if (improvedAuditingEnabled) {
        expectImplicitView.viewOn = 'test.implicitCollection';
        expectImplicitView.pipeline = [];
    }
    audit.assertEntryForAdmin('createCollection', expectImplicitView);
    assert.commandWorked(
        test.createView('addZedView', 'implicitCollection', [{'$addFields': {z: 1}}]));
    const expectZedView = {ns: 'test.addZedView'};
    if (improvedAuditingEnabled) {
        expectZedView.viewOn = 'test.implicitCollection';
        expectZedView.pipeline = [{'$addFields': {z: 1}}];
    }
    audit.assertEntryForAdmin('createCollection', expectZedView);

    assert.commandWorked(test.createView('explicitView', 'explicitCollection', []));
    const expectExplicitView = {ns: 'test.explicitView'};
    if (improvedAuditingEnabled) {
        expectExplicitView.viewOn = 'test.explicitCollection';
        expectExplicitView.pipeline = [];
    }
    audit.assertEntryForAdmin('createCollection', expectExplicitView);

    // Drop views
    test.explicitView.drop();
    audit.assertEntryForAdmin('dropCollection', expectExplicitView);
    test.addZedView.drop();
    audit.assertEntryForAdmin('dropCollection', expectZedView);
    test.implicitView.drop();
    audit.assertEntryForAdmin('dropCollection', expectImplicitView);

    // In sharded environments, dropping a collection or view that doesn't exist does not return an
    // error by design, but standalones return NamespaceNotFound. Both scenarios are audited with
    // the NamespaceNotFound error code.
    if (mode == 'Sharded') {
        assert.commandWorked(test.runCommand({drop: "nonexistentView"}));
    } else {
        assert.commandFailedWithCode(test.runCommand({drop: "nonexistentView"}),
                                     [ErrorCodes.NamespaceNotFound]);
    }
    const expectNamespaceErrorView = {ns: 'test.nonexistentView'};
    if (improvedAuditingEnabled) {
        expectNamespaceErrorView.viewOn = '';
        expectNamespaceErrorView.pipeline = [];
    }
    audit.assertEntryForAdmin(
        'dropCollection', expectNamespaceErrorView, ErrorCodes.NamespaceNotFound);

    //// Drop Collections
    test.implicitCollection.drop();
    audit.assertEntryForAdmin('dropCollection', {ns: 'test.implicitCollection'});
    audit.assertEntryForAdmin('dropIndex',
                              {ns: dropTestCollNSS('implicitCollection'), indexName: '_id_'});
    audit.assertEntryForAdmin('dropIndex',
                              {ns: dropTestCollNSS('implicitCollection'), indexName: 'x_1'});

    test.explicitCollection.drop();
    audit.assertEntryForAdmin('dropCollection', {ns: 'test.explicitCollection'});
    audit.assertEntryForAdmin('dropIndex',
                              {ns: dropTestCollNSS('explicitCollection'), indexName: '_id_'});

    //// Rename
    assert.writeOK(test.origCollection.insert({x: 1}));
    audit.assertEntryForAdmin('createCollection', {ns: 'test.origCollection'});
    assert.commandWorked(test.origCollection.renameCollection('newCollection', false));
    audit.assertEntryForAdmin('renameCollection',
                              {old: 'test.origCollection', new: 'test.newCollection'});

    //// Rename with overwrite
    assert.writeOK(test.origCollection.insert({x: 2}));
    audit.assertEntryForAdmin('createCollection', {ns: 'test.origCollection'});
    assert.commandWorked(test.origCollection.renameCollection('newCollection', true));
    audit.assertEntryForAdmin('dropCollection', {ns: 'test.newCollection'});
    {
        // Due to deferred drop via system.drop rename,
        // the order of these two entries is not guaranteed.
        // Rewind partially to allow arbitrary ordering.
        const dropCollPos = audit._auditLine;
        audit.assertEntryForAdmin('dropIndex',
                                  {ns: dropTestCollNSS('newCollection'), indexName: '_id_'});
        audit._auditLine = dropCollPos;
        audit.assertEntryForAdmin('renameCollection',
                                  {old: 'test.origCollection', new: 'test.newCollection'});
    }
    assert.eq(test.newCollection.count({}), 1);
    assert.eq(test.newCollection.count({x: 2}), 1);

    //// Drop Database
    assert.commandWorked(test.dropDatabase());
    if (mode !== 'Standalone') {
        // TODO: SERVER-50994 Collections dropped during dropDatabase only audit in sharded,
        // not standlone.
        audit.assertEntryForAdmin('dropCollection', {ns: 'test.newCollection'});
        audit.assertEntryForAdmin('dropIndex',
                                  {ns: dropTestCollNSS('newCollection'), indexName: '_id_'});
    }
    audit.assertEntryForAdmin('dropDatabase', {ns: 'test'});

    jsTest.log('SUCCESS audit/ddl-ops.js ' + mode);
}

// Establish whether or not the featureFlag has been enabled during standalone run.
// We *should* do this independently during the sharding run,
// but the feature flags aren't set on mongos.
// Trust that if it's enabled for mongod here, it'll be enabled for mongod there.
let improvedAuditingEnabled = false;
{
    const options = {auth: null};
    jsTest.log('Starting StandaloneTest with options: ' + tojson(options));
    const mongod = MongoRunner.runMongodAuditLogger(options, false);
    improvedAuditingEnabled = isImprovedAuditingEnabled(mongod);
    runTests('Standalone', mongod, mongod.auditSpooler(), improvedAuditingEnabled);
    MongoRunner.stopMongod(mongod);
}

{
    const options = {
        mongos: [{auth: null}],
        config: [{auth: null}],
        shards: [{
            auth: null,
            auditDestination: 'file',
            auditPath: MongoRunner.dataPath + '/shard_audit.log',
            auditFormat: 'JSON',
        }],
        keyFile: 'jstests/libs/key1',
    };

    jsTest.log('Starting ShardingTest with options: ' + tojson(options));
    const st = new ShardingTest(options);
    const audit = new AuditSpooler(options.shards[0].auditPath, false);
    runTests('Sharded', st.s0, audit, improvedAuditingEnabled);
    st.stop();
}
})();
