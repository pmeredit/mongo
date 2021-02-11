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
        assert.eq(entry.roles.length, 1);
        const user = entry.users[0];
        const role = entry.roles[0];
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
        assert.eq(role.role, 'root');
        assert.eq(role.db, 'admin');

        // If the expected result code is specified, check this as well.
        if (expectedResult) {
            assert.eq(expectedResult, entry.result);
        }
    };

    /**
     * Returns a RegExp instance for a drop* namespace.
     * Will match test.collname or test.system.drop.1234i56t78.collname
     * This covers both single-phase and two-phase drop strategies.
     */
    function dropTestCollNSS(dbname, collname) {
        return new RegExp(dbname + '\\.(system\\.(drop\\.\\d+i\\d+t\\d+\\.)?)?' + collname);
    }

    jsTest.log('START audit/ddl-ops.js ' + mode);

    const admin = mongo.getDB("admin");
    const testOne = mongo.getDB('testOne');
    const testTwo = mongo.getDB('testTwo');
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "admin", roles: ["root"]}));
    assert(admin.auth('admin', 'admin'));
    audit.fastForward();

    //// Create Collection (and implicitly Database)

    assert.writeOK(testOne.implicitCollection.insert({x: 1}));
    audit.assertEntryForAdmin('createDatabase', {ns: 'testOne'});
    audit.assertEntryForAdmin('createCollection', {ns: 'testOne.implicitCollection'});
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.implicitCollection',
        indexName: '_id_',
        indexSpec: {v: 2, key: {_id: 1}, name: '_id_'}
    });

    assert.commandWorked(testOne.createCollection('explicitCollection'));
    audit.assertEntryForAdmin('createCollection', {ns: 'testOne.explicitCollection'});
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.explicitCollection',
        indexName: '_id_',
        indexSpec: {v: 2, key: {_id: 1}, name: '_id_'}
    });

    //// Create Index
    assert.commandWorked(testOne.implicitCollection.createIndex({x: 1}));
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.implicitCollection',
        indexName: 'x_1',
        indexSpec: {v: 2, key: {x: 1}, name: 'x_1'}
    });

    assert.commandWorked(testOne.explicitCollection.createIndex({y: 1}));
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.explicitCollection',
        indexName: 'y_1',
        indexSpec: {v: 2, key: {y: 1}, name: 'y_1'}
    });

    assert.commandWorked(testOne.explicitCollection.dropIndex({y: 1}));
    audit.assertEntryForAdmin('dropIndex', {ns: 'testOne.explicitCollection', indexName: 'y_1'});

    //// Create View
    assert.commandWorked(testOne.createView('implicitView', 'implicitCollection', []));
    const expectImplicitViewTestOne = {ns: 'testOne.implicitView'};
    if (improvedAuditingEnabled) {
        expectImplicitViewTestOne.viewOn = 'testOne.implicitCollection';
        expectImplicitViewTestOne.pipeline = [];
    }
    audit.assertEntryForAdmin('createCollection', expectImplicitViewTestOne);

    assert.commandWorked(
        testOne.createView('addZedView', 'implicitCollection', [{'$addFields': {z: 1}}]));
    const expectZedView = {ns: 'testOne.addZedView'};
    if (improvedAuditingEnabled) {
        expectZedView.viewOn = 'testOne.implicitCollection';
        expectZedView.pipeline = [{'$addFields': {z: 1}}];
    }
    audit.assertEntryForAdmin('createCollection', expectZedView);

    assert.commandWorked(testOne.createView('explicitView', 'explicitCollection', []));
    const expectExplicitView = {ns: 'testOne.explicitView'};
    if (improvedAuditingEnabled) {
        expectExplicitView.viewOn = 'testOne.explicitCollection';
        expectExplicitView.pipeline = [];
    }
    audit.assertEntryForAdmin('createCollection', expectExplicitView);

    assert.commandWorked(testTwo.createView('implicitView', 'implicitCollection', []));
    const expectImplicitViewTestTwo = {ns: 'testTwo.implicitView'};
    if (improvedAuditingEnabled) {
        expectImplicitViewTestTwo.viewOn = 'testTwo.implicitCollection';
        expectImplicitViewTestTwo.pipeline = [];
    }
    audit.assertEntryForAdmin('createCollection', expectImplicitViewTestTwo);

    // Drop views
    testOne.explicitView.drop();
    testOne.addZedView.drop();

    if (improvedAuditingEnabled) {
        audit.assertEntryForAdmin('dropCollection', expectExplicitView);
        audit.assertEntryForAdmin('dropCollection', expectZedView);
        // In sharded environments, dropping a collection or view that doesn't exist does not return
        // an error by design, but standalones return NamespaceNotFound. Both scenarios are audited
        // with the NamespaceNotFound error code.
        if (mode == 'Sharded') {
            assert.commandWorked(testOne.runCommand({drop: "nonexistentView"}));
        } else {
            assert.commandFailedWithCode(testOne.runCommand({drop: "nonexistentView"}),
                                         [ErrorCodes.NamespaceNotFound]);
        }
        const expectNamespaceErrorView = {ns: 'testOne.nonexistentView'};
        expectNamespaceErrorView.viewOn = '';
        expectNamespaceErrorView.pipeline = [];
        audit.assertEntryForAdmin(
            'dropCollection', expectNamespaceErrorView, {}, ErrorCodes.NamespaceNotFound);
    }

    //// Drop Collections
    testOne.explicitCollection.drop();
    audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.explicitCollection'});
    audit.assertEntryForAdmin(
        'dropIndex', {ns: dropTestCollNSS('testOne', 'explicitCollection'), indexName: '_id_'});

    //// Rename
    assert.writeOK(testOne.origCollection.insert({x: 1}));
    audit.assertEntryForAdmin('createCollection', {ns: 'testOne.origCollection'});
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.origCollection',
        indexName: '_id_',
        indexSpec: {v: 2, key: {_id: 1}, name: '_id_'}
    });

    assert.commandWorked(testOne.origCollection.renameCollection('newCollection', false));
    audit.assertEntryForAdmin('renameCollection',
                              {old: 'testOne.origCollection', new: 'testOne.newCollection'});

    //// Rename with overwrite
    assert.writeOK(testOne.origCollection.insert({x: 2}));
    audit.assertEntryForAdmin('createCollection', {ns: 'testOne.origCollection'});
    audit.assertEntryForAdmin('createIndex', {
        ns: 'testOne.origCollection',
        indexName: '_id_',
        indexSpec: {v: 2, key: {_id: 1}, name: '_id_'}
    });

    assert.commandWorked(testOne.origCollection.renameCollection('newCollection', true));
    audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.newCollection'});

    {
        // Due to deferred drop via system.drop rename,
        // the order of these two entries is not guaranteed.
        // Rewind partially to allow arbitrary ordering.
        const dropCollPos = audit._auditLine;
        audit.assertEntryForAdmin(
            'dropIndex', {ns: dropTestCollNSS('testOne', 'newCollection'), indexName: '_id_'});
        audit._auditLine = dropCollPos;
        audit.assertEntryForAdmin('renameCollection',
                                  {old: 'testOne.origCollection', new: 'testOne.newCollection'});
    }
    assert.eq(testOne.newCollection.count({}), 1);
    assert.eq(testOne.newCollection.count({x: 2}), 1);

    //// Drop Database
    assert.commandWorked(testOne.dropDatabase());
    if (mode === 'Standalone') {
        // In standalones, dropDatabase is audited first. All views are dropped and audited before
        // the collection drops, and the indexes in the collections being dropped are audited before
        // the collections themselves. However, the order of the collections and their indexes
        // relative to each other is arbitrary, so partially rewind to account for that.
        audit.assertEntryForAdmin('dropDatabase', {ns: 'testOne'});
        if (improvedAuditingEnabled) {
            audit.assertEntryForAdmin('dropCollection', expectImplicitViewTestOne);
        }

        {
            const startLine = audit._auditLine;
            audit.assertEntryForAdmin(
                'dropIndex', {ns: dropTestCollNSS('testOne', 'newCollection'), indexName: '_id_'});
            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.newCollection'});
            audit._auditLine = startLine;

            audit.assertEntryForAdmin(
                'dropIndex',
                {ns: dropTestCollNSS('testOne', 'implicitCollection'), indexName: '_id_'});
            audit.assertEntryForAdmin(
                'dropIndex',
                {ns: dropTestCollNSS('testOne', 'implicitCollection'), indexName: 'x_1'});
            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.implicitCollection'});
            audit._auditLine = startLine;

            audit.assertEntryForAdmin('dropIndex',
                                      {ns: dropTestCollNSS('testOne', 'views'), indexName: '_id_'});
            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.system.views'});
        }
    } else {
        // In sharded environments, the collections are explicitly dropped first (again in arbitrary
        // order). Each collection drop audit precedes the audits for the indexes in that collection
        // being dropped. Views are audited after the `system.views` collection, and dropDatabase is
        // audited at the end.
        {
            const startLine = audit._auditLine;
            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.newCollection'});
            audit.assertEntryForAdmin(
                'dropIndex', {ns: dropTestCollNSS('testOne', 'newCollection'), indexName: '_id_'});
            audit._auditLine = startLine;

            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.implicitCollection'});
            audit.assertEntryForAdmin(
                'dropIndex',
                {ns: dropTestCollNSS('testOne', 'implicitCollection'), indexName: '_id_'});
            audit.assertEntryForAdmin(
                'dropIndex',
                {ns: dropTestCollNSS('testOne', 'implicitCollection'), indexName: 'x_1'});
            audit._auditLine = startLine;

            audit.assertEntryForAdmin('dropCollection', {ns: 'testOne.system.views'});
            audit.assertEntryForAdmin('dropIndex',
                                      {ns: dropTestCollNSS('testOne', 'views'), indexName: '_id_'});
            if (improvedAuditingEnabled) {
                audit.assertEntryForAdmin('dropCollection', expectImplicitViewTestOne);
            }
        }

        audit.assertEntryForAdmin('dropDatabase', {ns: 'testOne'});
    }

    // Since views dropped during dropDatabase are audited by iterating through the ViewCatalog,
    // this simply serves as a sanity check to ensure that the view created on database testTwo
    // isn't audited until testTwo is actually dropped.
    audit.assertNoEntry('dropCollection', expectImplicitViewTestTwo);

    assert.commandWorked(testTwo.dropDatabase());
    if (mode === 'Standalone') {
        audit.assertEntryForAdmin('dropDatabase', {ns: 'testTwo'});
        if (improvedAuditingEnabled) {
            audit.assertEntryForAdmin('dropCollection', expectImplicitViewTestTwo);
        }
    } else {
        if (improvedAuditingEnabled) {
            audit.assertEntryForAdmin('dropCollection', expectImplicitViewTestTwo);
        }
        audit.assertEntryForAdmin('dropDatabase', {ns: 'testTwo'});
    }
}

function unreplicatedNamespaceRegex() {
    return new RegExp('(system.profile)|(local.*)');
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
    jsTest.log('SUCCESS audit/ddl-ops.js Standalone');
}

{
    const primaryOptions = {
        auth: null,
        auditDestination: 'file',
        auditPath: MongoRunner.dataPath + '/set_0_audit.log',
        auditFormat: 'JSON',
    };
    const secondaryOptions = {
        auth: null,
        auditDestination: 'file',
        auditPath: MongoRunner.dataPath + '/set_1_audit.log',
        auditFormat: 'JSON',
    };
    const options = {
        mongos: [{auth: null}],
        config: [{auth: null}],
        shards: 1,
        rs: {
            nodes: [primaryOptions, secondaryOptions],
        },
        keyFile: 'jstests/libs/key1',
    };

    jsTest.log('Starting ShardingTest with options: ' + tojson(options));
    const st = new ShardingTest(options);
    const primaryAudit = new AuditSpooler(options.rs.nodes[0].auditPath, false);
    runTests('Sharded', st.s0, primaryAudit, improvedAuditingEnabled);

    if (improvedAuditingEnabled) {
        const secondaryAudit = new AuditSpooler(options.rs.nodes[1].auditPath, false);
        const ddlAtypes = [
            'createDatabase',
            'createCollection',
            'createIndex',
            'dropDatabase',
            'dropCollection',
            'dropIndex',
            'importCollection',
            'renameCollection'
        ];
        secondaryAudit.assertAllAtypeEntriesRelaxed(ddlAtypes, {ns: unreplicatedNamespaceRegex()});
    }

    jsTest.log('SUCCESS audit/ddl-ops.js Sharded');

    st.stop();
}
})();
