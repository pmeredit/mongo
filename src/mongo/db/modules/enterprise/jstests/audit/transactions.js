// Verify operations in transactions are sent to audit log.
//
// @tags: [uses_transactions]

(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/audit/audit.js");

    const replSetName = "rs";
    // Start the node with the "replSet" command line option and enable the audit of CRUD ops.
    const mongo = MongoRunner.runMongodAuditLogger(
        {replSet: replSetName, setParameter: "auditAuthorizationSuccess=true"});
    const audit = mongo.auditSpooler();

    const dbName = "test";
    const collName = "transaction_audit";
    const testDB = mongo.getDB(dbName);

    // Initiate the single node replset.
    let config = {_id: replSetName, protocolVersion: 1};
    config.members = [{_id: 0, host: mongo.host}];
    assert.commandWorked(testDB.adminCommand({replSetInitiate: config}));
    // Wait until the single node becomes primary.
    assert.soon(() => testDB.runCommand({ismaster: 1}).ismaster);

    testDB.runCommand({drop: collName, writeConcern: {w: "majority"}});
    assert.commandWorked(testDB.createCollection(collName, {writeConcern: {w: "majority"}}));

    const sessionOptions = {causalConsistency: false};
    const session = mongo.startSession(sessionOptions);
    const sessionDb = session.getDatabase(dbName);
    const sessionColl = sessionDb[collName];

    // Warm up the session so that the following operations don't have to look up
    // the transaction table.
    session.startTransaction();
    assert.eq(sessionColl.find().toArray(), []);
    session.abortTransaction();

    // Test insert and commitTransaction.
    audit.fastForward();
    session.startTransaction();
    const txnDoc = {_id: "txn-insert"};
    assert.commandWorked(sessionColl.insert(txnDoc));
    let entry = audit.getNextEntry();
    assert.eq(entry.atype, "authCheck");
    assert.eq(entry.param.command, "insert");
    assert.eq(entry.param.ns, sessionColl.getFullName());
    assert.eq(entry.param.args.insert, collName);
    assert.docEq(entry.param.args.documents, [txnDoc]);
    assert.eq(entry.param.args.startTransaction, true);
    assert.eq(entry.param.args.autocommit, false);

    session.commitTransaction();
    entry = audit.getNextEntry();
    assert.eq(entry.atype, "authCheck");
    assert.eq(entry.param.command, "commitTransaction");
    assert.eq(entry.param.args.autocommit, false);

    // Test find and abortTransaction.
    audit.fastForward();
    session.startTransaction();
    assert.docEq(sessionColl.find().toArray(), [txnDoc]);

    entry = audit.getNextEntry();
    assert.eq(entry.atype, "authCheck");
    assert.eq(entry.param.command, "find");
    assert.eq(entry.param.ns, sessionColl.getFullName());
    assert.eq(entry.param.args.startTransaction, true);
    assert.eq(entry.param.args.autocommit, false);

    session.abortTransaction();
    entry = audit.getNextEntry();
    assert.eq(entry.atype, "authCheck");
    assert.eq(entry.param.command, "abortTransaction");
    assert.eq(entry.param.args.autocommit, false);

    session.endSession();
    MongoRunner.stopMongod(mongo);
})();
