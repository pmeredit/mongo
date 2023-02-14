/**
 * Kill the parent thread on insert when calling into the txn api.
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */

load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/log.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/curop_helpers.js");

(function() {
'use strict';

// TODO: SERVER-73303 remove when v2 CRUD is implemented
if (isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
    return;
}

const COMMENT_STR = "op_to_kill";

function runContentionTest(db, conn, failpointName, operationName, parallelFunction) {
    // Setup a failpoint that hangs in insert
    let preResponse = assert.commandWorked(
        db.adminCommand({configureFailPoint: failpointName, mode: "alwaysOn"}));
    jsTestLog(preResponse);

    // Start one operation
    let operationOne = startParallelShell(parallelFunction, conn.port);

    const obj = {"command.comment": COMMENT_STR};
    const commandName = "command." + operationName;
    obj[commandName] = "basic";

    // Wait for $currentOp to show we are running the command.
    const ops = waitForCurOpByFilter(db, obj);
    print(tojson(ops));
    const opId = ops[0].opid;

    jsTestLog("killing " + opId);
    db.killOp(opId);

    assert.soon(() => {
        let log = assert.commandWorked(db.adminCommand({getLog: "global"})).log;
        return findMatchingLogLine(log, {id: 20884, attr: `{"opId":${opId}}`});
    });

    // Unblock the operation
    let postResponse =
        assert.commandWorked(db.adminCommand({configureFailPoint: failpointName, mode: "off"}));
    jsTestLog(postResponse);

    // Make sure we hit the failpoint
    assert.gt(postResponse.count, preResponse.count);

    // Wait for the the parallel shell
    operationOne({checkExitSuccess: false});

    sleep(2000);
    jsTestLog("Parallel shell has been killed.");
}

function runTest(conn) {
    let dbName = 'txn_contention_test';
    let db = conn.getDB(dbName);

    let client = new EncryptedClient(db.getMongo(), dbName);
    let edb = client.getDB();

    edb.basic.drop();
    edb.enxcol_.basic.esc.drop();
    edb.enxcol_.basic.ecc.drop();
    edb.enxcol_.basic.ecoc.drop();

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    // Hang insert
    runContentionTest(db, conn, "fleCrudHangPreInsert", "insert", function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_test");
        let edb = client.getDB();
        assert.commandWorked(edb.basic.runCommand({
            insert: edb.basic.getName(),
            documents: [{_id: 1, "first": "mark"}],
            comment: "op_to_kill"
        }));
        print("Parallel insert finished");
    });

    // Make sure insert still works
    assert.soonNoExcept(() => {
        assert.commandWorked(edb.basic.insert({"first": "jack", "fm:": "jack"}));
        return true;
    });

    // Hang update
    runContentionTest(db, conn, "fleCrudHangPreUpdate", "update", function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_test");
        let edb = client.getDB();
        assert.commandWorked(edb.basic.runCommand({
            update: edb.basic.getName(),
            updates: [{q: {_id: 1}, u: {$set: {"first": "marco"}}}],
            comment: "op_to_kill"
        }));
        print("Parallel update finished");
    });

    // Make sure update still works
    assert.soonNoExcept(() => {
        assert.commandWorked(edb.basic.update({_id: 1}, {$set: {"first": "marco"}}));
        return true;
    });

    // Hang find and modify
    runContentionTest(db, conn, "fleCrudHangPreFindAndModify", "findAndModify", function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_test");
        let edb = client.getDB();
        assert.commandWorked(edb.basic.runCommand({
            findAndModify: edb.basic.getName(),
            query: {"_id": 1},
            update: {$set: {"first": "markus"}},
            comment: "op_to_kill"
        }));
        print("Parallel findAndModify finished");
    });

    // Make sure findAndModify still works
    assert.soonNoExcept(() => {
        assert.commandWorked(edb.basic.runCommand({
            findAndModify: edb.basic.getName(),
            query: {"_id": 1},
            update: {$set: {"first": "markus"}},
        }));

        return true;
    });

    // Hang delete
    runContentionTest(db, conn, "fleCrudHangPreDelete", "delete", function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_test");
        let edb = client.getDB();
        assert.commandWorked(edb.basic.runCommand({
            delete: edb.basic.getName(),
            deletes: [
                {
                    q: {"_id": 1},
                    limit: 1,
                },
            ],
            comment: "op_to_kill"
        }));

        print("Parallel insert finished");
    });

    // Make sure it still works
    assert.soonNoExcept(() => {
        assert.commandWorked(edb.basic.deleteOne({_id: 1}));
        return true;
    });
}

jsTestLog("ReplicaSet: Testing fle2 contention on insert");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}
}());
