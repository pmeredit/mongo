/**
 * Test error handling in fle2 sharded clusters
 *
 * This test only runs against shared clusters because it allow us to setua failpoint on
 * getQueryableEncryptionCountInfo which is only called in sharded clusters. In repl sets, the call
 * is inlined.
 *
 * @tags: [
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

function testTxnTransient(session, func) {
    try {
        jsTest.log("WriteCommandError should have error labels inside transactions.");
        session.startTransaction();

        let res = func();
        print("RESULT: " + tojson(res));

        assert.eq(res.errorLabels, ["TransientTransactionError"]);

    } finally {
        session.abortTransaction_forTesting();
    }
}

function runTest(mongosConn, shardConn) {
    const dbName = 'fle_sharded_errors';
    const client = new EncryptedClient(mongosConn, dbName);

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields:
            {"fields": [{"path": "a", "bsonType": "string", "queries": {"queryType": "equality"}}]}
    }));

    assert.commandWorked(
        mongosConn.adminCommand({shardCollection: 'txn_sharded.basic', key: {b: 'hashed'}}));
    const edb = mongosConn.getDB(dbName);

    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB.getCollection("basic");

    let res = edb.basic.einsert({_id: "foo", a: "foo"});
    assert.commandWorked(res);

    jsTest.log("failCommand should be able to return errors with TransientTransactionError");

    // Any error code will do that is a TransientTransactionError and does not require extraInfo.
    // SnapshotTooOld is just one possibility
    assert.commandWorked(shardConn.adminCommand({
        configureFailPoint: "failCommand",
        mode: "alwaysOn",
        data: {
            errorCode: ErrorCodes.SnapshotTooOld,
            failCommands: ["getQueryableEncryptionCountInfo"],
            failInternalCommands: true
        }
    }));

    testTxnTransient(session, () => { return sessionColl.einsert({_id: "bar", a: "bar"}); });

    testTxnTransient(session, () => { return sessionColl.einsert({_id: "bar", a: "bar"}); });

    testTxnTransient(session, () => {
        return sessionColl.erunCommand(
            {"update": "basic", updates: [{q: {_id: "foo"}, u: {$set: {a: "bar2"}}}]});
    });

    testTxnTransient(session, () => {
        return sessionColl.erunCommand({
            "delete": "basic",
            deletes: [{
                q: {a: "foobar"},
                limit: 1,

            }]
        });
    });

    assert.commandWorked(shardConn.adminCommand({configureFailPoint: "failCommand", mode: "off"}));
}

jsTestLog("Sharding: Testing fle2 errors");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s, st.shard0);

    st.stop();
}
