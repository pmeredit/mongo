/**
 * Test encrypted retryable works
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_non_retryable_commands,
 * assumes_read_preference_unchanged,
 * requires_capped,
 * assumes_unsharded_collection,
 * exclude_from_large_txns,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

function countOplogEntries(primaryConn) {
    var oplog = primaryConn.getDB('local').oplog.rs;

    return oplog.find(({"ns": "admin.$cmd", "o.applyOps.ns": "retry.basic", "op": 'c'})).itcount();
}

function assertRetriedStmtIds(retryResult, expectedStmtIds) {
    assert(retryResult.hasOwnProperty("retriedStmtIds"), "retriedStmtIds expected, but not found");
    const r = retryResult.retriedStmtIds;
    assert(Array.isArray(r));
    assert.eq(r.length, expectedStmtIds.length, "unexpected retriedStmtIds length");
    for (let id of expectedStmtIds) {
        assert(r.includes(id),
               `stmtId ${id} expected but not found in retriedStmtIds: ${tojson(r)}`);
    }
}

// primaryConn = connection to primary of shard in mongos otherwise primaryConn = conn
function runTest(conn, primaryConn) {
    let dbName = 'retry';

    let client = new EncryptedClient(conn, dbName);

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    let edb = client.getDB();

    const lsid = UUID();

    // Test retryable writes for insert
    //
    let command = {
        insert: "basic",
        documents: [{"_id": 1, "first": "mark", "last": "marco"}],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    };
    let result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));

    let oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, 1);

    let retryResult = assert.commandWorked(edb.runCommand(command));

    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assertRetriedStmtIds(retryResult, [2]);
    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 1, 1, 1);

    // Test retryable writes for update
    //
    command = {
        "update": "basic",
        updates: [
            {q: {"last": "marco"}, u: {$set: {"first": "matthew"}}},
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(37)
    };
    result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));

    client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

    let origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand(command));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assertRetriedStmtIds(retryResult, [2]);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));
    client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

    // Test retryable writes for delete
    //
    command = {
        "delete": "basic",
        deletes: [
            {
                q: {"last": "marco"},
                limit: 1,
            },
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(41)
    };
    result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand(command));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assertRetriedStmtIds(retryResult, [0]);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 0, 2, 2);

    // Test retryable writes for findAndModify update
    //
    assert.commandWorked(edb.runCommand(
        {"insert": "basic", documents: [{"_id": 1, "first": "mark", "last": "marco"}]}));

    command = {
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"first": "matthew"}},
        lsid: {id: lsid},
        txnNumber: NumberLong(43)
    };
    result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 2);  // +2 for the insert and findAndModify

    retryResult = assert.commandWorked(edb.runCommand(command));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assert.eq(retryResult.retriedStmtId, 2);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 1, 4, 4);

    // Test retryable writes for findAndModify delete
    //
    command = {
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        remove: true,
        lsid: {id: lsid},
        txnNumber: NumberLong(47)
    };
    result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand(command));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assert.eq(retryResult.retriedStmtId, 0);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 0, 4, 4);

    // Test retryable writes for batched inserts
    //
    command = {
        insert: "basic",
        documents: [
            {"_id": 1, "first": "mark", "last": "marco"},
            {"_id": 2, "first": "marco", "last": "mark"}
        ],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(48)
    };
    result = assert.commandWorked(edb.runCommand(command));
    print(tojson(result));
    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 2);

    retryResult = assert.commandWorked(edb.runCommand(command));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assertRetriedStmtIds(retryResult, [2, 5]);
    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 2, 6, 6);
}

jsTestLog("ReplicaSet: Testing fle2 contention on update");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on update");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s, st.shard0);

    st.stop();
}
