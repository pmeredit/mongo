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

function runUpdateRetryWithPreimageRemovedTest(conn, primaryConn) {
    jsTestLog("Running Test: runUpdateRetryWithPreimageRemovedTest");
    let dbName = 'retryUpdateWithPreimageRemoved';
    let collName = 'basic';
    let client = new EncryptedClient(conn, dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    let edb = client.getDB();
    const lsid = UUID();

    assert.commandWorked(edb.runCommand({
        insert: collName,
        documents: [{"_id": 1, "first": "mark", "last": "marco"}],
        lsid: {id: lsid},
        txnNumber: NumberLong(1)
    }));
    client.assertEncryptedCollectionCounts(collName, 1, 1, 1);

    let command = {
        update: collName,
        updates: [
            {q: {"_id": 1}, u: {$set: {"first": "matthew"}}},
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(20)
    };
    let result = assert.commandWorked(edb.runCommand(command));
    let retryResult = assert.commandWorked(edb.runCommand(command));

    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assertRetriedStmtIds(retryResult, [2]);
    client.assertEncryptedCollectionCounts(collName, 1, 2, 2);

    assert.commandWorked(edb.runCommand({delete: collName, deletes: [{q: {_id: 1}, limit: 1}]}));
    client.assertEncryptedCollectionCounts(collName, 0, 2, 2);

    let oplogCount = countOplogEntries(primaryConn);
    let secondRetryResult = assert.commandWorked(edb.runCommand(command));

    // Assert we get the same response even if the original document was removed
    assert.eq(secondRetryResult.ok, retryResult.ok);
    assert.eq(secondRetryResult.n, retryResult.n);
    assert.eq(secondRetryResult.writeErrors, retryResult.writeErrors);
    assert.eq(secondRetryResult.writeConcernErrors, retryResult.writeConcernErrors);
    assertRetriedStmtIds(secondRetryResult, retryResult.retriedStmtIds);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));
}

function runFindAndModifyRetryWithPreimageRemovedTest(conn, primaryConn) {
    jsTestLog("Running Test: runFindAndModifyRetryWithPreimageRemovedTest");
    let dbName = 'retryFindAndModifyWithPreimageRemoved';
    let collName = 'basic';
    let client = new EncryptedClient(conn, dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    let edb = client.getDB();
    const lsid = UUID();

    let command = {
        insert: collName,
        documents: [{"_id": 1, "first": "mark", "last": "marco"}],
        lsid: {id: lsid},
        txnNumber: NumberLong(1)
    };
    assert.commandWorked(edb.runCommand(command));
    client.assertEncryptedCollectionCounts(collName, 1, 1, 1);

    command = {
        findAndModify: collName,
        query: {_id: 1},
        update: {$set: {"first": "matthew"}},
        lsid: {id: lsid},
        txnNumber: NumberLong(20)
    };
    let result = assert.commandWorked(edb.runCommand(command));
    let retryResult = assert.commandWorked(edb.runCommand(command));

    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(retryResult.retriedStmtId, 2);
    client.assertEncryptedCollectionCounts(collName, 1, 2, 2);

    assert.commandWorked(edb.runCommand({delete: collName, deletes: [{q: {_id: 1}, limit: 1}]}));
    client.assertEncryptedCollectionCounts(collName, 0, 2, 2);

    let oplogCount = countOplogEntries(primaryConn);
    let secondRetryResult = assert.commandWorked(edb.runCommand(command));

    // Assert we get the same response even if the original document was removed
    assert.eq(secondRetryResult.ok, retryResult.ok);
    assert.eq(secondRetryResult.n, retryResult.n);
    assert.eq(secondRetryResult.writeErrors, retryResult.writeErrors);
    assert.eq(secondRetryResult.writeConcernErrors, retryResult.writeConcernErrors);
    assert.eq(secondRetryResult.retriedStmtId, retryResult.retriedStmtId);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));
}

jsTestLog("ReplicaSet: Testing fle2 contention on update");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    runUpdateRetryWithPreimageRemovedTest(rst.getPrimary(), rst.getPrimary());
    runFindAndModifyRetryWithPreimageRemovedTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on update");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s, st.shard0);
    runUpdateRetryWithPreimageRemovedTest(st.s, st.shard0);
    runFindAndModifyRetryWithPreimageRemovedTest(st.s, st.shard0);

    st.stop();
}
