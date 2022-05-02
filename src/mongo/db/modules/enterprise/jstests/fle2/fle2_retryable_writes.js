/**
 * Test encrypted retryable works
 *
 * @tags: [
 * requires_fcv_60
 * assumes_unsharded_collection,
 * requires_non_retryable_commands,
 * assumes_read_preference_unchanged,
 * requires_capped,
 * assumes_unsharded_collection,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

function countOplogEntries(primaryConn) {
    var oplog = primaryConn.getDB('local').oplog.rs;

    return oplog.find(({"ns": "admin.$cmd", "o.applyOps.ns": "retry.basic", "op": 'c'})).itcount();
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
    let result = assert.commandWorked(edb.runCommand({
        "insert": "basic",
        documents: [{"_id": 1, "first": "mark", "last": "marco"}],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    }));
    print(tojson(result));

    let oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, 1);

    let retryResult = assert.commandWorked(edb.runCommand({
        "insert": "basic",
        documents: [{"_id": 1, "first": "mark", "last": "marco"}],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    }));

    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 1, 1, 0, 1);

    // Test retryable writes for update
    //
    result = assert.commandWorked(edb.runCommand({
        "update": "basic",
        updates: [
            {q: {"last": "marco"}, u: {$set: {"first": "matthew"}}},
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(37)
    }));
    print(tojson(result));

    client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

    let origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand({
        "update": "basic",
        updates: [
            {q: {"last": "marco"}, u: {$set: {"first": "matthew"}}},
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(37)
    }));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));
    client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

    // Test retryable writes for delete
    //
    result = assert.commandWorked(edb.runCommand({
        "delete": "basic",
        deletes: [
            {
                q: {"last": "marco"},
                limit: 1,
            },
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(41)
    }));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand({
        "delete": "basic",
        deletes: [
            {
                q: {"last": "marco"},
                limit: 1,
            },
        ],
        lsid: {id: lsid},
        txnNumber: NumberLong(41)
    }));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 0, 2, 2, 4);

    // Test retryable writes for findAndModify update
    //
    assert.commandWorked(edb.runCommand(
        {"insert": "basic", documents: [{"_id": 1, "first": "mark", "last": "marco"}]}));

    result = assert.commandWorked(edb.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"first": "matthew"}},
        lsid: {id: lsid},
        txnNumber: NumberLong(43)
    }));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 2);  // +2 for the insert and findAndModify

    retryResult = assert.commandWorked(edb.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"first": "matthew"}},
        lsid: {id: lsid},
        txnNumber: NumberLong(43)
    }));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assert.eq(retryResult.retriedStmtId, 2);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 1, 4, 3, 7);

    // Test retryable writes for findAndModify delete
    //
    result = assert.commandWorked(edb.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        remove: true,

        lsid: {id: lsid},
        txnNumber: NumberLong(47)
    }));
    print(tojson(result));

    origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, origOplogCount + 1);

    retryResult = assert.commandWorked(edb.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        remove: true,

        lsid: {id: lsid},
        txnNumber: NumberLong(47)
    }));
    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    assert.eq(retryResult.retriedStmtId, 0);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts("basic", 0, 4, 4, 8);
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
}());
