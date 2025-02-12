/**
 * Test encrypted retryable writes work with text search indexed fields
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_non_retryable_commands,
 * assumes_read_preference_unchanged,
 * requires_capped,
 * assumes_unsharded_collection,
 * exclude_from_large_txns,
 * featureFlagQETextSearchPreview,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    SubstringField,
    SuffixAndPrefixField,
    SuffixField
} from "jstests/fle2/libs/qe_text_search_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

function countOplogEntries(primaryConn, dbname) {
    const oplog = primaryConn.getDB('local').oplog.rs;
    return oplog.find(({"ns": "admin.$cmd", "o.applyOps.ns": `${dbname}.basic`, "op": 'c'}))
        .itcount();
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
function runTest(conn, primaryConn, dbName, textFieldHandle) {
    print(`Running retryable write test on db ${dbName}`);

    const client = new EncryptedClient(conn, dbName);
    const edb = client.getDB();
    const lsid = UUID();

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields": [{
                "path": "first",
                "bsonType": "string",
                "queries": textFieldHandle.createQueryTypeDescriptor()
            }]
        }
    }));

    // Test retryable writes for one substring insert
    //
    let command = {
        insert: "basic",
        documents: [{"_id": 1, "first": "mark"}],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    };
    let expectedTagCount = textFieldHandle.calculateExpectedTagCount("mark".length);

    let result = assert.commandWorked(edb.erunCommand(command));
    print(`single insert result: ${tojson(result)}, expected_tag_count: ${expectedTagCount}`);

    client.assertEncryptedCollectionCounts("basic", 1, expectedTagCount, expectedTagCount);

    let oplogCount = countOplogEntries(primaryConn, dbName);
    assert.eq(oplogCount, 1);

    let retryResult = assert.commandWorked(edb.erunCommand(command));

    print(`single insert retry result: ${tojson(retryResult)}`);
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);

    // The stmtId for the actual EDC insert is offsetted by the number of ESC & ECOC inserts
    // which are both expectedTagCount.
    let expectedStmtId = expectedTagCount * 2;
    assertRetriedStmtIds(retryResult, [expectedStmtId]);

    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn, dbName));

    client.assertEncryptedCollectionCounts("basic", 1, expectedTagCount, expectedTagCount);

    // Test retryable writes for batched inserts
    //
    command = {
        insert: "basic",
        documents: [{"_id": 101, "first": "marianne"}, {"_id": 102, "first": "marsha"}],
        ordered: true,
        lsid: {id: lsid},
        txnNumber: NumberLong(48)
    };
    const doc1TagCount = textFieldHandle.calculateExpectedTagCount("marianne".length);
    const doc2TagCount = textFieldHandle.calculateExpectedTagCount("marsha".length);
    expectedTagCount += doc1TagCount + doc2TagCount;

    result = assert.commandWorked(edb.erunCommand(command));
    print(`batched insert result: ${tojson(result)}, doc1_tag_count: ${
        doc1TagCount}, doc2_tag_count: ${doc2TagCount}`);
    client.assertEncryptedCollectionCounts("basic", 3, expectedTagCount, expectedTagCount);

    let origOplogCount = oplogCount;
    oplogCount = countOplogEntries(primaryConn, dbName);
    assert.eq(oplogCount, origOplogCount + 2);

    retryResult = assert.commandWorked(edb.erunCommand(command));
    print(`batched insert retry result: ${tojson(retryResult)}`);
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);

    // The first document stmtdId is offsetted by the ESC + ECOC inserts (doc1TagCount * 2).
    // The second document stmtId is offsetted by the total inserts for the first document
    // (doc1TagCount * 2 + 1) plus its ESC and ECOC inserts (doc2TagCount * 2).
    assertRetriedStmtIds(retryResult,
                         [doc1TagCount * 2, (doc1TagCount * 2 + 1) + (doc2TagCount * 2)]);
    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn, dbName));

    client.assertEncryptedCollectionCounts("basic", 3, expectedTagCount, expectedTagCount);
}

function runTestSubstring(conn, primaryConn) {
    const field = new SubstringField(1000, 3, 100, false, false, 1);
    runTest(conn, primaryConn, "retrySubstring", field);
}

function runTestSuffix(conn, primaryConn) {
    const field = new SuffixField(3, 100, false, false, 1);
    runTest(conn, primaryConn, "retrySuffix", field);
}

function runTestSuffixAndPrefix(conn, primaryConn) {
    const field = new SuffixAndPrefixField(3, 100, 2, 10, false, false, 1);
    runTest(conn, primaryConn, "retrySuffixPrefix", field);
}

jsTestLog("ReplicaSet: Testing fle2 retryable writes with encrypted text fields");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTestSubstring(rst.getPrimary(), rst.getPrimary());
    runTestSuffix(rst.getPrimary(), rst.getPrimary());
    runTestSuffixAndPrefix(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 retryable writes with encrypted text fields");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTestSubstring(st.s, st.shard0);
    runTestSuffix(st.s, st.shard0);
    runTestSuffixAndPrefix(st.s, st.shard0);
    st.stop();
}
