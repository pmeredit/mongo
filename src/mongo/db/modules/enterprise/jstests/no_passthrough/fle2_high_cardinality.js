/**
 * Test we fall back to high cardinality mode.
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

function testQueries(edb) {
    // Test find rewrite
    let ret;
    ret = edb.basic.find({first: "mark"});
    assert.eq(ret.itcount(), 21);

    ret = edb.basic.find({first: "mark", last: "marco"});
    assert.eq(ret.itcount(), 21);

    ret = edb.basic.find({first: "mark", last: "marcus"});
    assert.eq(ret.itcount(), 0);
    ret = edb.basic.find({first: "markus", last: "marco"});
    assert.eq(ret.itcount(), 0);

    // Test $in rewrite
    ret = edb.basic.find({first: {$in: ["marco", "mark"]}});
    assert.eq(ret.itcount(), 21);
    ret = edb.basic.find({first: {$in: ["marco"]}});
    assert.eq(ret.itcount(), 0);

    ret = edb.basic.aggregate([{$match: {first: "mark"}}]);
    assert.eq(ret.itcount(), 21);
    ret = edb.basic.aggregate([{$match: {first: "mark"}}]);
    assert.eq(ret.itcount(), 21);

    ret = edb.basic.aggregate([{$match: {first: "mark", last: "marco"}}]);
    assert.eq(ret.itcount(), 21);

    // Test: $expr queries
    ret = edb.basic.aggregate([{$match: {$expr: {$eq: ["$first", "mark"]}}}]);
    assert.eq(ret.itcount(), 21);
    ret = edb.basic.aggregate([{$match: {$expr: {$eq: ["mark", "$first"]}}}]);
    assert.eq(ret.itcount(), 21);

    ret = edb.basic.aggregate(
        [{$match: {$expr: {$and: [{$eq: ["$first", "mark"]}, {$eq: ["$last", "marco"]}]}}}]);
    assert.eq(ret.itcount(), 21);
    ret = edb.basic.aggregate(
        [{$match: {$expr: {$and: [{$eq: ["mark", "$first"]}, {$eq: ["marco", "$last"]}]}}}]);
    assert.eq(ret.itcount(), 21);

    ret = edb.basic.aggregate([{$match: {$expr: {$in: ["$first", ["marco", "mark"]]}}}]);
    assert.eq(ret.itcount(), 21);
}

function runTest(conn) {
    let dbName = 'low_card';
    let db = conn.getDB(dbName);

    let client = new EncryptedClient(db.getMongo(), dbName);

    let edb = client.getDB();

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "last", "bsonType": "string", "queries": {"queryType": "equality"}}
            ]
        }
    }));

    for (let i = 0; i < 21; i++) {
        assert.commandWorked(edb.basic.einsert({num: i, first: "mark", last: "marco"}));
    }

    // Run queries in normal mode
    client.runEncryptionOperation(() => { testQueries(edb); });

    // Setting this parameter means encrypted rewrites will generate no more than 20 encrypted tags
    // and we will trigger low selectivity mode.
    jsTestLog("Testing Low selectivity mode");

    assert.commandWorked(
        edb.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 10 * 40 + 10 * 41}));

    // Run queries in low selectivity mode
    client.runEncryptionOperation(() => { testQueries(edb); });

    const result = assert.commandWorked(edb.erunCommand({
        explain: {
            find: "basic",
            projection: {[kSafeContentField]: 0},
            filter: {first: "mark"},
        },
        verbosity: "queryPlanner"
    }));
    print(tojson(result));

    let inputQuery;
    if (result.queryPlanner.winningPlan.shards) {
        inputQuery = result.queryPlanner.winningPlan.shards[0].parsedQuery;
    } else {
        inputQuery = result.queryPlanner.parsedQuery;
    }

    assert(inputQuery.hasOwnProperty("$expr"));
    assert(inputQuery["$expr"].hasOwnProperty("$_internalFleEq"));

    let fleEq = inputQuery["$expr"]["$_internalFleEq"];

    // Run a incorrect query directly with the wrong ServerZerosToken
    let ret = db.basic.aggregate([{
        $match: {
            $expr: {
                $_internalFleEq: {
                    field: "$first",
                    server: BinData(6, "CEWSmQID7SfwyAUI3ZkSFkATKryDQfnxXEOGad5d4Rsg")
                }
            }
        }
    }]);
    assert.eq(ret.itcount(), 0);

    // Run a correct query directly
    ret = db.basic.aggregate(
        [{$match: {$expr: {$_internalFleEq: {field: "$first", server: fleEq.server}}}}]);
    assert.eq(ret.itcount(), 21);
}

jsTestLog("ReplicaSet: Testing fle2 high cardinality");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 high cardinality");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s);

    st.stop();
}
