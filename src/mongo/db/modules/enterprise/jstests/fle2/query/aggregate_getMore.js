/**
 * Test aggregations on encrypted collections with getMore calls.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_60,
 *   requires_getmore,
 * ]
 */

load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/utils/agg_utils.js");

(function() {
// TODO: SERVER-72926 remove when v2 find works
if (isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
    return;
}

const {schema, docs, tests} = fleAggTestData;

// Set up the encrypted collection.
const dbName = "aggregateGetMoreDB";
const collName = "aggregateColl";
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
let client = new EncryptedClient(db.getMongo(), dbName);
assert.commandWorked(client.createEncryptionCollection(collName, schema));
let edb = client.getDB();

const coll = edb[collName];
for (const doc of docs) {
    assert.commandWorked(coll.insert(doc));
}
assert.commandWorked(coll.createIndex({location: "2dsphere"}));

// Run the pipeline on the provided collection, and assert that the results are equivalent to
// 'expected'. The pipeline is appended with a $project stage to project out safeContent data
// and other fields that are inconvenient to have in the output.
const runTest = (pipeline, collection, expected, extraInfo) => {
    const aggPipeline = pipeline.slice();
    aggPipeline.push({$project: {[kSafeContentField]: 0, distance: 0}});
    const result = collection.aggregate(aggPipeline, {cursor: {batchSize: 1}}).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

// Run all of the tests.
for (const testData of tests) {
    const extraInfo = Object.assign({transaction: false}, testData);
    runTest(testData.pipeline, coll, testData.expected, extraInfo);
}
}());
