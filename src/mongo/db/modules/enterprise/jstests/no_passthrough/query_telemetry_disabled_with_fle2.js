/**
 * Test aggregations on encrypted collections.
 */

load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/feature_flag_util.js");

(function() {
const docs = [
    {_id: 0, ssn: "123", uniqueFieldName: "A", manager: "B", age: NumberLong(25), location: [0, 0]},
    {_id: 1, ssn: "456", uniqueFieldName: "B", manager: "C", age: NumberLong(35), location: [0, 1]},
    {_id: 2, ssn: "789", uniqueFieldName: "C", manager: "D", age: NumberLong(45), location: [0, 2]},
    {_id: 3, ssn: "123", uniqueFieldName: "D", manager: "A", age: NumberLong(55), location: [0, 3]},
];

const schema = {
    encryptedFields: {
        fields: [
            {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
            {path: "age", bsonType: "long", queries: {queryType: "equality"}}
        ]
    },
};

// Turn on the collecting of telemetry metrics.
let options = {
    setParameter:
        {featureFlagTelemetry: true, internalQueryConfigureTelemetrySamplingRate: 2147483647},
};
const rst = new ReplSetTest({nodes: 1, nodeOptions: options});
rst.startSet();

rst.initiate();
rst.awaitReplication();

// Set up the non-encrypted collection.
const rstConn = rst.getPrimary();
const testDB = rstConn.getDB('test');
testDB.dropDatabase();
var coll = testDB[jsTestName()];
coll.drop();

// Set up the encrypted collection.
const encryptedCollName = jsTestName() + "_encrypted";
let encryptedClient = new EncryptedClient(rstConn, testDB);
let res = encryptedClient.createEncryptionCollection(encryptedCollName, schema);
assert.commandWorked(res);
let edb = encryptedClient.getDB();
const encryptedColl = edb[encryptedCollName];

for (const doc of docs) {
    assert.commandWorked(coll.insert(doc));
    assert.commandWorked(encryptedColl.insert(doc));
}

function checkTelemetryOnAggregation(namespace, pipeline) {
    let telStore = testDB.adminCommand({aggregate: 1, pipeline: [{$telemetry: {}}], cursor: {}});
    for (let i = 0; i < telStore.cursor.firstBatch.length; i++) {
        const entry = telStore.cursor.firstBatch[i];
        if (entry.key.namespace == namespace) {
            if (entry.key.pipeline && documentEq(entry.key.pipeline, pipeline)) {
                return true;
            }
        }
    }
    return false;
}

function checkTelemetryOnFind(filter, execCount = 1) {
    let telStore = testDB.adminCommand({
        aggregate: 1,
        pipeline: [
            {
                $telemetry: {},

            },
            {$match: {"key.queryShape.filter": filter}}
        ],
        cursor: {}
    });
    const results = telStore.cursor.firstBatch;
    if (results.length != 1) {
        return false;
    }
    return results[0].metrics.execCount == execCount;
}

const pipeline = [{$match: {_id: 0}}];
const redactedPipeline = [{$match: {_id: "###"}}];

// Assert that telemetry is not collected on an aggregation on an encryption-enabled collection.
assert.eq(encryptedColl.aggregate(pipeline).itcount(), 1);
assert(!checkTelemetryOnAggregation(encryptedColl, redactedPipeline));

// Assert that telemetry is collected on an aggregation on a regular collection.
assert.eq(coll.aggregate(pipeline).itcount(), 1);
assert(checkTelemetryOnAggregation(coll, redactedPipeline));

const encryptedPipeline = [{$match: {ssn: "456"}}];
const redactedEncryptedPipeline = [{$match: {ssn: "###"}}];

// Assert that telemetry is not collected on an aggregation that queries an encrypted field.
assert.eq(encryptedColl.aggregate(encryptedPipeline).itcount(), 1);
assert(!checkTelemetryOnAggregation(encryptedColl, redactedEncryptedPipeline));

// Assert that telemetry is collected when querying the same field in a regular collection.
assert.eq(coll.aggregate(encryptedPipeline).itcount(), 1);
assert(checkTelemetryOnAggregation(coll, redactedEncryptedPipeline));

// Assert that telemetry is collected on a collection-less aggregation. $telemetry is an
// aggregation without a targeted collection, so the results of the $telemetry command should be
// visible in the telemetry store.
testDB.adminCommand({aggregate: 1, pipeline: [{$telemetry: {}}], cursor: {}});
assert(checkTelemetryOnAggregation("admin.$cmd.aggregate", [{$telemetry: {}}]));

// Since many internal commands use "find", we have to use a unique field name to ensure that name
// is not found in the telemetry store.
const findCmd = {
    "uniqueFieldName": "A"
};
const redactedFindCmd = {
    "uniqueFieldName": {$eq: "?string"}
};

// Assert that telemetry is not collected on a find command on an encryption-enabled collection.
assert.eq(encryptedColl.find(findCmd).itcount(), 1);
assert(!checkTelemetryOnFind(redactedFindCmd));

// Assert that telemetry is collected on a find command on a regular collection.
assert.eq(coll.find(findCmd).itcount(), 1);
assert(checkTelemetryOnFind(redactedFindCmd));

const encryptedFindCmd = {
    "ssn": "456"
};
const redactedEncryptedFindCmd = {
    "ssn": {$eq: "?string"}
};

// Assert that telemetry is not collected on a find command on an encryption-enabled collection.
assert.eq(encryptedColl.find(encryptedFindCmd).itcount(), 1);
assert(!checkTelemetryOnFind(redactedEncryptedFindCmd));

// Assert that telemetry is collected on a find command on a regular collection.
assert.eq(coll.find(encryptedFindCmd).itcount(), 1);
assert(checkTelemetryOnFind(redactedEncryptedFindCmd));

rst.stopSet();
}());
