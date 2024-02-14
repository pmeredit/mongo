/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

function testStreamMeta(streamMetaOptionValue, streamMetaFieldName) {
    const uri = 'mongodb://' + db.getMongo().host;
    const dbName = "db";
    const collName = "coll";
    const dlqCollName = "dlq";
    const connectionName = "db1";
    const connectionRegistry = [{name: connectionName, type: 'atlas', options: {uri: uri}}];
    const sp = new Streams(connectionRegistry);
    const coll = db.getSiblingDB(dbName)[collName];
    const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
    coll.drop();
    dlqColl.drop();

    const processorName = "processor";
    const processor = sp.createStreamProcessor(processorName, [
        {
            $source: {
                timeField: {$toDate: "$timestamp"},
                tsFieldOverride: "__ts",
                documents: [
                    {timestamp: "2024-01-01T00:00:00Z", a: 0},
                    {timestamp: "2024-01-01T00:00:01Z", a: 1},
                ]
            }
        },
        {$addFields: {b: {$divide: [1, "$a"]}}},
        {$merge: {into: {connectionName: connectionName, db: dbName, coll: collName}}}
    ]);

    const options = {
        streamMetaFieldName: streamMetaOptionValue,
        dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName},
    };
    assert.commandWorked(processor.start(options));
    assert.soon(() => { return coll.find().itcount() === 1 && dlqColl.find().itcount() === 1; });

    // Test sink results.
    const results = coll.find().toArray();
    assert.eq(results.length, 1, results);
    for (let result of results) {
        const meta = {timestamp: result.__ts};
        if (streamMetaFieldName !== null) {
            // If field name is set, metadata object is under the expected field.
            assert.docEq(result[streamMetaFieldName], meta, results);
        } else {
            // Otherwise, metadata object is not under any field.
            assert(Object.values(result).every(val => !friendlyEqual(val, meta)), results);
        }
    }

    // Test DLQ results.
    const dlqResults = dlqColl.find().toArray();
    assert.eq(dlqResults.length, 1, dlqResults);
    for (let result of dlqResults) {
        if (streamMetaFieldName !== null) {
            // If field name is set, desired metadata field exists.
            assert(Object.keys(result).includes(streamMetaFieldName), dlqResults);
        } else {
            // Otherwise, the field doesn't exist.
            assert(!Object.keys(result).includes(streamMetaFieldName), dlqResults);
        }
    }

    assert.commandWorked(processor.stop());
}

testStreamMeta(undefined, "_stream_meta");
testStreamMeta(null, "_stream_meta");
testStreamMeta("abc", "abc");
testStreamMeta("", null);
