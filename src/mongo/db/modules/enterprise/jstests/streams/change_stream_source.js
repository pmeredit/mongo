/**
 * Test which verifies that $source can be configured to draw input from a change stream.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');
load('src/mongo/db/modules/enterprise/jstests/streams/utils.js');

const outputDB = "outputDB";
const outputCollName = "outputColl";
const connectionName = "conn1";
const outputColl = db.getSiblingDB(outputDB)[outputCollName];
outputColl.drop();

const uri = 'mongodb://' + db.getMongo().host;
let connectionRegistry = [{name: connectionName, type: 'atlas', options: {uri: uri}}];
sp = new Streams(connectionRegistry);

// Collections and databases to issues writes against. When we write to these namespaces, this will
// generate change events that may or may not be picked up by our stream processor, depending on how
// we've configured our change stream pipeline.
const writeCollOne = "writeToThisColl";
const writeCollTwo = "writeToThisOtherCollection";
const writeDBOne = "writeToThisDB";
const writeDBTwo = "writeToThisOtherDB";

// Utility to perform writes against a combination of the namespaces above. This should generate
// some change events.
function performWrites() {
    // writeDBOne

    // Write 1 document to writeCollOne.
    let writeColl = db.getSiblingDB(writeDBOne)[writeCollOne];
    assert.commandWorked(writeColl.insert({_id: 1, a: 1, otherTimeField: Date.now()}));

    // Write 2 documents to writeCollTwo.
    writeColl = db.getSiblingDB(writeDBOne)[writeCollTwo];
    assert.commandWorked(writeColl.insertMany(
        [{_id: 2, a: 2, otherTimeField: Date.now()}, {_id: 3, a: 3, otherTimeField: Date.now()}]));

    // writeDBTwo

    // Write 5 documents to writeCollOne. Note that the first document in each collection has a
    // '_ts' field to demonstrate that this field can be overwritten.
    writeColl = db.getSiblingDB(writeDBTwo)[writeCollOne];
    assert.commandWorked(writeColl.insertMany([
        {_id: 5, a: 7, otherTimeField: Date.now(), _ts: Date.now()},
        {_id: 6, a: 33, otherTimeField: Date.now()},
        {_id: 7, a: 35, otherTimeField: Date.now()},
        {_id: 10, a: 133, otherTimeField: Date.now()},
        {_id: 17, a: 33, otherTimeField: Date.now()}
    ]));

    // Write 6 documents to writeCollTwo.
    writeColl = db.getSiblingDB(writeDBTwo)[writeCollTwo];
    assert.commandWorked(writeColl.insertMany([
        {_id: 5, a: 7, otherTimeField: Date.now(), _ts: Date.now()},
        {_id: 6, a: 33, otherTimeField: Date.now()},
        {_id: 7, a: 35, otherTimeField: Date.now()},
        {_id: 10, a: 133, otherTimeField: Date.now()},
        {_id: 17, a: 33, otherTimeField: Date.now()},
        {_id: 18, a: 7878, otherTimeField: Date.now()},
    ]));
}

function clearState() {
    db.getSiblingDB(writeDBOne).dropDatabase();
    db.getSiblingDB(writeDBTwo).dropDatabase();
    outputColl.drop();
}

function runChangeStreamSourceTest(
    {expectedNumberOfDataMessages, dbName, collName, overrideTsField, timeField}) {
    clearState();

    let sourceSpec = {connectionName: connectionName};
    if (dbName) {
        sourceSpec.db = dbName;
    }
    if (collName) {
        sourceSpec.coll = collName;
    }
    if (overrideTsField) {
        sourceSpec.tsFieldOverride = overrideTsField;
    }
    if (timeField) {
        // Use $toDate to access the field and obtain the value. Note that this code assumes that
        // we are only reading 'insert' change events.
        sourceSpec.timeField = {$toDate: "$fullDocument." + timeField};
    }

    const processorName = "changeStreamSourceProcessor";
    sp.createStreamProcessor(processorName, [
        {$source: sourceSpec},
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    const processor = sp[processorName];
    assert.commandWorked(processor.start());
    performWrites();

    processor.sample();
    assert.commandWorked(processor.stop());
    const res = outputColl.find().toArray();

    assert.eq(res.length, expectedNumberOfDataMessages);
    let previousTime = null;
    let resumeTokenSet = new Set();
    for (const doc of res) {
        assert(doc.hasOwnProperty("_stream_meta", doc));

        // Verify that the time values reported in our output documents align with what we expect.
        const actualTimeValue = function() {
            if (overrideTsField) {
                assert(doc.hasOwnProperty(overrideTsField), doc);
                return doc[overrideTsField];
            } else {
                assert(doc.hasOwnProperty("_ts"), doc);
                return doc["_ts"];
            }
        }();

        const expectedTimeValue = function() {
            if (timeField) {
                assert(doc.hasOwnProperty("fullDocument"), doc);
                const fullDoc = doc["fullDocument"];
                assert(fullDoc.hasOwnProperty(timeField), doc);

                // 'timeField' is a timestamp and needs to be converted to an ISODate for comparison
                // with 'actualTimeValue'.
                const asDate = new Date(fullDoc[timeField]);
                return ISODate(asDate.toISOString());
            } else {
                assert(doc.hasOwnProperty("wallTime"), doc);
                return doc["wallTime"];
            }
        }();

        assert.eq(actualTimeValue, expectedTimeValue, doc);

        // Verify that the times reported by our stream processor are increasing.
        if (previousTime) {
            assert.gte(actualTimeValue, previousTime, res);
        }
        previousTime = actualTimeValue;

        // Verify that we haven't seen this change event's resume token before.
        const resumeToken = doc["_id"];
        assert(!resumeTokenSet.has(resumeToken), res);
        resumeTokenSet.add(resumeToken);
    }
}

// Field to use other than wallTime.
const timeFieldName = "otherTimeField";
const tsOutputField = "overrideTimeField";

// Configure a $source with a change stream against a specific database.
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 3,
    dbName: writeDBOne,
    collName: null,
    overrideTsField: null,
    timeField: null,
});
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 11,
    dbName: writeDBTwo,
    collName: null,
    overrideTsField: null,
    timeField: null,
});

// Configure a $source with a change stream against a specific collection.
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 1,
    dbName: writeDBOne,
    collName: writeCollOne,
    overrideTsField: null,
    timeField: timeFieldName
});
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 2,
    dbName: writeDBOne,
    collName: writeCollTwo,
    overrideTsField: tsOutputField,
    timeField: null,
});
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 5,
    dbName: writeDBTwo,
    collName: writeCollOne,
    overrideTsField: tsOutputField,
    timeField: timeFieldName
});
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 6,
    dbName: writeDBTwo,
    collName: writeCollTwo,
    overrideTsField: null,
    timeField: null,
});

// Verify that change stream $source can be used to feed stages such as $hoppingWindow.
function testChangeStreamSourceWindowPipeline() {
    clearState();
    const processorName = "changeStreamSourceAndWindowProcessor";
    sp.createStreamProcessor(processorName, [
        {$source: {connectionName: connectionName, db: writeDBOne}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                pipeline: [
                    {
                        $group: {
                            _id: "$operationType",
                            sum: {$sum: 1},
                            pushAll: {$push: "$$ROOT"},
                            firstDocTime: {$first: "$_ts"},
                            lastDocTime: {$last: "$_ts"}
                        }
                    },
                ]
            }
        },
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    const processor = sp[processorName];
    assert.commandWorked(processor.start());

    const writeColl = db.getSiblingDB(writeDBOne)[writeCollOne];

    // Perform 3 inserts.
    assert.commandWorked(writeColl.insertMany([{_id: 2, a: 2}, {_id: 3, a: 3}, {_id: 4, a: 4}]));

    // Perform 3 updates.
    assert.commandWorked(writeColl.updateOne({_id: 2}, {$inc: {a: 1}}));
    assert.commandWorked(writeColl.updateOne({_id: 3}, {$inc: {a: 1}}));
    assert.commandWorked(writeColl.updateOne({_id: 4}, {$inc: {a: 1}}));

    let writeResults = writeColl.find().toArray();
    assert.eq(writeResults.length, 3, writeResults);

    // Sleep for 6 seconds and issue another write. This will close one window.
    sleep(6001);
    assert.commandWorked(writeColl.insertOne({_id: 100, a: 100}));
    writeResults = writeColl.find().toArray();
    assert.eq(writeResults.length, 4, writeResults);
    processor.sample();
    assert.commandWorked(processor.stop());

    const res = outputColl.find().toArray();

    // We should have exactly two groups, one for the inserts, and another for the updates.
    assert.eq(res.length, 2, res);
    for (const result of res) {
        for (let field
                 of ["_id", "_stream_meta", "pushAll", "sum", "firstDocTime", "lastDocTime"]) {
            assert(result.hasOwnProperty(field));

            // Verify that '_stream_meta' has both 'windowStartTimestamp' and 'windowEndTimestamp'.
            if (field === "_stream_meta") {
                const streamMeta = result[field];
                assert(streamMeta.hasOwnProperty("windowStartTimestamp"), result);
                assert(streamMeta.hasOwnProperty("windowEndTimestamp"), result);
            }

            // There should be at most 3 events in each group.
            if (field === "pushAll") {
                const len = result["pushAll"].length;
                assert.lte(len, 3, result);
                assert.gte(len, 1, result);
            }
        }

        // At this point, we know we have 'firstDocTime', 'lastDocTime', and '_stream_meta'. Verify
        // that our two reported doc times fall within the window boundaries.
        const streamMeta = result["_stream_meta"];
        const windowStart = streamMeta["windowStartTimestamp"];
        const windowEnd = streamMeta["windowEndTimestamp"];
        const firstTime = result["firstDocTime"];
        const lastTime = result["lastDocTime"];

        assert.gte(firstTime, windowStart, result);
        assert.gte(windowEnd, firstTime, result);
        assert.gte(lastTime, windowStart, result);
        assert.gte(windowEnd, lastTime, result);
    }
}

testChangeStreamSourceWindowPipeline();

// Verify that we can't start a change stream $source stream processor with mutually exclusive
// options.
function verifyThatStreamProcessorFailsToStartGivenInvalidOptions() {
    clearState();
    // Start a normal change stream, and get a ResumeToken.
    const cursor = db.getSiblingDB(writeDBOne).watch();
    const coll = db.getSiblingDB(writeDBOne)[writeCollOne];
    assert.commandWorked(coll.insertOne({_id: 1, a: 1}));
    assert(cursor.hasNext());
    const doc = cursor.next();
    assert(doc.hasOwnProperty("_id"), doc);
    const resumeToken = doc["_id"];
    cursor.close();

    const processorName = "thisProcessorShouldNotStart";
    sp.createStreamProcessor(processorName, [
        {
            $source: {
                connectionName: connectionName,
                db: writeDBOne,
                resumeAfter: resumeToken,
                startAfter: resumeToken,
            }
        },
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    // Start the processor.
    const processor = sp[processorName];
    assert.commandWorked(processor.start());
    const listCmd = {streams_listStreamProcessors: ''};
    let result = db.runCommand(listCmd);
    assert.eq(result["ok"], 1, result);
    assert.eq(result["streamProcessors"].length, 1, result);

    // Though starting the processor will work, the failure will cause it to eventually be stopped
    // and pruned by the background thread. As such, we verify that, after sleeping for longer than
    // the background thread, our stream processor has been stopped.
    // TODO SERVER-77657: Replace this with a unit test; also, add a test that verifies that stop()
    // works when a continuous stream of events is flowing through $source.
    sleep(70 * 1000);
    result = db.runCommand(listCmd);
    assert.eq(result["ok"], 1, result);
    assert.eq(result["streamProcessors"].length, 0, result);
}

verifyThatStreamProcessorFailsToStartGivenInvalidOptions();

// TODO SERVER-77657: Add coverage for our handling of invalidate events.
}());
