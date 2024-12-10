/**
 * Test which verifies that $source can be configured to draw input from a change stream.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    listStreamProcessors,
    sampleUntil,
    stopStreamProcessor,
    TEST_TENANT_ID,
    waitForCount
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const outputDB = "outputDB";
const outputCollName = "outputColl";
const connectionName = "conn1";
const outputColl = db.getSiblingDB(outputDB)[outputCollName];
outputColl.drop();

const uri = 'mongodb://' + db.getMongo().host;
let connectionRegistry = [{name: connectionName, type: 'atlas', options: {uri: uri}}];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

// Collections and databases to issues writes against. When we write to these namespaces, this will
// generate change events that may or may not be picked up by our stream processor, depending on how
// we've configured our change stream pipeline.
const writeCollOne = "writeToThisColl";
const writeCollTwo = "writeToThisOtherCollection";
const writeCollThree = "writeToNewCollection";

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

    // write 3 documents to writeCollThree
    writeColl = db.getSiblingDB(writeDBTwo)[writeCollThree];
    assert.commandWorked(writeColl.insertMany([
        {_id: 21, a: 7, otherTimeField: Date.now(), _ts: Date.now()},
        {_id: 22, a: 33, otherTimeField: Date.now()},
        {_id: 23, a: 35, otherTimeField: Date.now()},
    ]));
}

function clearState() {
    db.getSiblingDB(writeDBOne).dropDatabase();
    db.getSiblingDB(writeDBTwo).dropDatabase();
    outputColl.drop();
}

function runChangeStreamSourceTest({
    expectedNumberOfDataMessages,
    dbName,
    collName,
    overrideTsField,
    streamMetaFieldName,
    timeField,
    pushdownPipeline
}) {
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
    if (streamMetaFieldName) {
        sourceSpec.streamMetaFieldName = streamMetaFieldName;
    }
    if (timeField) {
        // Use $toDate to access the field and obtain the value. Note that this code assumes that
        // we are only reading 'insert' change events.
        sourceSpec.timeField = {$toDate: "$fullDocument." + timeField};
    }
    if (pushdownPipeline) {
        sourceSpec.config = {pipeline: pushdownPipeline};
    }

    const processorName = "changeStreamSourceProcessor";
    sp.createStreamProcessor(processorName,
                             [{$source: sourceSpec}, {$emit: {connectionName: '__testMemory'}}]);

    const processor = sp[processorName];
    let startResult = processor.start({featureFlags: {}, shouldStartSample: true});
    assert.commandWorked(startResult);
    const cursorId = startResult["sampleCursorId"];

    performWrites();

    let outputDocs = sampleUntil(cursorId, expectedNumberOfDataMessages, processorName);

    // Get verbose stats.
    const verboseStats = getStats(processorName);
    jsTestLog(verboseStats);
    assert.eq(verboseStats["ok"], 1);
    const startingPoint = verboseStats['changeStreamState'];
    assert(startingPoint);
    assert.commandWorked(processor.stop());

    let previousTime = null;
    let resumeTokenSet = new Set();
    for (const doc of outputDocs) {
        assert(doc.hasOwnProperty(streamMetaFieldName ? streamMetaFieldName : "_stream_meta", doc));

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
            assert.gte(actualTimeValue, previousTime, outputDocs);
        }
        previousTime = actualTimeValue;

        // Verify that we haven't seen this change event's resume token before.
        const resumeToken = doc["_id"];
        assert(!resumeTokenSet.has(resumeToken), outputDocs);
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
    expectedNumberOfDataMessages: 14,
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

runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 11,
    dbName: writeDBTwo,
    collName: [writeCollOne, writeCollTwo],
    overrideTsField: null,
    timeField: null,
});

// Configure a $source with a change stream configured with a pipeline.
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 2,
    dbName: writeDBTwo,
    collName: writeCollOne,
    pushdownPipeline: [
        {$match: {"fullDocument.a": 33}},
    ],
});
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 1,
    dbName: writeDBTwo,
    collName: writeCollOne,
    pushdownPipeline: [
        {$addFields: {x: {$divide: ["$fullDocument.a", 100]}}},
        {$match: {x: {$gt: 1}}},
    ],
});

// Use non-default streamMetaFieldName configuration.
runChangeStreamSourceTest({
    expectedNumberOfDataMessages: 3,
    dbName: writeDBOne,
    collName: null,
    overrideTsField: null,
    streamMetaFieldName: "foo",
    timeField: null,
});

// Test whole cluster support.
runChangeStreamSourceTest({
    // The 17 documents inserted in performWrites.
    expectedNumberOfDataMessages: 17,
    dbName: undefined,
    collName: undefined,
});

// With fullDocumentOnly
function runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages,
    dbName,
    collName,
    timeField,
    overrideTsField,
    fullDocumentMode
}) {
    clearState();

    let sourceSpec = {connectionName: connectionName, config: {fullDocumentOnly: true}};
    if (dbName) {
        sourceSpec.db = dbName;
    }
    if (collName) {
        sourceSpec.coll = collName;
    }
    // Use $toDate to access the field and obtain the value. Note that this code assumes that
    // we are only reading 'insert' change events.
    if (timeField) {
        sourceSpec.timeField = {$toDate: "$" + timeFieldName};
    }
    if (overrideTsField) {
        sourceSpec.tsFieldName = overrideTsField;
    }
    if (fullDocumentMode) {
        sourceSpec.config.fullDocument = fullDocumentMode;
    }

    const processorName = "changeStreamFullDocument";
    sp.createStreamProcessor(processorName, [
        {$source: sourceSpec},
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    const processor = sp[processorName];
    let startResult =
        assert.commandWorked(processor.start({featureFlags: {}, shouldStartSample: true}));
    assert.commandWorked(startResult);
    const cursorId = startResult["sampleCursorId"];

    performWrites();

    let outputDocs = sampleUntil(cursorId, expectedNumberOfDataMessages, processorName);

    assert.commandWorked(processor.stop());

    let previousTime = null;
    for (const doc of outputDocs) {
        assert(doc.hasOwnProperty("_stream_meta", doc));
        assert(doc.hasOwnProperty("fullDocument") == false, doc);
        if (overrideTsField) {
            assert(doc.hasOwnProperty(overrideTsField, doc));
        } else {
            assert(doc.hasOwnProperty("_ts", doc));
        }
        if (timeField) {
            // Verify that the time values reported in our output documents align with what we
            // expect.
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
                assert(doc.hasOwnProperty(timeFieldName), doc);
                // 'timeField' is a timestamp and needs to be converted to an ISODate for comparison
                // with 'actualTimeValue'.
                const asDate = new Date(doc[timeFieldName]);
                return ISODate(asDate.toISOString());
            }();

            assert.eq(actualTimeValue, expectedTimeValue, doc);

            // Verify that the times reported by our stream processor are increasing.
            if (previousTime) {
                assert.gte(actualTimeValue, previousTime, outputDocs);
            }
            previousTime = actualTimeValue;
        }
    }
}

runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 5,
    dbName: writeDBTwo,
    collName: writeCollOne,
    timeField: timeFieldName,
    overrideTsField: null,
    fullDocumentMode: "required",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 6,
    dbName: writeDBTwo,
    collName: writeCollTwo,
    timeField: timeFieldName,
    overrideTsField: null,
    fullDocumentMode: "required",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 5,
    dbName: writeDBTwo,
    collName: writeCollOne,
    timeField: timeFieldName,
    overrideTsField: null,
    fullDocumentMode: "updateLookup",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 6,
    dbName: writeDBTwo,
    collName: writeCollTwo,
    timeField: timeFieldName,
    overrideTsField: null,
    fullDocumentMode: "updateLookup",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 6,
    dbName: writeDBTwo,
    collName: writeCollTwo,
    timeField: timeFieldName,
    overrideTsField: tsOutputField,
    fullDocumentMode: "updateLookup",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 5,
    dbName: writeDBTwo,
    collName: writeCollOne,
    timeField: timeFieldName,
    overrideTsField: tsOutputField,
    fullDocumentMode: "updateLookup",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 2,
    dbName: writeDBOne,
    collName: writeCollTwo,
    timeField: null,
    overrideTsField: null,
    fullDocumentMode: "updateLookup",
});
runChangeStreamSourceTestWithFullDocumentOnly({
    expectedNumberOfDataMessages: 2,
    dbName: writeDBOne,
    collName: writeCollTwo,
    timeField: null,
    overrideTsField: null,
    fullDocumentMode: "required",
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
    assert.commandWorked(processor.start({featureFlags: {}}));

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

            // Verify that '_stream_meta' has both 'window.start' and 'window.end'.
            if (field === "_stream_meta") {
                const streamMeta = result[field];
                assert(streamMeta.hasOwnProperty("window"), result);
                assert(streamMeta.window.hasOwnProperty("start"), result);
                assert(streamMeta.window.hasOwnProperty("end"), result);
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
        const windowStart = streamMeta["window"]["start"];
        const windowEnd = streamMeta["window"]["end"];
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
                config: {
                    startAfter: resumeToken,
                    startAtOperationTime: db.hello().$clusterTime.clusterTime
                }
            }
        },
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    // Start the processor.
    const processor = sp[processorName];
    assert.commandFailed(processor.start({featureFlags: {}}, false));
    let result = listStreamProcessors();
    assert.eq(result["ok"], 1, result);
    assert.eq(result["streamProcessors"].length, 0, result);
}

verifyThatStreamProcessorFailsToStartGivenInvalidOptions();

// Verify that we can't start a stream processor with invalid fullDocumentMode (default and
// whenAvailable) when fullDocumentOnly is set to true.
function verifyThatStreamProcessorFailsToStartForInvalidFullDocumentMode(fullDocumentMode) {
    clearState();
    let sourceSpec = {connectionName: connectionName};
    sourceSpec.db = writeDBOne;
    sourceSpec.timeField = {$toDate: ".otherTimeField"};
    sourceSpec.config = {fullDocumentOnly: true};
    if (fullDocumentMode) {
        sourceSpec.config.fullDocument = fullDocumentMode;
    }

    const processorName = "invalidFullDocModeProcessorFail";
    sp.createStreamProcessor(processorName, [
        {$source: sourceSpec},
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);

    // Start the processor.
    const processor = sp[processorName];
    assert.commandFailed(processor.start({featureFlags: {}}, false));
    let result = listStreamProcessors();
    assert.eq(result["ok"], 1, result);
    assert.eq(result["streamProcessors"].length, 0, result);
}

verifyThatStreamProcessorFailsToStartForInvalidFullDocumentMode("whenAvailable");
verifyThatStreamProcessorFailsToStartForInvalidFullDocumentMode(null /* default */);

function verifyUpdateFullDocument() {
    const processorName = "sp1";

    let id = 0;
    let innerTest = (fullDocumentMode,
                     expectFullDocument,
                     validateResults,
                     fullDocumentBeforeChange = null) => {
        // Clears the output collection.
        clearState();
        const dbName = "test";
        const collName = "coll" + id;
        db.getSiblingDB(dbName)[collName].drop();
        db.getSiblingDB("test").createCollection(collName,
                                                 {changeStreamPreAndPostImages: {enabled: true}});
        sp.createStreamProcessor(processorName, [
            {
                $source: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: collName,
                    config: {
                        fullDocument: fullDocumentMode,
                        fullDocumentBeforeChange: fullDocumentBeforeChange
                    }
                }
            },
            {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
        ]);

        const processor = sp[processorName];
        assert.commandWorked(processor.start({featureFlags: {}}, false));
        let result = listStreamProcessors();
        assert.eq(result["ok"], 1, result);
        assert.eq(result["streamProcessors"].length, 1, result);

        let writeColl = db.getSiblingDB(dbName)[collName];
        assert.commandWorked(writeColl.insert({_id: id, a: 0}));
        for (let i = 0; i < 100; ++i) {
            assert.commandWorked(writeColl.updateOne({_id: id}, {$inc: {a: 1}}));
        }

        let outputColl = db.getSiblingDB(outputDB)[outputCollName];
        // Wait for all the output to show up.
        assert.soon(() => { return outputColl.find({operationType: "update"}).count() == 100; });
        let output =
            outputColl.find({operationType: "update"}).sort({"fullDocument.a": 1}).toArray();
        assert.eq(100, output.length);
        for (let i = 0; i < output.length; i += 1) {
            if (expectFullDocument) {
                assert(output[i].hasOwnProperty('fullDocument'), output[i]);
                if (validateResults) {
                    assert.eq(i + 1, output[i].fullDocument.a);
                }
            } else {
                assert(!output[i].hasOwnProperty('fullDocument'), output[i]);
            }

            if (fullDocumentBeforeChange != null) {
                assert(output[i].hasOwnProperty("fullDocumentBeforeChange"));
                assert.eq(output[i].fullDocument.a - 1, output[i].fullDocumentBeforeChange.a);
            } else {
                assert(!output[i].hasOwnProperty("fullDocumentBeforeChange"));
            }
        }

        const metrics = db.runCommand({"streams_getMetrics": ""});
        assert.commandWorked(metrics);
        assert.gt(metrics['counters'].find(c => c.name == 'read_single_change_event_count').value,
                  0);

        sp[processorName].stop();
        id += 1;
    };

    innerTest("updateLookup", true /* expectFullDocument */, false /* validateUpdateContents */);
    innerTest("whenAvailable", true /* expectFullDocument */, true /* validateUpdateContents */);
    innerTest("required", true /* expectFullDocument */, true /* validateUpdateContents */);
    innerTest("default", false /* expectFullDocument */, false /* validateUpdateContents */);
    innerTest(null, false /* expectFullDocument */, false /* validateUpdateContents */);
    innerTest("whenAvailable",
              true /* expectFullDocument */,
              true /* validateUpdateContents */,
              "required");
    innerTest("whenAvailable",
              true /* expectFullDocument */,
              true /* validateUpdateContents */,
              "whenAvailable");
}

verifyUpdateFullDocument();

// Test that changestream $source still works after an invalidate event,
// which occurs after a collection drop.
function testAfterInvalidate() {
    const uri = 'mongodb://' + db.getMongo().host;
    const connectionName = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const outputCollName = "testout";
    const inputColl = db.getSiblingDB(dbName)[inputCollName];
    const outputColl = db.getSiblingDB(dbName)[outputCollName];
    const connectionRegistry = [
        {
            name: connectionName,
            type: 'atlas',
            options: {
                uri: uri,
            }
        },
    ];
    const spName = "sp1";

    // Create the collection and drop it. This will cause an invalidate
    // event in the changestream.
    inputColl.insert({a: 1});
    inputColl.drop();
    // Start a streamProcessor.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        processorId: spName,
        tenantId: TEST_TENANT_ID,
        pipeline: [
            {
                $source: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {
                $merge: {
                    into: {connectionName: connectionName, db: dbName, coll: outputCollName},
                }
            }
        ],
        connections: connectionRegistry,
        options: {featureFlags: {}},
    });
    assert.commandWorked(result);

    // Get verbose stats.
    const verboseStats = getStats(spName);
    jsTestLog(verboseStats);
    assert.eq(verboseStats["ok"], 1);
    // Ensure that the maxMemoryUsage for the source operator is more than 0.
    const sourceStats = verboseStats['operatorStats'][0];
    assert.gt(sourceStats['maxMemoryUsage'], 0);

    // Insert a few documents into the source collection.
    inputColl.insert({a: 2});
    inputColl.insert({a: 3});
    // Validate that the documents show up in the sink.
    assert.soon(() => {
        let result = outputColl.find({}).toArray();
        return result.some(doc => {
            if (doc.hasOwnProperty("fullDocument")) {
                return doc.fullDocument.a === 3;
            }
            return false;
        });
    });
    // Stop the streamProcessor.
    stopStreamProcessor(spName);
}

testAfterInvalidate();

function testAfterInvalidateWithFullDocumentOnly() {
    const uri = 'mongodb://' + db.getMongo().host;
    const connectionName = "dbgood";
    const dbName = "test";
    const inputCollName = "testin";
    const outputCollName = "testout";
    const dlqCollName = "dlqColl";
    const inputColl = db.getSiblingDB(dbName)[inputCollName];
    const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
    const connectionRegistry = [
        {
            name: connectionName,
            type: 'atlas',
            options: {
                uri: uri,
            }
        },
    ];
    const spName = "sp1";
    dlqColl.drop();
    inputColl.drop();

    // Create the collection and drop it. This will cause an invalidate
    // event in the changestream.
    inputColl.insert({a: 1});
    let startAtOperationTime = db.hello().$clusterTime.clusterTime;
    inputColl.drop();
    // Start a streamProcessor.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        processorId: spName,
        tenantId: "testTenant",
        pipeline: [
            {
                $source: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: inputCollName,
                    config: {
                        fullDocumentOnly: true,
                        fullDocument: "required",
                        startAtOperationTime: startAtOperationTime
                    },
                }
            },
            {
                $merge: {
                    into: {connectionName: connectionName, db: dbName, coll: outputCollName},
                }
            }
        ],
        connections: connectionRegistry,
        options: {
            dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName},
            featureFlags: {}
        },
    });
    assert.commandWorked(result);

    // Get verbose stats.
    const verboseStats = getStats(spName);
    jsTestLog(verboseStats);
    assert.eq(verboseStats["ok"], 1);
    // Validate that the drop and invalidate events were dlq'ed.
    assert.soon(() => { return dlqColl.count() == 2; });

    let res = dlqColl.find({}).toArray();

    const dropOperation = res.find(doc => doc.doc.operationType == "drop");
    const invalidateOperation = res.find(doc => doc.doc.operationType == "invalidate");

    if (dropOperation == null || invalidateOperation == null) {
        jsTestLog("BF-35656: Troubleshooting a very intermittent issue");
        jsTestLog("res = " + JSON.stringify(res));
    }

    assert.includes(dropOperation.errInfo.reason, "Missing fullDocument field");
    assert.includes(invalidateOperation.errInfo.reason, "Missing fullDocument field");
    for (const dlqDoc of res) {
        assert.eq(dlqDoc.operatorName, "ChangeStreamConsumerOperator");
    }
    // Stop the streamProcessor.
    stopStreamProcessor(spName);
    dlqColl.drop();
}

testAfterInvalidateWithFullDocumentOnly();

function testCollWithoutDb() {
    clearState();
    const processorName = "changeStreamSourceProcessor";
    sp.createStreamProcessor(processorName, [
        {
            $source: {
                connectionName: connectionName,
                coll: writeCollOne,
            }
        },
        {$merge: {into: {connectionName: connectionName, db: outputDB, coll: outputCollName}}}
    ]);
    const processor = sp[processorName];
    let result = processor.start({featureFlags: {}}, false /* assertWorked */);
    // This is the error the target changestream $source gives us.
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
}

testCollWithoutDb();

// This test tests that we observe the change stream lag in the verbose stats. This is
// achieved via enabling a failpoint that slows down message processing in the executor thread in
// mongod this lets the lag grow large enough (> 1 second) for it to be observed. After observing
// the lag the failpoint is disabled so that the server catches up with the stream and at that
// point, we append newer events to a different collection that is not being watched by this
// changestream. Then the test validates that we do not observe any lag in this situation, since we
// are effectively "caught up" with the stream
function testChangeStreamSourceLagStat() {
    clearState();

    assert.commandWorked(db.adminCommand(
        {'configureFailPoint': 'changestreamSlowEventProcessing', 'mode': 'alwaysOn'}));

    const processorName = "changeStreamSourceLaggingProcessor";
    sp.createStreamProcessor(processorName, [
        {$source: {connectionName: connectionName, db: writeDBOne, coll: writeCollOne}},
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

    const writeColl = db.getSiblingDB(writeDBOne)[writeCollOne];
    const writeColl2 = db.getSiblingDB(writeDBOne)[writeCollTwo];
    // Perform 10 inserts
    for (let i = 0; i < 10; i++) {
        assert.commandWorked(writeColl.insertOne({_id: 2 + i, a: 2 + i}));
    }

    const processor = sp[processorName];
    assert.commandWorked(processor.start({featureFlags: {}}));

    // Keep appending new events to the oplog and check that eventually startingPointLag > 0.
    // Since we have enabled slowChangestreamSource fail point, so the server
    // is being slow in processing events. Hence, eventually we should
    // see a gap between the resume token point and the latest event in the oplog
    var lagSeen = false;
    var i = 13;
    for (let j = 0; j < 50; j++) {
        // append events, with some sleeps to get the oplog event times to increase noticeably
        for (let k = 0; k < 10; k++) {
            assert.commandWorked(writeColl.insertOne({_id: i, a: 2 + i}));
            i += 1;
        }

        // Now check if we have started seeing the lag yet
        const verboseStats = getStats(processorName);
        // jsTestLog(verboseStats);
        assert.eq(verboseStats["ok"], 1);
        const startingPoint = verboseStats['changeStreamState'];
        assert(startingPoint);
        const startingPointLag = verboseStats['changeStreamTimeDifferenceSecs'];
        if (startingPointLag > 0) {
            lagSeen = true;
            break;
        }
        sleep(200);
    }

    assert(lagSeen);

    // Now disable failpoint so that we read all events and catch up
    assert.commandWorked(
        db.adminCommand({'configureFailPoint': 'changestreamSlowEventProcessing', 'mode': 'off'}));

    // Now wait till we have read all events
    assert.soon(() => {
        const verboseStats = getStats(processorName);
        // jsTestLog(verboseStats);
        assert.eq(verboseStats["ok"], 1);
        const startingPoint = verboseStats['changeStreamState'];
        assert(startingPoint);
        const startingPointLag = verboseStats['changeStreamTimeDifferenceSecs'];
        return startingPointLag == 0;
    });

    // Now write to coll2, since we are only watching coll1, we will not get any
    // new events and so will be "caught up" and so the lag will show
    // as 0
    for (let j = 0; j < 25; j++) {
        for (let k = 0; k < 50; k++) {
            assert.commandWorked(writeColl2.insertOne({_id: i, a: 2 + i}));
            i += 1;
        }
        sleep(200);
    }

    // Give some time for things to settle down
    sleep(5000);

    assert.soon(() => {
        const verboseStats = getStats(processorName);
        // jsTestLog(verboseStats);
        assert.eq(verboseStats["ok"], 1);
        const startingPoint = verboseStats['changeStreamState'];
        assert(startingPoint);
        const startingPointLag = verboseStats['changeStreamTimeDifferenceSecs'];
        return startingPointLag == 0;
    });

    stopStreamProcessor(processorName);
    clearState();
}

testChangeStreamSourceLagStat();

// TODO SERVER-77657: add a test that verifies that stop() works when a continuous
//  stream of events is flowing through $source.

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);

(function changeStreamCycle() {
    const inDB = "db";
    const inColl = "coll";
    db.getSiblingDB(inDB).dropDatabase();
    let sourceSpec = {
        $source: {
            connectionName: connectionName,
            db: inDB,
            coll: inColl,
            timeField: {$toDate: "$fullDocument.otherTimeField"},
            config: {fullDocument: "required"}
        }
    };
    let replaceRootStage = {$replaceRoot: {newRoot: "$fullDocument"}};
    let updateStage = {$set: {a: {$add: ["$a", 1]}}};
    let projectStage = {$project: {"_stream_meta": 0}};
    let mergeStage = {
        $merge: {
            into: {connectionName: connectionName, db: inDB, coll: inColl},
            on: "_id",
            whenMatched: "merge",
            whenNotMatched: "insert",
        }
    };
    const outputColl = db.getSiblingDB(outputDB)[outputCollName];
    outputColl.drop();
    const processorName = "sp1";
    sp.createStreamProcessor(processorName,
                             [sourceSpec, replaceRootStage, updateStage, projectStage, mergeStage]);
    const processor = sp[processorName];
    assert.commandWorked(processor.start({
        dlq: {
            connectionName: connectionName,
            db: inDB,
            coll: 'dlq',
        },
        featureFlags: {}
    }));
    db.getSiblingDB(inDB)[inColl].drop();
    db.getSiblingDB(inDB).createCollection(inColl, {changeStreamPreAndPostImages: {enabled: true}});
    let writeColl = db.getSiblingDB(inDB)[inColl];
    let dlqColl = db.getSiblingDB(inDB)["dlq"];
    assert.commandWorked(writeColl.insert({_id: 1, a: 1, otherTimeField: Date.now()}));
    let res;
    assert.soon(() => {
        res = writeColl.find().toArray();
        jsTestLog(res);
        let dlqres = dlqColl.find().toArray();
        if (dlqres.length > 0) {
            jsTestLog(dlqres);
        }
        let stats = processor.stats();
        jsTestLog(stats);
        return res[0]["a"] == 5;
    });

    assert.commandWorked(processor.stop());
}());

function createChangestreamSourceProcessor(processorName, sourceSpecOverrides) {
    let sourceSpec = {
        connectionName: connectionName,
        db: writeDBOne,
        coll: writeCollOne,
    };

    sourceSpec = {...sourceSpec, ...sourceSpecOverrides};

    sp.createStreamProcessor(processorName,
                             [{$source: sourceSpec}, {$emit: {connectionName: '__testMemory'}}]);

    return sp[processorName];
}

function testChangeStreamOnTimeseries() {
    clearState();

    const timeseriesCollName = "timeseriesColl";
    assert.commandWorked(db.getSiblingDB(writeDBOne).createCollection(timeseriesCollName, {
        timeseries: {
            timeField: "timestamp",
            metaField: "metadata",
            granularity: "seconds",
        },
    }));

    let sourceSpecOverrides = {
        coll: timeseriesCollName,
    };

    const processorName = "changeStreamSourceProcessor";
    const processor = createChangestreamSourceProcessor(processorName, sourceSpecOverrides);
    const startResult = processor.start({featureFlags: {}, shouldStartSample: true}, false);
    assert.commandFailedWithCode(startResult, ErrorCodes.StreamProcessorInvalidOptions);
    assert.eq(startResult.errorLabels[0], "StreamProcessorUserError");
}

testChangeStreamOnTimeseries();

function runChangeStreamSourceTestFailOnStart(
    sourceSpecOverrides, expectedErrCode, expectedErrLabels = []) {
    clearState();

    const processorName = "changeStreamSourceProcessor";
    const processor = createChangestreamSourceProcessor(processorName, sourceSpecOverrides);

    let startResult = processor.start({featureFlags: {}, shouldStartSample: true}, false);
    assert.commandFailedWithCode(startResult, expectedErrCode);
    assert.eq(startResult.errorLabels.length, expectedErrLabels.length);
    for (const label of expectedErrLabels) {
        startResult.errorLabels.includes(label);
    }
}

// Should fail due to unsupported operator in the source's aggregation pipeline
runChangeStreamSourceTestFailOnStart(
    {config: {pipeline: [{$someStageThatDoesNotExist: {}}]}},
    ErrorCodes.StreamProcessorInvalidOptions,
    ["StreamProcessorUserError"],
);

// Should fail due to empty stage specification in source's aggregation pipeline
runChangeStreamSourceTestFailOnStart(
    {config: {pipeline: [{}]}},
    ErrorCodes.StreamProcessorInvalidOptions,
    ["StreamProcessorUserError"],
);

// Should fail due to stage specification having more than one field in source's aggregation
// pipeline
runChangeStreamSourceTestFailOnStart(
    {
        config: {
            pipeline: [{
                $match: {"fullDocument.a": 33},
                $addFields: {"someField": "someValue"},
            }]
        }
    },
    ErrorCodes.StreamProcessorInvalidOptions,
    ["StreamProcessorUserError"],
);

function runChangeStreamSourceTestFailOnInsert(sourceSpecOverrides, expectedErrCode, isUserError) {
    clearState();

    const processorName = "changeStreamSourceProcessor";
    const processor = createChangestreamSourceProcessor(processorName, sourceSpecOverrides);
    processor.start({featureFlags: {}, shouldStartSample: true});

    performWrites();
    let spListResult;
    assert.soon(() => {
        let listResult = listStreamProcessors();
        assert.eq(listResult["ok"], 1, listResult);
        spListResult = listResult.streamProcessors.find((item) => item.name == processorName);
        return spListResult.status == "error";
    });

    processor.stop();

    assert.eq(spListResult?.error.code, expectedErrCode);
    assert.eq(spListResult?.error.userError, isUserError);
}

// Should fail on write due to modifying changestream resume token
runChangeStreamSourceTestFailOnInsert(
    {config: {pipeline: [{$replaceRoot: {newRoot: {_id: 123}}}]}},
    ErrorCodes.StreamProcessorInvalidOptions,
    true,
);
