/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {waitForCount, waitForDoc} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function generateInput(size, msPerDocument = 1) {
    let input = [];
    let baseTs = ISODate("2023-01-01T00:00:00.000Z");
    for (let i = 0; i < size; i++) {
        let ts = new Date(baseTs.getTime() + msPerDocument * i);
        input.push({
            ts: ts,
            idx: i,
            a: 1,
        });
    }
    return input;
}

function uuidStr() {
    return UUID().toString().split('"')[1];
}

function removeProjections(doc) {
    delete doc._ts;
    delete doc._stream_meta;
}

class TestHelper {
    constructor(input, middlePipeline, interval = 0, sourceType = "kafka") {
        this.sourceType = sourceType;
        this.input = input;
        this.uri = 'mongodb://' + db.getMongo().host;
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = true;
        this.kafkaTopic = "topic1";
        this.dbConnectionName = "db1";
        this.dbName = "test";
        this.dlqCollName = uuidStr();
        this.inputCollName = uuidStr();
        this.outputCollName = uuidStr();
        this.processorId = uuidStr();
        this.tenantId = uuidStr();
        this.outputColl = db.getSiblingDB(this.dbName)[this.outputCollName];
        this.inputColl = db.getSiblingDB(this.dbName)[this.inputCollName];
        this.dlqColl = db.getSiblingDB(this.dbName)[this.dlqCollName];
        this.spName = uuidStr();
        this.checkpointCollName = uuidStr();
        this.checkpointColl = db.getSiblingDB(this.dbName)[this.checkpointCollName];
        this.checkpointIntervalMs = null;  // Use the default.
        if (interval !== null) {
            this.checkpointIntervalMs = NumberInt(interval);
        }
        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: {
                storage: {
                    uri: this.uri,
                    db: this.dbName,
                    coll: this.checkpointCollName,
                },
                debugOnlyIntervalMs: this.checkpointIntervalMs,
            },
        };
        this.connectionRegistry = [
            {name: this.dbConnectionName, type: 'atlas', options: {uri: this.uri}},
            {
                name: this.kafkaConnectionName,
                type: 'kafka',
                options: {
                    bootstrapServers: this.kafkaBootstrapServers,
                    isTestKafka: this.kafkaIsTest,
                },
            }
        ];

        this.pipeline = [];
        if (this.sourceType === 'kafka') {
            this.pipeline.push({
                $source: {
                    connectionName: this.kafkaConnectionName,
                    topic: this.kafkaTopic,
                    testOnlyPartitionCount: NumberInt(1),
                    timeField: {
                        $toDate: "$ts",
                    }
                }
            });
        } else {
            this.pipeline.push({
                $source: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.inputCollName,
                    timeField: {
                        $toDate: "$fullDocument.ts",
                    }
                }
            });
        }
        for (let stage of middlePipeline) {
            this.pipeline.push(stage);
        }
        this.pipeline.push({
            $merge: {
                into: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.outputCollName
                }
            }
        });
        this.sp = new Streams(this.connectionRegistry);
    }

    // Helper functions.
    run(firstStart = true) {
        this.sp.createStreamProcessor(this.spName, this.pipeline);
        this.sp[this.spName].start(this.startOptions, this.processorId, this.tenantId);
        if (this.sourceType === 'kafka') {
            // Insert the input.
            assert.commandWorked(db.runCommand({
                streams_testOnlyInsert: '',
                name: this.spName,
                documents: this.input,
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input));
        }
    }

    stop(assertWorked = true) {
        this.sp[this.spName].stop(assertWorked);
    }

    getCheckpointIds() {
        return this.checkpointColl.find({_id: {$regex: "^checkpoint"}}).sort({_id: -1}).toArray();
    }

    getResults() {
        return this.outputColl.find({}).sort({idx: 1}).toArray();
    }

    getStartOffsetFromCheckpoint(checkpointId) {
        let sourceState =
            this.checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        if (this.sourceType === 'kafka') {
            return sourceState[0]["state"]["partitions"][0]["offset"];
        } else {
            throw 'only supported with kafka source';
        }
    }

    getSourceState(checkpointId) {
        let sourceState =
            this.checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        return sourceState[0];
    }

    getStartingPointFromCheckpoint(checkpointId) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }
        // This is a variant<object, Timestamp> corresponding to a resumeToken or
        // timestamp.
        let state = this.getSourceState(checkpointId)["state"]["startingPoint"];
        if (state instanceof Timestamp) {
            return {
                resumeToken: null,
                startAtOperationTime: state,
            };
        } else {
            return {resumeToken: state, startAtOperationTime: null};
        }
    }

    getNextEvent(resumeAfterToken) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        const stream = this.inputColl.watch([], {resumeAfter: resumeAfterToken});
        if (stream.hasNext()) {
            return stream.next();
        } else {
            return null;
        }
    }

    errStr() {
        return `checkpointCollName: ${this.checkpointCollName}`;
    }
}

function smokeTestCorrectness() {
    const input = generateInput(2000);
    let test = new TestHelper(input, []);

    // Run the streamProcessor for the first time.
    test.run();
    // Wait until the last doc in the input appears in the output collection.
    waitForDoc(test.outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
    test.stop();
    // Verify the output matches the input.
    let results = test.getResults();
    assert.eq(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }
    // Get the checkpoint IDs from the run.
    let ids = test.getCheckpointIds();
    // Note: this minimum is an implementation detail subject to change.
    // KafkaConsumerOperator has a maximum docs per run, hardcoded to 500.
    // Since the checkpoint interval is 0, we should see at least see a checkpoint
    // after every batch of 500 docs.
    const maxDocsPerRun = 500;
    const expectedMinimumCheckpoints = input.length / maxDocsPerRun;
    assert.gte(ids.length,
               expectedMinimumCheckpoints,
               `expected at least ${expectedMinimumCheckpoints} checkpoints`);

    // Replay from each written checkpoint and verify the results are expected.
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        const startingOffset = test.getStartOffsetFromCheckpoint(id._id);
        const expectedOutputCount = input.length - startingOffset;

        // Clean the output.
        test.outputColl.deleteMany({});
        // Run the streamProcessor.
        test.run();
        if (expectedOutputCount > 0) {
            // If we're expected to output anything from this checkpoint,
            // wait for the last document in the input.
            waitForDoc(
                test.outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
            replaysWithData++;
        } else {
            // Else, just sleep 5 seconds, we will verify nothing is output after this.
            sleep(1000 * 5);
        }
        test.stop();
        // Verify the results.
        let results = test.getResults();
        // ex. if the checkpoint starts at offset 500, we verify the output
        // to contain input[500] to input[length - 1].
        assert.eq(results.length, expectedOutputCount);
        for (let i = 0; i < results.length; i++) {
            assert.eq(input.length[i + startingOffset], removeProjections(results[i]));
        }
        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        test.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id._id), $not: /^operator/}});
    }
    assert.gt(replaysWithData, 0);
}

function smokeTestCorrectnessTumblingWindow() {
    const maxDocsPerRun = 500;
    const inputSize = 10 * maxDocsPerRun;
    const msPerDoc = 1;
    const windowInterval = {size: NumberInt(1), unit: "second"};
    const windowSizeMs = 1000;
    const allowedLatenessInteval = {size: NumberInt(0), unit: "second"};
    const docsPerWindow = windowSizeMs / msPerDoc;
    const expectedWindowCount = Math.floor(inputSize * msPerDoc / windowSizeMs);
    const input = generateInput(inputSize + 2, msPerDoc);
    const pipeline = [
        {
            $tumblingWindow: {
                interval: windowInterval,
                allowedLateness: allowedLatenessInteval,
                pipeline: [{
                    $group: {
                        _id: null,
                        sum: {$sum: "$a"},
                        minIdx: {$min: "$idx"},
                        maxIdx: {$max: "$idx"}
                    }
                }]
            }
        },
        {$project: {_id: {$toString: "$minIdx"}}}
    ];
    let test = new TestHelper(input, pipeline);

    test.run();
    waitForCount(test.outputColl, expectedWindowCount, 60);
    test.stop();
    let originalResults = test.getResults();
    assert.eq(expectedWindowCount, originalResults.length);
    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, `expected some checkpoints`);

    // Replay from each written checkpoint and verify the results are expected.
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        const startingOffset = test.getStartOffsetFromCheckpoint(id._id);
        const expectedInputCount = input.length - startingOffset;
        const expectedOutputCount = Math.floor(expectedInputCount / docsPerWindow);
        // Clean the output.
        test.outputColl.deleteMany({});
        // Run the streamProcessor.
        test.run();
        if (expectedOutputCount > 0) {
            // Wait for the expected output to arrive.
            waitForCount(test.outputColl, expectedOutputCount);
            replaysWithData++;
        } else {
            // Else, just sleep 5 seconds, we will verify nothing is output after this.
            sleep(1000 * 5);
        }
        test.stop();
        // Verify the results.
        let results = test.getResults();
        assert.eq(results.length, expectedOutputCount);
        assert.lte(results.length, originalResults.length);
        for (let i = 0; i < results.length; i++) {
            let originalResult = originalResults[originalResults.length - 1 - i];
            let restoreResult = results[results.length - 1 - i];
            assert.eq(originalResult, restoreResult);
        }
        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        test.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id._id), $not: /^operator/}});
    }
    assert.gt(replaysWithData, 0);
}

function smokeTestCheckpointOnStop() {
    // Use a large input so it is not finished by the time we call stop.
    const input = generateInput(330000);
    // Use a long checkpoint interval so they don't automatically happen.
    let test = new TestHelper(input, [], /* interval */ 1000000000);
    // Run the streamProcessor the streamProcessor.
    test.run();
    // Wait until some output appears in the output collection.
    waitForCount(test.outputColl, 1, /* maxWaitSeconds */ 60);
    test.stop();
    // Verify we did not output everything.
    let results = test.getResults();
    assert.lt(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }
    // Get the checkpoint IDs from the run.
    let ids = test.getCheckpointIds();
    // There should be 1 checkpoint for the start of a fresh streamProcessor,
    // 1 checkpoint from the stop.
    assert.eq(2, ids.length);
}

function smokeTestCorrectnessChangestream() {
    const input = generateInput(503);
    let test = new TestHelper(input, [], 0, 'changestream');

    test.run();
    // Wait until the last doc in the input appears in the output collection.
    waitForCount(test.outputColl, input.length, /* maxWaitSeconds */ 60);
    test.stop();
    // Verify the output matches the input.
    let results = test.getResults();
    assert.eq(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }

    // Get the checkpoint IDs from the run.
    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0);
    jsTestLog(ids);

    // Replay from each written checkpoint and verify the results are expected.
    let firstNewIdx = input.length;
    let newIdx = firstNewIdx;
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        let expectedMinIdx;
        let startingPoint = test.getStartingPointFromCheckpoint(id._id);
        assert.neq(null, startingPoint);
        if (startingPoint.resumeToken != null) {
            let expectedFirstEvent = test.getNextEvent(startingPoint.resumeToken);
            expectedMinIdx =
                expectedFirstEvent == null ? firstNewIdx : expectedFirstEvent.fullDocument.idx;
        } else {
            assert.neq(null, startingPoint.startAtOperationTime);
            expectedMinIdx = 0;
        }
        let details = {
            checkpointId: id._id,
            resumeToken: startingPoint.resumeToken,
            testUtils: test.errStr(),
            ids: ids,
            sourceState: test.getSourceState(id._id),
        };

        // Clean the output.
        assert.commandWorked(test.outputColl.deleteMany({}));
        // Run the streamProcessor.
        test.run(false);
        // Insert a new doc and wait for it to show up.
        assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: newIdx}));
        waitForDoc(
            test.outputColl, (doc) => doc.fullDocument.idx == newIdx, /* maxWaitSeconds */ 60);
        test.stop();
        // Verify the results.
        let results = test.getResults();
        if (results.length > 1) {
            replaysWithData++;
        }
        let expectedMaxIdx = newIdx;
        // Get the actual min/max idx values in the output.
        let min = null;
        for (let doc of results) {
            if (min === null || doc.fullDocument.idx < min) {
                min = doc.fullDocument.idx;
            }
        }
        let max = null;
        for (let doc of results) {
            if (max === null || doc.fullDocument.idx > max) {
                max = doc.fullDocument.idx;
            }
        }
        assert.eq(expectedMinIdx, min, details);
        assert.eq(expectedMaxIdx, max, details);

        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        assert.commandWorked(test.checkpointColl.deleteMany(
            {_id: {$nin: ids.map(id => id._id), $not: /^operator/}}));
        newIdx += 1;
    }
    assert.gt(replaysWithData, 0);
}

function failPointTestAfterFirstOutput() {
    try {
        assert.commandWorked(db.adminCommand(
            {'configureFailPoint': 'failAfterRemoteInsertSucceeds', 'mode': 'alwaysOn'}));
        const input = generateInput(200);
        let test = new TestHelper(input, [], 99999999 /* long interval */, 'changestream');
        test.sp.createStreamProcessor(test.spName, test.pipeline);
        assert.commandWorked(
            test.sp[test.spName].start(test.startOptions, test.processorId, test.tenantId));
        // The streamProcessor will crash after the first document is output.
        assert.commandWorked(test.inputColl.insertMany(test.input));
        waitForCount(test.checkpointColl, 2);
        // Verify there is 1 checkpoint document committed.
        assert.eq(1, test.checkpointColl.find({_id: {$regex: "^checkpoint"}}).toArray().length);
        // Verify there is 1 operator state document for the $source.
        assert.eq(1, test.checkpointColl.find({_id: {$regex: "^operator"}}).toArray().length);
        let checkpointId = test.getCheckpointIds()[0]._id;
        // Verify the first checkpoint for a changestream $source uses a Timestamp.
        let startingPoint = test.getStartingPointFromCheckpoint(checkpointId).startAtOperationTime;
        assert(startingPoint instanceof Timestamp);
        // Sleep for a while and verify the first document in the input is in the output collection.
        sleep(5000);
        assert.eq(
            input[0].idx,
            test.outputColl.find({"fullDocument.idx": input[0].idx}).toArray()[0].fullDocument.idx);
        // Stop the streamProcessor that failed. We don't assert success
        // here, there is a small chance the StreamManager has already cleaned this erroring
        // streamProcessor up.
        test.stop(false /* assertWorked */);
        // Clean the output.
        assert.commandWorked(test.outputColl.deleteMany({}));
        assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: input.length}));
        // This will start the streamProcessor ~5 seconds after the inserts
        // above. But since we should be resuming from a checkpoint, this streamProcessor
        // should still pick up the very first inserted event.
        test.run(false);
        assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: input.length + 1}));
        waitForCount(test.outputColl, /* count */ 1);
        assert.eq(
            input[0].idx,
            test.outputColl.find({"fullDocument.idx": input[0].idx}).toArray()[0].fullDocument.idx);
        // TODO(SERVER-79801): Remove this sleep once the deadlock is fixed.
        sleep(3000);
        test.stop(false /* assertWorked */);
    } finally {
        assert.commandWorked(db.adminCommand(
            {'configureFailPoint': 'failAfterRemoteInsertSucceeds', 'mode': 'off'}));
    }
}

function smokeTestStatsInCheckpoint() {
    const input = generateInput(503);
    let test =
        new TestHelper(input, [] /* pipeline */, null /* default interval */, 'changestream');
    test.run();
    // Wait until the last doc in the input appears in the output collection.
    waitForCount(test.outputColl, input.length, /* maxWaitSeconds */ 60);
    assert.soon(() => input.length == test.sp[test.spName].stats(false).inputMessageCount);
    // Stop the streamProcessor, which will write a checkpoint.
    test.stop();
    // Restart the stream processor.
    test.run(false);
    let stats = test.sp[test.spName].stats(false);
    assert.eq(input.length, stats.inputMessageCount);
    assert.eq(input.length, stats.outputMessageCount);
    // Re-insert the input.
    assert.commandWorked(test.inputColl.insertMany(input));
    // Verify the stats are updated.
    waitForCount(test.outputColl, input.length * 2, /* maxWaitSeconds */ 60);
    assert.soon(() => input.length * 2 == test.sp[test.spName].stats(false).inputMessageCount);
    assert.soon(() => input.length * 2 == test.sp[test.spName].stats(false).outputMessageCount);
    // Restart the stream processor.
    test.stop();
    test.run(false);
    // Verify the stats were persisted.
    stats = test.sp[test.spName].stats(false);
    assert.eq(input.length * 2, stats.inputMessageCount);
    assert.eq(input.length * 2, stats.outputMessageCount);
    test.stop();
}

function smokeTestEmptyChangestream() {
    // In SERVER-80668 we found a bug where an changestream $source with zero events after start
    // would cause stop to hang with checkpointing enabled. This test covers that scenario.
    let test = new TestHelper(
        [] /* empty input */, [] /* pipeline */, null /* default interval */, 'changestream');
    test.run();
    test.stop();
    test.run(false);
    test.stop();
}

// Validates that the resume token advances for a changestream $source,
// even when the collection changestream is empty.
function emptyChangestreamResumeTokenAdvances() {
    let test = new TestHelper([] /* empty input */, [] /* pipeline */, 0, 'changestream');
    test.run();
    // Slowly insert events into another collection on the same database so there will
    // be new resume tokens to advance to.
    let otherColl = db.getSiblingDB(test.dbName)["otherCollection"];
    let waitForNumCheckpoints = 5;
    let i = 0;
    otherColl.insert({a: i++});
    while (test.getCheckpointIds().length < waitForNumCheckpoints) {
        otherColl.insert({a: i++});
        sleep(100);
    }
    test.stop();

    let ids = test.getCheckpointIds();
    assert.gte(ids.length, waitForNumCheckpoints);
    // Verify we have at least 2 unique resume tokens in our checkpoints.
    let resumeTokens = ids.map(id => test.getStartingPointFromCheckpoint(id._id))
                           .filter(startingPoint => startingPoint.resumeToken != null);
    assert.gte(new Set(resumeTokens).size, 2);
}

smokeTestCorrectness();
smokeTestCorrectnessTumblingWindow();
smokeTestCheckpointOnStop();
smokeTestCorrectnessChangestream();
failPointTestAfterFirstOutput();
smokeTestStatsInCheckpoint();
smokeTestEmptyChangestream();
emptyChangestreamResumeTokenAdvances();
