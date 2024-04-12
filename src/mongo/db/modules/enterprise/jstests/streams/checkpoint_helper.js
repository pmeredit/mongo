/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    documentEq,
} from "jstests/aggregation/extras/utils.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    LocalDiskCheckpointUtil,
    waitForCount,
    waitWhenThereIsMoreData
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

export function uuidStr() {
    return UUID().toString().split('"')[1];
}

export function removeProjections(doc) {
    delete doc._ts;
    delete doc._stream_meta;
    delete doc._id;
    return doc;
}

export class TestHelper {
    constructor(input,
                middlePipeline,
                interval = 0,
                sourceType = "kafka",
                useNewCheckpointing = true,
                spId = null,
                writeDir = null,
                restoreDir = null,
                dbForTest = null,
                targetSourceMergeDb = null) {
        assert(useNewCheckpointing);
        this.sourceType = sourceType;
        this.input = input;
        // By default use the global db.
        this.targetSourceMergeDb = db;
        if (targetSourceMergeDb != null) {
            this.targetSourceMergeDb = targetSourceMergeDb;
        }
        this.uri = 'mongodb://' + this.targetSourceMergeDb.getMongo().host;
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = true;
        this.kafkaTopic = "topic1";
        this.dbConnectionName = "db1";
        this.dbName = "test";
        this.dlqCollName = uuidStr();
        this.inputCollName = uuidStr();
        this.outputCollName = uuidStr();
        // By default use the global db.
        this.db = db;
        if (dbForTest != null) {
            this.db = dbForTest;
        }
        if (spId == null) {
            this.processorId = uuidStr();
        } else {
            this.processorId = spId;
        }
        this.tenantId = "jstests-tenant";
        this.outputColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.outputCollName];
        this.inputColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.inputCollName];
        this.dlqColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.dlqCollName];
        this.spName = uuidStr();
        this.checkpointIntervalMs = null;  // Use the default.

        this.useNewCheckpointing = useNewCheckpointing;
        if (writeDir == null) {
            this.writeDir = "/tmp/checkpoint";
        } else {
            this.writeDir = writeDir;
        }
        if (restoreDir == null) {
            this.restoreDir = "/tmp/checkpoint";
        } else {
            this.restoreDir = restoreDir;
        }
        this.spWriteDir = `${this.writeDir}/${this.tenantId}/${this.processorId}`;
        this.spRestoreDir = `${this.restoreDir}/${this.tenantId}/${this.processorId}`;
        mkdir(this.writeDir);
        mkdir(this.restoreDir);

        if (interval !== null) {
            this.checkpointIntervalMs = NumberInt(interval);
        }

        let checkpointOptions = {
            debugOnlyIntervalMs: this.checkpointIntervalMs,
        };
        checkpointOptions.storage = null;
        checkpointOptions.localDisk = {writeDirectory: this.writeDir};

        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: checkpointOptions
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
            },
            {
                name: '__testMemory',
                type: 'in_memory',
                options: {},
            }
        ];

        // Setup the pipeline.
        this.pipeline = [];
        // First, append either a kafka or changestream source.
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
        } else if (this.sourceType === 'memory') {
            this.pipeline.push(
                {$source: {connectionName: '__testMemory', timeField: {$toDate: "$ts"}}});
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
                },
            }
        });
        this.sp = new Streams(this.connectionRegistry, this.db);
        this.checkpointUtil = new LocalDiskCheckpointUtil(
            this.writeDir, this.tenantId, this.processorId, this.spName);
    }

    // Helper functions.
    run(firstStart = true) {
        this.startFromLatestCheckpoint();
        if (this.sourceType === 'kafka' || this.sourceType === 'memory') {
            // Insert the input.
            assert.commandWorked(this.db.runCommand({
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

    startFromLatestCheckpoint(assertWorked = true) {
        // Set the restore directory to the latest committed checkpoint on disk.
        let idsOnDisk = this.getCheckpointIds();
        if (idsOnDisk.length > 0) {
            this.startOptions.checkpointOptions.localDisk.restoreDirectory =
                `${this.restoreDir}/${this.tenantId}/${this.processorId}/${idsOnDisk[0]}`;
        } else {
            this.startOptions.checkpointOptions.localDisk.restoreDirectory = null;
        }

        this.sp.createStreamProcessor(this.spName, this.pipeline);
        jsTestLog(`Starting ${this.spName} with pipeline ${this.pipeline}, options ${
            tojson(this.startOptions)}`);
        return this.sp[this.spName].start(
            this.startOptions, this.processorId, this.tenantId, assertWorked);
    }

    stop(assertWorked = true) {
        this.sp[this.spName].stop(assertWorked);
    }

    stats() {
        return this.sp[this.spName].stats();
    }

    checkpoint(force = false) {
        return this.sp[this.spName].checkpoint(force);
    }

    // returns the commit checkpoint IDs
    // the 0-th index of the returned array is the most recently committed checkpoint
    getCheckpointIds() {
        if (!pathExists(this.spWriteDir)) {
            return [];
        }
        let ids =
            listFiles(this.spWriteDir)
                // Get the committed checkpoint directories by looking for manifest files.
                .filter((checkpointDir) => {
                    let manifestFile =
                        listFiles(checkpointDir.name).find(file => file.name.includes("MANIFEST"));
                    return manifestFile !== null;
                })
                // Retrieve the checkpointID from each directory.
                .map(checkpointDir => checkpointDir.name.split('/').pop());
        // Sort by checkpoint ID.
        ids.sort();
        // Reverse the array so the most recently committed checkpoint ID is at index 0.
        return ids.reverse();
    }

    getResults(changeStreamEvent = false) {
        if (changeStreamEvent) {
            return this.outputColl.find({}).sort({"fullDocument.idx": 1}).toArray();
        } else {
            return this.outputColl.find({}).sort({idx: 1}).toArray();
        }
    }

    getStartOffsetFromCheckpoint(checkpointId, useLogLineDuringRestore = false) {
        assert(this.sourceType === 'kafka', "only valid to call for kafka source type");
        jsTestLog(`Getting start offset from ${checkpointId}`);
        const restoreCheckpoint = this.getCheckpointDescription(checkpointId);
        return restoreCheckpoint.sourceState.partitions[0].offset;
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

    getCheckpointDescription(checkpointId) {
        const res = this.sp.listStreamProcessors(true /* verbose */).streamProcessors;
        const filter = res.filter(x => x.name == this.spName);
        assert(filter.length == 1, `Expected to find a single SP of name ${this.spName}`);
        const spResponse = filter[0];
        const verbose = spResponse["verboseStatus"];
        const restoreCheckpoint = verbose["restoredCheckpoint"];
        assert(restoreCheckpoint != null, `Expected a restoredCheckpoint`);
        assert.eq(restoreCheckpoint.id, checkpointId);
        return restoreCheckpoint;
    }

    getStartingPointFromCheckpoint(checkpointId) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        const restoreCheckpoint = this.getCheckpointDescription(checkpointId);
        const startingPoint = restoreCheckpoint.sourceState.startingPoint;
        if (startingPoint instanceof Timestamp) {
            return {resumeToken: null, startAtOperationTime: startingPoint};
        } else {
            return {resumeToken: startingPoint, startAtOperationTime: null};
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
        return `checkpointWriteDir: ${this.writeDir}`;
    }

    removeCheckpointsNotInList(ids) {
        let idsOnDisk = listFiles(this.spWriteDir).map(f => f.name.split('/').pop());
        for (let idOnDisk of idsOnDisk) {
            if (!ids.includes(idOnDisk)) {
                let path = `${this.spWriteDir}/${idOnDisk}`;
                removeFile(path);
            }
        }
    }
}

/**
 * CheckpointTestHelper adds a utility method
 * to send partial input.
 */
class CheckPointTestHelper extends TestHelper {
    constructor(inputDocs,
                pipeline,
                interval,
                sourceType = "kafka",
                useNewCheckpointing = false,
                spId = null,
                writeDir = null,
                restoreDir = null) {
        super(inputDocs,
              pipeline,
              interval,
              sourceType,
              useNewCheckpointing,
              spId,
              writeDir,
              restoreDir);
    }
    runWithInputSlice(endRange, firstStart = true) {
        this.sp.createStreamProcessor(this.spName, this.pipeline);
        this.sp[this.spName].start(this.startOptions, this.processorId, this.tenantId);
        if (this.sourceType === 'kafka') {
            // Insert the input.
            assert.commandWorked(this.db.runCommand({
                streams_testOnlyInsert: '',
                name: this.spName,
                documents: this.input.slice(0, endRange),
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input[0..endRange]));
        }
    }
}

/**
 * run the stream processor and run the provided window pipeline
 * returns the results to be used for comparison
 * @param {*} inputDocs list of documents
 * @param {*} middlePipeline the window pipeline
 * @returns
 */
function runTestsWithoutCheckpoint(inputDocs, middlePipeline) {
    // get the original results for the inputDocs
    var test = new TestHelper(inputDocs, middlePipeline, 10000000, "kafka", true);
    test.run();
    waitForCount(test.outputColl, 1, 60);
    waitWhenThereIsMoreData(test.outputColl);
    // TODO -- may need to improve so that we can wait for complete output
    test.stop();
    let originalResults = test.getResults();
    for (let i = 0; i < originalResults.length; i++) {
        originalResults[i] = removeProjections(originalResults[i]);
    }
    assert.gt(originalResults.length, 0);
    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, `expected some checkpoints`);
    test.outputColl.deleteMany({});  // delete output data
    return originalResults;
}

/**
 * Collects the results without a checkpoint
 * and compares with results obtained by taking checkpoint in
 * the middle
 * @param {*} inputDocs list of documents
 * @param {*} middlePipeline the window pipeline
 */
export function checkpointInTheMiddleTest(
    inputDocs, middlePipeline, compareFunc, intermediateStateDumpDir = null) {
    const originalResults = runTestsWithoutCheckpoint(inputDocs, middlePipeline);

    var test2 = new CheckPointTestHelper(inputDocs, middlePipeline, 10000000, "kafka", true);
    // now split the input, stop the run in the middle, continue and verify the output is same
    jsTestLog(`input docs size=${inputDocs.length}`);
    var randomPoint = Random.randInt(inputDocs.length / 2);
    randomPoint = randomPoint + inputDocs.length / 4;
    randomPoint = Math.floor(randomPoint);
    jsTestLog(`random point=${randomPoint}`);
    assert.gt(randomPoint, 0);
    assert.gt(randomPoint, inputDocs.length / 4 - 1);
    assert.lt(randomPoint, inputDocs.length);
    assert.gt(randomPoint, 0);
    test2.runWithInputSlice(randomPoint);
    test2.stop();  // this will force the checkpoint
    let ids2 = test2.getCheckpointIds();
    assert.eq(ids2.length, 2, `expected some checkpoints`);  // not sure why we get 2 checkpoints.
    const id = ids2[0];

    if (intermediateStateDumpDir != null) {
        // Save state that can then be used in a backwards compat test to verify
        // that resuming from a checkpoint works
        jsTestLog("intermediateStateDump: Starting offset=" + startingOffset +
                  "; checkpointId=" + id);
        mkdir(intermediateStateDumpDir);
        // eslint-disable-next-line
        writeBsonArrayToFile(intermediateStateDumpDir + "/inputDocs.bson", inputDocs);
        // eslint-disable-next-line
        writeBsonArrayToFile(intermediateStateDumpDir + "/expectedResults.bson", originalResults);
        // eslint-disable-next-line
        copyDir(test2.spWriteDir + "/" + id, intermediateStateDumpDir);
    }
    // Run the streamProcessor.
    test2.run();
    const startingOffset = test2.getStartOffsetFromCheckpoint(id);
    assert(startingOffset > 0 && startingOffset <= randomPoint);
    waitForCount(test2.outputColl, originalResults.length, 60);
    waitWhenThereIsMoreData(test2.outputColl);
    test2.stop();
    let results = test2.getResults();
    for (let i = 0; i < results.length; i++) {
        results[i] = removeProjections(results[i]);
    }
    // we only compare the results we read originally
    // this could be less than full results
    for (let i = 0; i < originalResults.length; i++) {
        compareFunc(results[i], originalResults[i]);
    }
}

export function resumeFromCheckpointTest(testDir, spid, windowPipeline, expectedStartOffset) {
    let inputDocs = _readDumpFile(testDir + "/inputDocs.bson");
    let expectedResults = _readDumpFile(testDir + "/expectedResults.bson");

    var test2 = new CheckPointTestHelper(
        inputDocs, windowPipeline, 10000000, "kafka", true, spid, testDir, testDir);
    let ids = test2.getCheckpointIds();
    assert.eq(ids.length, 1, "expected one checkpoint");
    const id = ids[0];

    test2.run();
    assert.soon(() => { return test2.outputColl.find({}).count() == expectedResults.length; });

    const startingOffset = test2.getStartOffsetFromCheckpoint(id, true);
    assert.eq(startingOffset, expectedStartOffset);
    test2.stop();

    let results = test2.getResults();
    assert.eq(results.length, expectedResults.length);

    var r = new Set();
    for (let i = 0; i < results.length; i++) {
        results[i] = removeProjections(results[i]);
        for (let j = 0; j < expectedResults.length; j++) {
            if (documentEq(results[i], expectedResults[j])) {
                if (r.has(j)) {
                    continue;
                } else {
                    r.add(j);
                    break;
                }
            }
        }
    }
    assert.eq(r.size, results.length);
}