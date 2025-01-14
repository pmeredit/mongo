/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {Thread} from "jstests/libs/parallelTester.js";
import {
    getDefaultSp,
    Streams
} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    TEST_TENANT_ID,
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

// Used in commonTest to force the next document to be in a new batch after previous documents
// have all been processed.
export const batchBreakerField = "__batchBreaker";
export function makeBatchBreakerDoc() {
    let doc = {};
    doc[batchBreakerField] = 1;
    return doc;
}
// Makes comparisons easier in commonTest.
export function sanitizeDlqDoc(d) {
    delete d["_stream_meta"];
    delete d["processorName"];
    delete d["dlqTime"];
    delete d["doc"]["_ts"];
    delete d["doc"]["_stream_meta"];
    return d;
}

/**
 * Helper function to test that the pipeline returns the expectedOutput for the given input.
 * First, a test source is used.
 * Then, a change stream source is used, and there is a checkpoint in the middle.
 */
export function commonTest({input, pipeline, expectedOutput, timeField = "$ts", expectedDlq = []}) {
    // Test with a test source and no checkpoints.
    const sp = getDefaultSp();
    const documentsSourceInput = input.filter(d => !d.hasOwnProperty(batchBreakerField));
    const output = sp.process([
        {$source: {documents: documentsSourceInput, timeField: timeField}},
        ...pipeline,
    ]);
    const dlqMessageField = "_dlqMessage";
    const dlqOutput = output.filter(d => d.hasOwnProperty(dlqMessageField))
                          .map(d => sanitizeDlqDoc(d[dlqMessageField]));
    const actualOutput = output.filter(d => !d.hasOwnProperty(dlqMessageField));
    assert(resultsEq(actualOutput, expectedOutput, true /* verbose */));
    assert(resultsEq(dlqOutput, expectedDlq, true /* verbose */));
    // Test with a changestream source and a checkpoint in the middle.
    const newPipeline = [
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        ...pipeline,
    ];
    const test = new TestHelper(
        input,
        newPipeline,
        undefined,  // interval
        "atlas"     // sourcetype
    );

    jsTestLog(`Running with input length ${input.length}, first doc ${input[tojson(0)]}`);

    const insertData = (docs) => {
        let numInserts = 0;
        for (const doc of docs) {
            if (doc.hasOwnProperty(batchBreakerField)) {
                // Wait for all other docs to be processed.
                assert.soon(() => { return test.stats().inputMessageCount == numInserts; },
                            `waiting for inputMessageCount of ${numInserts}`,
                            1000 * 10);
            } else {
                assert.commandWorked(test.inputColl.insertOne(doc));
                numInserts += 1;
            }
        }

        assert.soon(() => { return test.stats().inputMessageCount == numInserts; },
                    `waiting for inputMessageCount of ${numInserts}`,
                    1000 * 10);
    };

    // Start the processor and write part of the input.
    test.startFromLatestCheckpoint();
    const firstInput = input.slice(0, input.length - 1);
    insertData(firstInput);
    // Stop the processor and write the rest of the input.
    test.stop();
    assert.commandWorked(test.inputColl.insertOne(input[input.length - 1]));
    // Start the processor from its last checkpoint.
    test.startFromLatestCheckpoint();
    // Validate the expected results are obtained.
    assert.soon(() => { return test.stats().outputMessageCount == expectedOutput.length; });
    assert(
        resultsEq(test.outputColl.find({}).toArray(), expectedOutput, true /* verbose */, ["_id"]));
    // Wait for expected DLQ results.
    assert.soon(() => { return test.stats().dlqMessageCount == expectedDlq.length; },
                "waiting for expected DLQ stats",
                10 * 1000);
    assert(resultsEq(test.dlqColl.find({}).toArray().map(d => sanitizeDlqDoc(d)),
                     expectedDlq,
                     true /* verbose */,
                     ["_id"]));

    test.stop();
}

// Utilities to interact with the new local disk checkpoint storage.
export class LocalDiskCheckpointUtil {
    constructor(checkpointDir, tenantId, streamProcessorId) {
        this.tenantId = tenantId;
        this.checkpointBaseDir = checkpointDir;
        this._spCheckpointDir = `${checkpointDir}/${tenantId}/${streamProcessorId}`;
        this.processorId = streamProcessorId;
        this.flushedIds = [];
    }

    get streamProcessorCheckpointDir() {
        return this._spCheckpointDir;
    }

    get checkpointIds() {
        if (!fileExists(this._spCheckpointDir)) {
            return [];
        }

        const checkpointIds =
            listFiles(this._spCheckpointDir)
                .filter((e) => e.isDirectory)
                .filter((checkpointDir) => {
                    const manifestFile =
                        listFiles(checkpointDir.name).find((file) => file.baseName === "MANIFEST");
                    return manifestFile !== undefined;
                })
                .map((checkpointDir) => checkpointDir.baseName);
        checkpointIds.sort();
        return checkpointIds;
    }

    getCheckpointIds() {
        return this.checkpointIds;
    }

    get latestCheckpointId() {
        const ids = this.checkpointIds;
        assert.gt(ids.length, 0);
        // checkpointIds returns the IDs in earliest to latest order.
        return ids[ids.length - 1];
    }

    get hasCheckpoint() {
        return this.checkpointIds.length > 0;
    }

    getRestoreDirectory(checkpointId) {
        return `${this._spCheckpointDir}/${checkpointId}`;
    }

    deleteCheckpointDirectory(checkpointId) {
        const dir = this.getRestoreDirectory(checkpointId);
        jsTestLog(`Removing checkpoint directory ${dir}`);
        removeFile(dir);
    }

    clear() {
        if (!fileExists(this._spCheckpointDir)) {
            return;
        }

        listFiles(this._spCheckpointDir).forEach((file) => removeFile(file.name));
    }

    flushAll() {
        for (const id of this.getCheckpointIds()) {
            // only flush checkpoint IDs that we haven't flushed already.
            if (!this.flushedIds.includes(id)) {
                jsTestLog(`Flushing checkpoint ${id}`);
                assert.commandWorked(db.runCommand({
                    streams_sendEvent: '',
                    tenantId: this.tenantId,
                    processorId: this.processorId,
                    checkpointFlushedEvent: {checkpointId: parseInt(id)}
                }));
                this.flushedIds.push(id);
            }
        }
        return this.flushedIds;
    }
}

export function flushUntilStopped(
    name, tenantId, processorId, checkpointBaseDir, alreadyFlushedCheckpointIds = []) {
    let flushedIds = alreadyFlushedCheckpointIds;
    while (true) {
        let result = db.runCommand({streams_listStreamProcessors: '', tenantId: tenantId});
        assert.commandWorked(result);
        let processor = result.streamProcessors.filter(s => s.name == name)[0];
        if (processor == null || processor.length == 0) {
            // The processor has been stopped, return.
            return;
        }

        // Flush any committed checkpoints we haven't flushed already.
        const spCheckpointDir = `${checkpointBaseDir}/${tenantId}/${processorId}`;
        let files = [];
        try {
            files = listFiles(spCheckpointDir);
        } catch (e) {
            let stats = db.runCommand(
                {streams_getStats: '', tenantId: tenantId, name: name, verbose: true});
            jsTestLog(`Hit exception in flushUntilStopped: ${tojson({
                name: name,
                tenantId: tenantId,
                checkpointBaseDir: checkpointBaseDir,
                alreadyFlushedCheckpointIds: alreadyFlushedCheckpointIds,
                listStreamProcessors: result,
                spCheckpointDir: spCheckpointDir,
                stats: stats,
                exception: e
            })}`);
            throw e;
        }
        const checkpointIds =
            listFiles(spCheckpointDir)
                .filter((e) => e.isDirectory)
                .filter((checkpointDir) => {
                    const manifestFile =
                        listFiles(checkpointDir.name).find((file) => file.baseName === "MANIFEST");
                    return manifestFile !== undefined;
                })
                .map((checkpointDir) => checkpointDir.baseName);
        checkpointIds.sort();
        for (const id of checkpointIds) {
            // only flush checkpoint IDs that we haven't flushed already.
            if (!flushedIds.includes(id)) {
                jsTestLog(`Flushing checkpoint ${id}`);
                let result = db.runCommand({
                    streams_sendEvent: '',
                    tenantId: tenantId,
                    processorId: processorId,
                    checkpointFlushedEvent: {checkpointId: parseInt(id)}
                });
                assert(result.ok == 1 || result.code == ErrorCodes.StreamProcessorDoesNotExist);
                flushedIds.push(id);
            }
        }
    }
}

export class TestHelper {
    constructor(
        input,
        middlePipeline,
        interval = 0,
        sourceType = "kafka",
        useNewCheckpointing = true,
        useRestoredExecutionPlan = true,
        writeDir = null,
        restoreDir = null,
        dbForTest = null,
        targetSourceMergeDb = null,
        useTimeField = true,
        sinkType = "atlas",
        changestreamStalenessMonitoring = false,
        oplogSizeMB = undefined,
        kafkaIsTest = true,
    ) {
        assert(useNewCheckpointing);
        this.sourceType = sourceType;
        this.sinkType = sinkType;
        this.input = input;
        // By default use the global db.
        this.targetSourceMergeDb = db;
        if (targetSourceMergeDb != null) {
            this.targetSourceMergeDb = targetSourceMergeDb;
        }
        this.uri = 'mongodb://' + this.targetSourceMergeDb.getMongo().host;
        if (dbForTest != null) {
            this.db = dbForTest;
        } else {
            // Use the global default DB.
            this.db = db;
        }
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = kafkaIsTest;
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

        if (oplogSizeMB) {
            jsTestLog(`Setting oplogSize to MB ${oplogSizeMB}`);
            assert.commandWorked(db.adminCommand({replSetResizeOplog: 1, size: oplogSizeMB}));
            assert.commandWorked(db.adminCommand({setParameter: 1, syncdelay: 5}));
        }
        this.tenantId = TEST_TENANT_ID;
        this.outputColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.outputCollName];
        this.outputColl.drop();
        this.inputColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.inputCollName];
        this.dlqColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.dlqCollName];

        this.spName = uuidStr();
        this.processorId = this.spName;
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

        let featureFlags = {
            useExecutionPlanFromCheckpoint: useRestoredExecutionPlan,
            enableSessionWindow: true
        };
        if (changestreamStalenessMonitoring) {
            featureFlags.changestreamSourceStalenessMonitorPeriod = NumberInt(5);
        }
        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: checkpointOptions,
            featureFlags: featureFlags,
            checkpointOnStart: false
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
            },
            {
                name: '__testSample',
                type: 'sample_solar',
                options: {},
            },
        ];
        this.useTimeField = useTimeField;

        this._buildPipeline(middlePipeline);

        this.sp = new Streams(this.tenantId, this.connectionRegistry, this.db);
        this.checkpointUtil =
            new LocalDiskCheckpointUtil(this.writeDir, this.tenantId, this.processorId);
    }

    _buildPipeline(middlePipeline) {
        // Setup the pipeline.
        this.pipeline = [];
        // First, append either a kafka or changestream source.
        if (this.sourceType === 'kafka') {
            let sourceSpec = {
                connectionName: this.kafkaConnectionName,
                topic: this.kafkaTopic,
                testOnlyPartitionCount: NumberInt(1),
            };
            if (this.useTimeField) {
                sourceSpec.timeField = {$toDate: "$ts"};
            }
            this.pipeline.push({$source: sourceSpec});
        } else if (this.sourceType === 'memory') {
            let sourceSpec = {connectionName: '__testMemory'};
            if (this.useTimeField) {
                sourceSpec.timeField = {$toDate: "$ts"};
            }
            this.pipeline.push({$source: sourceSpec});
        } else if (this.sourceType === 'sample') {
            let sourceSpec = {connectionName: '__testSample'};
            this.pipeline.push({$source: sourceSpec});
        } else {
            let sourceSpec = {
                connectionName: this.dbConnectionName,
                db: this.dbName,
                coll: this.inputCollName,
            };
            if (this.useTimeField) {
                sourceSpec.timeField = {$toDate: "$fullDocument.ts"};
            }
            this.sourceSpec = sourceSpec;
            this.pipeline.push({$source: this.sourceSpec});
        }
        for (let stage of middlePipeline) {
            this.pipeline.push(stage);
        }
        if (this.sinkType == 'memory') {
            this.pipeline.push({$emit: {connectionName: '__testMemory'}});
        } else {
            this.pipeline.push({
                $merge: {
                    into: {
                        connectionName: this.dbConnectionName,
                        db: this.dbName,
                        coll: this.outputCollName
                    },
                }
            });
        }
    }

    // Helper functions.
    run(firstStart = true) {
        this.startFromLatestCheckpoint();
        if (this.sourceType === 'kafka' || this.sourceType === 'memory') {
            // Insert the input.
            assert.commandWorked(this.db.runCommand({
                streams_testOnlyInsert: '',
                tenantId: this.tenantId,
                name: this.spName,
                documents: this.input,
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input));
        }
    }

    startFromLatestCheckpoint(assertWorked = true,
                              checkpointOnStart = false,
                              validateOnly = false,
                              resumeFromCheckpointAfterModify) {
        // Set the restore directory to the latest committed checkpoint on disk.
        let idsOnDisk = this.getCheckpointIds();
        if (idsOnDisk.length > 0) {
            this.startOptions.checkpointOptions.localDisk.restoreDirectory =
                `${this.restoreDir}/${this.tenantId}/${this.processorId}/${idsOnDisk[0]}`;
            this.checkpointUtil.flushedIds = idsOnDisk;
        } else {
            this.startOptions.checkpointOptions.localDisk.restoreDirectory = null;
        }

        this.startOptions.checkpointOnStart = checkpointOnStart;

        this.sp.createStreamProcessor(this.spName, this.pipeline);
        if (validateOnly) {
            this.startOptions.validateOnly = true;
        } else {
            this.startOptions.validateOnly = undefined;
        }
        this.startOptions.resumeFromCheckpointAfterModify = resumeFromCheckpointAfterModify;

        jsTestLog(`Starting ${this.spName} with pipeline ${tojson(this.pipeline)}, options ${
            tojson(this.startOptions)}`);
        return this.sp[this.spName].start(this.startOptions, assertWorked, this.pipelineVersion);
    }

    modifyAndStart({
        newPipeline,
        validateShouldSucceed = true,
        modifyCollectionName = undefined,
        modifiedSourceSpec = undefined,
        resumeFromCheckpointAfterModify = true,
        sourceType = "changestream",
    }) {
        this.sourceType = sourceType;
        if (modifyCollectionName) {
            // If specified, change the output collection name for the $merge in the pipeline.
            this.outputCollName = modifyCollectionName;
            this.outputColl =
                this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.outputCollName];
        }
        this._buildPipeline(newPipeline);
        if (modifiedSourceSpec) {
            // If specified, change the $source in the pipeline.
            this.sourceSpec = modifiedSourceSpec;
            this.pipeline[0] = {$source: modifiedSourceSpec};
            this.inputCollName = this.sourceSpec.coll;
            this.inputColl = this.targetSourceMergeDb.getSiblingDB(this.dbName)[this.inputCollName];
        }

        // Increment the pipeline version.
        if (this.pipelineVersion === undefined) {
            this.pipelineVersion = 1;
        } else {
            this.pipelineVersion += 1;
        }

        const outputCount = this.outputColl.count();

        // Validate the modify operation, like the SPM does.
        const validateResult =
            this.startFromLatestCheckpoint(validateShouldSucceed /* assertWorked */,
                                           false /* checkpointOnStart */,
                                           true /* validateOnly */,
                                           resumeFromCheckpointAfterModify);
        if (!validateShouldSucceed) {
            return validateResult;
        }
        // Stream processor should not exist after validateOnly operation.
        assert.eq([], this.list());
        // Stream processor should not have written anything after validateOnly operation.
        assert.eq(this.outputColl.count(), outputCount);

        // Start the modified stream processor.
        this.startFromLatestCheckpoint(true /* validateShouldSucceed */,
                                       false /* checkpointOnStart */,
                                       false /* validateOnly */,
                                       resumeFromCheckpointAfterModify);
        assert.eq(1, this.list().length);
        return validateResult;
    }

    stop(assertWorked = true, checkpointOnStop = true) {
        // This will flush all the checkpoints on disk that have not already
        // been flushed.
        // skip checkpointing logic for the sample sourceTypes
        this.sp[this.spName].stop(
            assertWorked,
            this.checkpointUtil.flushedIds,
            checkpointOnStop && this.sourceType != "sample" /* checkpointOnStop */);
        // Update the flushedIds so we don't flush them again on a subsequent start/stop.
        this.checkpointUtil.flushedIds = this.checkpointUtil.checkpointIds;
    }

    stats() {
        return this.sp[this.spName].stats();
    }

    list() {
        return this.sp.listStreamProcessors().streamProcessors.filter((sp) =>
                                                                          sp.name == this.spName);
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
                        listFiles(checkpointDir.name).find(file => file.name.endsWith("MANIFEST"));
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
export class CheckPointTestHelper extends TestHelper {
    constructor(inputDocs,
                pipeline,
                interval,
                sourceType = "kafka",
                useNewCheckpointing = false,
                writeDir = null,
                restoreDir = null) {
        super(inputDocs,
              pipeline,
              interval,
              sourceType,
              useNewCheckpointing,
              true,
              writeDir,
              restoreDir);
    }
    runWithInputSlice(endRange, firstStart = true) {
        this.sp.createStreamProcessor(this.spName, this.pipeline);
        this.sp[this.spName].start(this.startOptions);
        if (this.sourceType === 'kafka') {
            // Insert the input.
            assert.commandWorked(this.db.runCommand({
                streams_testOnlyInsert: '',
                tenantId: this.tenantId,
                name: this.spName,
                documents: this.input.slice(0, endRange),
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input[0..endRange]));
        }

        assert.soon(() => this.getCheckpointIds().length > 0,
                    `SP failed to checkpoint at least once`);
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
    var test = new TestHelper(inputDocs, middlePipeline, 10000000, "kafka", true, true);
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
