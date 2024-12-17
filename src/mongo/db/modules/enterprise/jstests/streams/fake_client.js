/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Thread} from "jstests/libs/parallelTester.js";
import {
    flushUntilStopped
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {TEST_TENANT_ID} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

// StreamProcessor and Streams classes are used to make the
// client javascript look like the syntax spec.
// See kafkaExample below for usage instructions.
export class StreamProcessor {
    constructor(tenantId,
                name,
                pipeline,
                connectionRegistry,
                dbForTest,
                isRunningOnAtlasStreamProcessor = false) {
        this._tenantId = tenantId;
        this._name = name;
        this._pipeline = pipeline;
        this._connectionRegistry = connectionRegistry;
        this._processorId = this._name;
        this._db = db;
        this._pipelineVersion = undefined;
        if (dbForTest != null) {
            this._db = dbForTest;
        }
        this._isUsingAtlas = isRunningOnAtlasStreamProcessor;
    }

    // Utilities to make test streams comamnds.
    // Testing both scenarios for correlationId (null vs not-null) as this
    // is an optional param.
    makeStartCmd(options = {featureFlags: {}}) {
        return {
            streams_startStreamProcessor: '',
            tenantId: this._tenantId,
            name: this._processorId,
            processorId: this._name,
            pipeline: this._pipeline,
            connections: this._connectionRegistry,
            pipelineVersion: NumberInt(this._pipelineVersion),
            options: options,
            correlationId: Math.random() < 0.2 ? null : 'userRequest1'
        };
    }

    // Start the streamProcessor.
    start(options, assertWorked = true, pipelineVersion = undefined) {
        this._startOptions = options;
        this._pipelineVersion = pipelineVersion;
        const cmd = this.makeStartCmd(options);
        jsTestLog(`Starting processor ${tojson(cmd)}`);
        const result = this._db.runCommand(cmd);
        if (assertWorked) {
            assert.commandWorked(result);
        }
        return result;
    }

    makeStopCmd() {
        if (this._isUsingAtlas) {
            return {stopStreamProcessor: this._name};
        }

        return {streams_stopStreamProcessor: '', tenantId: this._tenantId, name: this._name};
    }

    // Stop the streamProcessor.
    stop(assertWorked = true, alreadyFlushedCheckpointIds = []) {
        let flushThread = null;
        if (this._startOptions != null && this._startOptions.checkpointOptions != null) {
            const checkpointOptions = this._startOptions.checkpointOptions;
            const checkpointBaseDir = checkpointOptions.localDisk.writeDirectory;
            flushThread = new Thread(flushUntilStopped,
                                     this._name,
                                     this._tenantId,
                                     this._processorId,
                                     checkpointBaseDir,
                                     alreadyFlushedCheckpointIds);
            flushThread.start();
        }

        const result = this._db.runCommand(this.makeStopCmd());
        if (flushThread != null) {
            flushThread.join();
        }
        if (assertWorked) {
            assert.commandWorked(result);
        }

        return result;
    }

    // Drop the streamProcessor.
    drop(assertWorked = true) {
        if (this._isUsingAtlas) {
            const result = this._db.runCommand({"dropStreamProcessor": this._name});
            if (assertWorked) {
                assert.commandWorked(result);
            }
        } else {
            this.stop(assertWorked);
        }
    }

    // Gets more sample results with the supplied cursor ID.
    getMoreSample(dbConn, cursorId, maxLoops = 10) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        for (let loop = 0; loop < maxLoops; loop++) {
            let cmd = {
                streams_getMoreStreamSample: cursorId,
                tenantId: this._tenantId,
                name: this._name
            };
            let result = dbConn.runCommand(cmd);
            assert.commandWorked(result);

            if (result["cursor"]["id"] == 0) {
                print("Sample stopping: returned cursor ID 0 from server");
                break;
            }

            let batch = result["cursor"]["nextBatch"];
            if (batch.length == 0) {
                sleep(1000);
            } else {
                for (const doc of batch) {
                    print(tojson(doc));
                }
                return batch;
            }
        }

        return [];
    }

    // Utility to sample a stream processor with a custom connection.
    runGetMoreSample(dbConn, maxLoops = 10) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        let cmd = {
            streams_startStreamSample: '',
            tenantId: this._tenantId,
            name: this._name,
        };
        let result = dbConn.runCommand(cmd);
        assert.commandWorked(result);
        let cursorId = result["id"];
        return this.getMoreSample(dbConn, cursorId, maxLoops);
    }

    // Sample the streamProcessor.
    sample(maxLoops = 10) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        return this.runGetMoreSample(db, maxLoops);
    }

    startSample() {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        let cmd = {
            streams_startStreamSample: '',
            tenantId: this._tenantId,
            name: this._name,
        };
        let result = this._db.runCommand(cmd);
        assert.commandWorked(result);
        return result;
    }

    getNextSample(cursorId) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        let cmd = {
            streams_getMoreStreamSample: cursorId,
            tenantId: this._tenantId,
            name: this._name
        };
        let result = this._db.runCommand(cmd);
        assert.commandWorked(result);
        return result["cursor"]["nextBatch"];
    }

    // `stats` returns the stats corresponding to this stream processor.
    stats(verbose = true, scale = 1) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        const res = this._db.runCommand({
            streams_getStats: '',
            tenantId: this._tenantId,
            name: this._name,
            scale: scale,
            verbose
        });
        assert.commandWorked(res);
        assert.eq(res["ok"], 1);
        return res;
    }

    checkpoint(force) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        const res = this._db.runCommand({
            streams_writeCheckpoint: '',
            tenantId: this._tenantId,
            name: this._name,
            force: force
        });
        assert.commandWorked(res);
        assert.eq(res["ok"], 1);
        return res;
    }

    testInsert(...documents) {
        assert(!this._isUsingAtlas, "Not supported against real Atlas");

        const res = this._db.runCommand({
            streams_testOnlyInsert: '',
            tenantId: this._tenantId,
            name: this._name,
            documents,
        });
        assert.commandWorked(res);
        return res;
    }
}

export class Streams {
    constructor(tenantId, connectionRegistry, dbForTest = null, isUsingAtlas = false) {
        this._tenantId = tenantId;
        this._connectionRegistry = connectionRegistry;
        this._db = db;
        this._isUsingAtlas = isUsingAtlas;
        if (dbForTest != null) {
            this._db = dbForTest;
        }
    }

    createStreamProcessor(name, pipeline) {
        if (this._isUsingAtlas) {
            let cmd = {"createStreamProcessor": name, "pipeline": pipeline};
            let result = this._db.runCommand(cmd);
            assert.commandWorked(result);
        }

        const sp = new StreamProcessor(
            this._tenantId, name, pipeline, this._connectionRegistry, this._db, this._isUsingAtlas);
        this[name] = sp;
        return sp;
    }

    listStreamProcessors(verbose = true) {
        const res = this._db.runCommand(
            {streams_listStreamProcessors: '', tenantId: this._tenantId, verbose: verbose});
        assert.commandWorked(res);
        return res;
    }

    process(pipeline, maxLoops = 3) {
        let name = UUID().toString();
        this[name] =
            new StreamProcessor(this._tenantId, name, pipeline, this._connectionRegistry, this._db);
        let startResult = this[name].start(
            {ephemeral: true, shouldStartSample: true, featureFlags: {enableSessionWindow: true}});
        assert.commandWorked(startResult);
        let cursorId = startResult.sampleCursorId;
        let sampleResults = this[name].getMoreSample(db, cursorId, maxLoops);
        assert.commandWorked(this[name].stop());
        return sampleResults;
    }

    metrics() {
        const res = this._db.runCommand({streams_getMetrics: ''});
        assert.commandWorked(res);
        assert.eq(res["ok"], 1);
        return res;
    }
}

export let sp = new Streams(TEST_TENANT_ID, []);
export const test = {
    atlasConnection: "StreamsAtlasConnection",
    dbName: "test",
    inputCollName: "testin",
    outputCollName: "testout",
    dlqCollName: "testdlq"
};

export function getDefaultSp() {
    const uri = 'mongodb://' + db.getMongo().host;
    return new Streams(TEST_TENANT_ID, [
        {
            name: test.atlasConnection,
            type: 'atlas',
            options: {uri: uri},
        },
    ]);
}

export function commonTestSetup() {
    const sp = getDefaultSp();
    const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];
    const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
    inputColl.drop();
    outputColl.drop();
    return [sp, inputColl, outputColl];
}

export function getAtlasStreamProcessorHandle(uri) {
    const m = new Mongo(uri);
    const dbForTest = m.getDB("admin");
    return new Streams(
        [] /*connectionRegistry*/, dbForTest, /*isRunningOnAtlasStreamProcessor*/ true);
}

export function kafkaExample(
    connectionName = "kafka1", inputTopic = "inputTopic", isTestKafka = false) {
    // First, we create some javascript objects that fake
    // some parts of the client shell and connection registry.
    let connectionRegistry = [{
        name: connectionName,
        type: 'kafka',
        options: {bootstrapServers: 'localhost:9092', isTestKafka: isTestKafka},
    }];
    sp = new Streams(TEST_TENANT_ID, connectionRegistry);

    // Below here, things should look mostly like the syntax spec.

    // Use the streams object to create a streamProcessor named myProcessor.
    sp.createStreamProcessor("myProcessor", [
        {
            $source: {
                connectionName: connectionName,
                topic: inputTopic,
                testOnlyPartitionCount: NumberInt(1),
                // Examples of optional fields below:
                // timeField: { $dateFromString: { dateString: '$myTsStr' }},
                // tsFieldName: '_myts',
            }
        },
        {$project: {id: 1, value: 1}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                pipeline: [
                    {$sort: {value: 1}},
                    {
                        $group: {
                            _id: "$id",
                            sum: {$sum: "$value"},
                            max: {$max: "$value"},
                            min: {$min: "$value"},
                            count: {$count: {}},
                            first: {$first: "$value"}
                        }
                    },
                    {$sort: {sum: 1}},
                    {$limit: 1}
                ]
            }
        },
        {$match: {_id: NumberInt(0)}},
        {$emit: {connectionName: "__testLog"}}
    ]);

    // Start the streamProcessor.
    let result = sp.myProcessor.start();
    assert.commandWorked(result);

    // Sample the streamProcessor.
    sp.myProcessor.sample();

    // Stop the streamProcessor.
    result = sp.myProcessor.stop();
    assert.commandWorked(result);
}
