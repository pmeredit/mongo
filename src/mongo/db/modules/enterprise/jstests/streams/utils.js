import {
    getCallerName,
} from 'jstests/core/timeseries/libs/timeseries_writes_util.js';

export const sink = Object.freeze({
    memory: {$emit: {connectionName: '__testMemory'}},
});

/**
 * Start a sample on a streamProcessor.
 */
export function startSample(name) {
    let startSampleCmd = {streams_startStreamSample: '', name: name};
    let result = db.runCommand(startSampleCmd);
    assert.commandWorked(result);
    return result["id"];
}

/**
 * Sample until count results are retrieved.
 */
export function sampleUntil(cursorId, count, name, maxIterations = 100, sleepInterval = 50) {
    let getMoreCmd = {streams_getMoreStreamSample: cursorId, name: name};
    let sampledDocs = [];
    let i = 0;
    while (i < maxIterations && sampledDocs.length < count) {
        let result = db.runCommand(getMoreCmd);
        assert.commandWorked(result);
        assert.eq(result["cursor"]["id"], cursorId);
        sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
        sleep(sleepInterval);
        i += 1;
    }
    assert.gte(sampledDocs.length, count, "Failed to retrieve expected number of docs");
    return sampledDocs;
}

/**
 * Wait until at least the specified number of documents exists in coll.
 */
export function waitForCount(coll, count, maxWaitSeconds = 5) {
    const sleepInterval = 50;
    const maxTime = Date.now() + 1000 * maxWaitSeconds;
    let currentCount = coll.find({}).count();
    while (currentCount < count && Date.now() < maxTime) {
        currentCount = coll.find({}).count();
        sleep(sleepInterval);
    }
    if (currentCount < count) {
        assert(false, 'maximum time elapsed');
    }
}

export function waitForDoc(coll, predicate, maxWaitSeconds = 5) {
    const sleepInterval = 50;
    const maxTime = Date.now() + 1000 * maxWaitSeconds;
    while (Date.now() < maxTime) {
        let docs = coll.find({}).toArray();
        if (docs.some(predicate)) {
            return;
        }
        sleep(sleepInterval);
    }
    throw 'maximum time elapsed';
}

export class CheckpointUtils {
    constructor(checkpointColl) {
        this.checkpointColl = checkpointColl;
    }

    getCheckpointIds(tenantId, processorId) {
        return this.checkpointColl.find({_id: {$regex: `^checkpoint/${tenantId}/${processorId}/`}})
            .sort({_id: -1})
            .toArray();
    }
}

/**
 * Returns a cloned object with the metadata fields removed (e.g. `_ts` and `_stream_meta`) for
 * easier comparison checks.
 */
export function sanitizeDoc(doc, fieldNames = ['_ts', '_stream_meta']) {
    let clone = Object.assign({}, doc);
    for (let fieldName of fieldNames) {
        delete clone[fieldName];
    }
    return clone;
}

export const connectionName = 'db1';
export const dbName = jsTestName();
export const dlqCollName = 'dlq';
export const outCollName = "out";

/**
 * Starts a stream processor named 'spName' with 'pipeline' specification. Returns the start command
 * result.
 *
 * The connections are pre-configured with
 * connections: [
 *     {name: "db1", type: 'atlas', options: {uri: "mongodb://127.0.0.1:_port_"}},
 *     {name: '__testMemory', type: 'in_memory', options: {}},
 * ]
 *
 * The DLQ is also pre-configured with
 * {dlq: {connectionName: "db1", db: "test", coll: "dlq"}}
 */
export function startStreamProcessor(spName, pipeline) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: 'tenant1',
        name: spName,
        processorId: spName,
        pipeline: pipeline,
        connections: [
            {name: connectionName, type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
            {
                name: "kafka1",
                type: 'kafka',
                options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
            },
        ],
        options: {dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName}}
    };

    jsTestLog(`Starting ${spName} - \n${tojson(startCmd)}`);
    return assert.commandWorked(db.runCommand(startCmd));
}

/**
 * Stops the stream processor named 'spName'. Returns the stop command result.
 */
export function stopStreamProcessor(spName) {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        name: spName,
    };
    jsTestLog(`Stopping ${spName} - \n${tojson(stopCmd)}`);
    return assert.commandWorked(db.runCommand(stopCmd));
}

/**
 * Inserts 'docs' into the test-only in-memory source for the stream processor named 'spName'.
 * Returns the insert command result.
 */
export function insertDocs(spName, docs) {
    let insertCmd = {
        streams_testOnlyInsert: '',
        name: spName,
        documents: docs,
    };

    return assert.commandWorked(db.runCommand(insertCmd));
}

/**
 * Lists all stream processors. Returns the listStreamProcessors command result.
 */
export function listStreamProcessors() {
    return assert.commandWorked(db.runCommand({streams_listStreamProcessors: ''}));
}

/**
 * Logs start of the test function at the upper level specified by 'level'.
 */
export function startTest({level}) {
    jsTestLog(`Starting ${getCallerName(level)}() test`);
}

/**
 * Logs stop of the test function at the upper level specified by 'level'.
 */
export function testDone({level}) {
    jsTestLog(`${getCallerName(level)}() test done`);
}

/**
 * Cleans up database tables, starts stream processor, runs verify action specified by the called
 * and stops stream processor
 */
export function runStreamProcessorOperatorTest({pipeline, verifyAction, spName}) {
    startTest({level: 3});

    db.getSiblingDB(dbName).outColl.drop();
    db.getSiblingDB(dbName)[dlqCollName].drop();
    var source = {$source: {connectionName: "__testMemory"}};

    // Starts a stream processor 'spName'.
    startStreamProcessor(spName, [
        source,
        ...pipeline,
        {
            $merge: {
                into: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: db.getSiblingDB(dbName).outColl.getName()
                },
                whenNotMatched: 'insert'
            }
        }
    ]);

    verifyAction();
    // Stops the streamProcessor.
    stopStreamProcessor(spName);

    testDone({level: 3});
}

export function runStreamProcessorWindowTest({pipeline, verifyAction, spName, dateField}) {
    const uri = 'mongodb://' + db.getMongo().host;
    startTest({level: 3});

    db.getSiblingDB(dbName).outColl.drop();
    db.getSiblingDB(dbName)[dlqCollName].drop();
    const source = {
        $source: {
            connectionName: "kafka1",
            topic: "test1",
            timeField: {$dateFromString: {"dateString": "$date"}},
            testOnlyPartitionCount: NumberInt(1)
        }
    };

    // Starts a stream processor 'spName'.
    startStreamProcessor(spName, [
        source,
        ...pipeline,
        {
            $merge: {
                into: {
                    connectionName: connectionName,
                    db: dbName,
                    coll: db.getSiblingDB(dbName).outColl.getName()
                },
                whenNotMatched: 'insert'
            }
        }
    ]);

    verifyAction();
    // Stops the streamProcessor.
    stopStreamProcessor(spName);

    testDone({level: 3});
}

/**
 * Runs a test with the given 'spName', 'pipeline', 'setupAction', 'verifyActions', and
 * 'tearDownAction'. The test sequence is as follows:
 *
 * 1. Runs 'setupAction'.
 * 2. Starts a stream processor 'spName' with 'pipeline'.
 * 3. Runs 'verifyActions'. Runs one by one in the order of array if it's an array of actions.
 * 4. Stops the stream processor 'spName'.
 * 5. Runs 'tearDownAction'.
 *
 * The default 'setupAction' is to drop the 'outCollName' and 'dlqCollName' collections and the
 * default 'tearDownAction' is to do nothing.
 */
export function runTest({
    spName,
    pipeline,
    setupAction =
        () => {
            db.getSiblingDB(dbName)[outCollName].drop();
            db.getSiblingDB(dbName)[dlqCollName].drop();
        },
    verifyActions,
    iteration = 1,
    tearDownAction = () => {},
}) {
    startTest({level: 3});

    setupAction();

    // Starts a stream processor 'spName'.
    startStreamProcessor(spName, pipeline);

    if (iteration === 1 && !Array.isArray(verifyActions)) {
        verifyActions = [verifyActions];
    }

    for (let i = 0; i < iteration; i++) {
        verifyActions[i]();
    }

    // Stops the streamProcessor.
    stopStreamProcessor(spName);

    tearDownAction();

    testDone({level: 3});
}

/**
 * Logs state of stream processor and target database table, dlq data.
 */
export function logState(spName) {
    const spState = `${spName} -\n${tojson(listStreamProcessors())}}`;
    jsTestLog(spState);
    const outCollState = `out -\n${tojson(db.getSiblingDB(dbName)[outCollName].find().toArray())}`;
    jsTestLog(outCollState);
    const dlqCollState = `dlq -\n${tojson(db.getSiblingDB(dbName)[dlqCollName].find().toArray())}`;
    jsTestLog(dlqCollState);
    return spState + "\n" + outCollState + "\n" + dlqCollState;
}

/**
 * Generates a document that has size ~16MB that we can stream to test largeDocument cases.
 */
export function generate16MBDoc() {
    const maxFields = 10;
    var doc = {};
    const maxStringLen = 1600000;
    const largeString = new Array(maxStringLen + 1).join('a');
    for (let i = 0; i < maxFields; i++) {
        doc["a" + i] = largeString;
    }
    doc["b"] = 0;
    return doc;
}

/**
 * Generates a pipeline that has 'args' middle stages between the in-memory $source and the $merge
 * into a collection named 'dbName.outCollName' on the remote DB server 'connectionName'.
 */
export function makeLookupPipeline(...args) {
    let pipeline = [{$source: {connectionName: "__testMemory"}}];
    pipeline = pipeline.concat(Array.from(args));
    pipeline.push({
        $merge: {
            into: {connectionName: connectionName, db: dbName, coll: outCollName},
            whenMatched: 'replace',
            whenNotMatched: 'insert'
        }
    });

    return pipeline;
}
