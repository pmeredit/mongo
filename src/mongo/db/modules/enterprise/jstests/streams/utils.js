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

// Returns a cloned object with the metadata fields removed (e.g. `_ts` and
// `_stream_meta`) for easier comparison checks.
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

export function startTest({level}) {
    jsTestLog(`Starting ${getCallerName(level)}() test`);
}

export function testDone({level}) {
    jsTestLog(`${getCallerName(level)}() test done`);
}
