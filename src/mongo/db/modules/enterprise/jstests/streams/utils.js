import {
    documentEq,
} from "jstests/aggregation/extras/utils.js";
import {
    getCallerName,
} from 'jstests/core/timeseries/libs/timeseries_writes_util.js';

export const TEST_TENANT_ID = 'testTenant';
export const TEST_PROJECT_ID = 'testProject';
// TODO: Rename following variables to use UPPER_SNAKE_CASE naming pattern.
export const connectionName = 'db1';
export const dbName = jsTestName();
export const dlqCollName = 'dlq';
export const outCollName = "out";

export const sink = Object.freeze({
    memory: {$emit: {connectionName: '__testMemory'}},
});

export function verifyDocsEqual(
    inputDoc, outputDoc, ignoreFields = ["_id", "_ts", "_stream_meta"]) {
    // TODO(SERVER-84656): This fails because the streams pipeline seems to change the field order.
    // Look into whether this is expected or not. assert.eq(left, removeProjections(right))

    if (!documentEq(inputDoc, outputDoc, false, null, ignoreFields)) {
        assert(false, `${tojson(inputDoc)} does not equal ${tojson(outputDoc)}`);
    }
}

/**
 * Verifies the input array equals the output array, ignoring _id, _ts, and _stream_meta
 * projections.
 * @param {*} input is an array of documents
 * @param {*} output is an array of documents
 */
export function verifyInputEqualsOutput(
    input, output, ignoreFields = ["_id", "_ts", "_stream_meta"]) {
    assert.eq(input.length, output.length);
    for (let i = 0; i < input.length; i += 1) {
        const inputDoc = input[i];
        const outputDoc = output[i];
        if (!documentEq(inputDoc, outputDoc, true, null, ignoreFields)) {
            assert(false, `${tojson(inputDoc)} does not equal ${tojson(outputDoc)}`);
        }
    }
}

/**
 * Start a sample on a streamProcessor.
 */
export function startSample(name, limit) {
    let startSampleCmd =
        {streams_startStreamSample: '', tenantId: TEST_TENANT_ID, name: name, limit: limit};
    let result = db.runCommand(startSampleCmd);
    assert.commandWorked(result);
    return result;
}

/**
 * Sample until count results are retrieved.
 */
export function sampleUntil(cursorId, count, name, maxIterations = 100, sleepInterval = 50) {
    let getMoreCmd = {streams_getMoreStreamSample: cursorId, tenantId: TEST_TENANT_ID, name: name};
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
        assert.eq(currentCount, count, "maximum time elapsed");
    }
}

export function waitWhenThereIsMoreData(coll, maxWaitSeconds = 10) {
    const sleepInterval = 100;
    const maxTime = Date.now() + 1000 * maxWaitSeconds;
    let currentCount = coll.find({}).count();
    var prevCount;
    do {
        prevCount = currentCount;
        sleep(sleepInterval);
        currentCount = coll.find({}).count();
    } while (currentCount > prevCount && Date.now < maxTime);
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
export function startStreamProcessor(spName, pipeline, assertWorked = true) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
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
        options: {
            dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName},
            featureFlags: {},
        }
    };

    jsTestLog(`Starting ${spName} - \n${tojson(startCmd)}`);
    const result = db.runCommand(startCmd);
    if (assertWorked) {
        assert.commandWorked(result);
    }
    return result;
}

/**
 * Stops the stream processor named 'spName'. Returns the stop command result.
 */
export function stopStreamProcessor(spName) {
    let stopCmd = {
        streams_stopStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
    };
    jsTestLog(`Stopping ${spName} - \n${tojson(stopCmd)}`);
    return assert.commandWorked(db.runCommand(stopCmd));
}

/**
 * Retrieves stats for the given stream processor.
 */
export function getStats(spName) {
    let statsCmd = {streams_getStats: '', tenantId: TEST_TENANT_ID, name: spName, verbose: true};
    return assert.commandWorked(db.runCommand(statsCmd));
}

/**
 * Inserts 'docs' into the test-only in-memory source for the stream processor named 'spName'.
 * Returns the insert command result.
 */
export function insertDocs(spName, docs) {
    let insertCmd = {
        streams_testOnlyInsert: '',
        tenantId: TEST_TENANT_ID,
        name: spName,
        documents: docs,
    };

    return assert.commandWorked(db.runCommand(insertCmd));
}

/**
 * Lists all stream processors. Returns the listStreamProcessors command result.
 */
export function listStreamProcessors() {
    return assert.commandWorked(
        db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID}));
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
export function runStreamProcessorOperatorTest(
    {pipeline, verifyAction, spName, whenMatchedOption}) {
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
                whenMatched: whenMatchedOption ? whenMatchedOption : "merge"
            }
        }
    ]);

    verifyAction();
    // Stops the streamProcessor.
    stopStreamProcessor(spName);

    testDone({level: 3});
}

/**
 * This function takes a pipeline adds a source/sink and runs the pipeline
 * @param {*} param0 is an object containing pipeline, verification action function and a stream
 *     processor name
 */
export function runStreamProcessorWindowTest({pipeline, verifyAction, spName}) {
    const uri = 'mongodb://' + db.getMongo().host;
    startTest({level: 3});

    db.getSiblingDB(dbName).outColl.drop();
    db.getSiblingDB(dbName)[dlqCollName].drop();

    // Starts a stream processor 'spName'.
    startStreamProcessor(spName, pipeline);
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
    const outCollState = `out -\n${tojson(db.getSiblingDB(dbName).outColl.find().toArray())}`;
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

/**
 * makes random string of specified length
 * @param {*} length
 * @returns
 */
export function makeRandomString(length) {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    let counter = 0;
    while (counter < length) {
        result += characters.charAt(Random.randInt(characters.length));
        counter += 1;
    }
    return result;
}

/**
 * creates a dataset with some fields for random values to be used in window pipelines
 * the dataset will add a timestamp that falls in one second window
 * the dataset will also add a record so that we close the window
 * @param {*} docSize
 * @returns
 */
export function generateDocs(docSize) {
    var startingDate = new Date('2023-03-03T20:42:30.000Z').getTime();
    const docs = [];
    for (let i = 0; i < docSize; i++) {
        var curDate = new Date(startingDate + i / docSize * 1000);
        docs.push({
            _id: i,
            a: Random.randInt(Math.max(docSize / 10, 10)),
            b: Random.randInt(docSize),
            c: Random.randInt(docSize),
            d: makeRandomString(10),
            ts: curDate.toISOString()
        });
    }
    // add one document which is 4 seconds from the starttime to account for allowed lateness of 3
    // seconds
    docs.push({
        id: docSize,
        ts: new Date(startingDate + 4001).toISOString()
    });  // to be able close the window.
    return docs;
}

/**
 * for hopping window, we needed to generate a little bit more sofisticated data set, so that we
 * would have some overlap and also multiple windows
 * @param {*} docSizePerWindow number of documents for window
 * @param {*} windowSize window size in ms
 * @param {*} hopSize the hopsize in ms
 * @param {*} nHops how many hops we need
 * @returns a set of documents that we can use for window tests
 */
/*
** for hopping window, we needed to generate a little bit more sofisticated data set, so that we
*would
** have some overlap and also multiple windows
*/
export function generateDocsForHoppingWindow(
    docSizePerWindow, windowSize = 5000, hopSize = 1000, nHops = 5) {
    var startingDate = new Date('2023-03-03T20:42:30.000Z').getTime();
    const docs = [];
    // 1 full window
    for (let i = 0; i < docSizePerWindow; i++) {
        var curDate = new Date(startingDate + i / docSizePerWindow * 1000);
        docs.push({
            _id: i,
            a: Random.randInt(Math.max(docSizePerWindow / 10, 10)),
            b: Random.randInt(docSizePerWindow),
            c: Random.randInt(docSizePerWindow),
            d: makeRandomString(10),
            ts: curDate.toISOString()
        });
    }
    const hopFactor = windowSize / hopSize;
    // For each hop generate some docs
    for (let i = 0; i < nHops; i++) {
        // Assumption: docSizePerWindow is a multiple of hopFactor
        // to get an even number of docs for hop
        // like default values
        for (let j = 0; j < docSizePerWindow / hopFactor; j++) {
            var curDate =
                new Date(startingDate + windowSize + i * hopSize + j / hopSize * 1000 * hopFactor);
            docs.push({
                _id: docSizePerWindow + i * docSizePerWindow / hopFactor + j,
                a: Random.randInt(Math.max(docSizePerWindow / 10, 10)),
                b: Random.randInt(docSizePerWindow),
                c: Random.randInt(docSizePerWindow),
                d: makeRandomString(10),
                ts: curDate.toISOString()
            });
        }
    }
    // Add one document which is 3 seconds + 1ms away from the endtime of last hop.
    // to allow for allowed lateness.
    docs.push({
        id: docSizePerWindow + nHops * docSizePerWindow / hopFactor,
        ts: new Date(startingDate + 1 + windowSize + hopSize * nHops + 3000).toISOString()
    });  // to be able close the window.
    return docs;
}

// floating calculations like stdDevPop, stdDevSamp can vary because of floating point precision
function approximateDataEquality(expected, resultDoc) {
    for (const [key, expectedValue] of Object.entries(expected)) {
        assert(resultDoc.hasOwnProperty(key));
        const resultValue = resultDoc[key];

        let matches = false;
        if (((typeof expectedValue) == "object") && ((typeof resultValue) == "object")) {
            // NumberDecimal case.
            matches = (bsonWoCompare({value: expectedValue}, {value: resultValue}) === 0);
        } else if (isFinite(expectedValue) && isFinite(resultValue)) {
            // Regular numbers; do an approximate comparison, expecting 48 bits of precision.
            const epsilon = Math.pow(2, -48);
            const delta = Math.abs(expectedValue - resultValue);
            matches = (delta === 0) ||
                ((delta /
                  Math.min(Math.abs(expectedValue) + Math.abs(resultValue), Number.MAX_VALUE)) <
                 epsilon);
        } else {
            matches = (expectedValue === resultValue);
        }
        assert(matches,
               `Mismatched ${key} field in document with _id ${resultDoc._id} -- Expected: ${
                   expectedValue}, Actual: ${resultValue}`);
    }
}

// addToSet returns documents in different order when compared to running on the collection
// this compare ignores the order for that.
function ignoreOrderInSetCompare(expected, resultDoc) {
    for (const [key, expectedValue] of Object.entries(expected)) {
        assert(resultDoc.hasOwnProperty(key));
        const resultValue = resultDoc[key];
        if (Array.isArray(expectedValue)) {
            assert.eq(Array.isArray(expectedValue), Array.isArray(resultValue));
            assert.eq(expectedValue.sort(), resultValue.sort());
        } else {
            assert.eq(expectedValue, resultValue);
        }
    }
}

export const group_sort_pipeline = {
    pipeline: [
        {
            $group: {
                _id: "$a",
                avg_a: {$avg: "$b"},
                bottom_a: {$bottom: {output: ["$b", "$c"], sortBy: {"c": -1, "b": -1}}},
                bottomn_a: {$bottomN: {output: ["$b", "$c"], sortBy: {"c": -1, "b": 1}, n: 3}},
                count: {$count: {}},
                // First/Last depend on the order, the testing method does not
                // produce the same results
                //           first_b: {$first: "$b"},
                //            firstn_b: {$firstN: {input: "$b", n: 5}},
                //           last_b: {$first: "$b"},
                //            lastn_b: {$firstN: {input: "$b", n: 5}},
                max_c: {$max: "$c"},
                maxn_c: {$maxN: {input: "$c", n: 4}},
                median_b: {$median: {input: "$b", method: 'approximate'}},
                min_b: {$min: "$b"},
                percentile_c: {$percentile: {input: "$c", p: [0.95], method: 'approximate'}},
                sum_a: {$sum: "$a"},
                sum_b: {$sum: "$b"},
                sum_c: {$sum: "$c"},
                top_b: {$top: {output: ["$b", "$c"], sortBy: {"c": 1, "b": 1}}},
                topn_b: {
                    $topN: {
                        output: ["$b", "$c"],
                        sortBy: {"c": 1, "b": 1},
                        n: 4,
                    }
                }
            }
        },
        {$sort: {_id: 1}}
    ]
};

export const sort_limit_pipeline = {
    pipeline: [{$sort: {b: 1, d: 1}}, {$limit: 50}]
};

const _windowPipelines = [
    {
        pipeline: [
            {
                $group: {
                    _id: "$a",
                    stddev_b: {$stdDevSamp: "$c"},
                    stddevpop_b: {$stdDevPop: "$c"},
                }
            },
            {$sort: {_id: 1}}
        ],
        compareFunction: approximateDataEquality
    },
    {
        pipeline: [{$group: {_id: "$a", result: {$addToSet: "$b"}}}, {$sort: {_id: 1}}],
        compareFunction: ignoreOrderInSetCompare
    },
    // _id is included in the sort keys because none of a, b, c, or d is guaranteed to be unique,
    // and we need a unique field to break ties so that we can match against an expected ordering.
    {pipeline: [{$sort: {a: 1, b: 1, _id: 1}}]},
    {pipeline: [{$sort: {a: 1, c: 1, _id: 1}}]},
    {pipeline: [{$sort: {b: 1, c: 1, _id: 1}}]},
    {pipeline: [{$sort: {d: 1, _id: 1}}]},
    {pipeline: [{$sort: {b: 1, d: 1, _id: 1}}]},
    {pipeline: [{$sort: {d: 1, _id: 1}}, {$limit: 100}]}
];

export const windowPipelines =
    [].concat([group_sort_pipeline], _windowPipelines, [sort_limit_pipeline]);

export function getOperatorStats(spName, operatorType) {
    let getStatsCmd = {streams_getStats: '', tenantId: TEST_TENANT_ID, name: spName, verbose: true};
    let result = db.runCommand(getStatsCmd);
    if (result["ok"] != 1) {
        return {};
    }

    let opStats = result["operatorStats"];
    jsTestLog(opStats);
    for (let i = 0; i < opStats.length; i++) {
        let op = opStats[i];
        jsTestLog(op);
        if (op["name"] == operatorType) {
            return op;
        }
    }
    return {};
}
