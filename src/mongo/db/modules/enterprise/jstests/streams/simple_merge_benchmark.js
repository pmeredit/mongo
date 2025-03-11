/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    getAtlasStreamProcessingInstance,
    getDefaultSp,
    test
} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {makeRandomString} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

// Setup the target cluster used for $source and $merge stage.
// by default use the local mongod as the source/merge target
let targetConn = new Mongo(db.getMongo().host);
let targetClusterURI = _getEnv("ATLAS_CLUSTER_URI");
if (targetClusterURI && targetClusterURI !== null && targetClusterURI !== "") {
    // Use a remote Atlas cluster as the source/merge target.
    targetConn = new Mongo(targetClusterURI);
    jsTestLog("Running against target Atlas cluster");
}

// Setup the sp client handle (either a locally running mongod, or
// a stream processing instance in Atlas Stream Processing).
let sp = getDefaultSp(targetClusterURI);
let aspUri = _getEnv("ASP_URI");
if (aspUri != "" && aspUri != null) {
    jsTestLog("Using ASP service");
    sp = getAtlasStreamProcessingInstance(aspUri);
}

let targetDb = targetConn.getDB(test.dbName);
const spName = "simple_merge_benchmark";
const inputColl = targetDb.getSiblingDB(test.dbName)[test.inputCollName];
const aggOutputCollName = `agg-${test.outputCollName}`;
const aggOutputColl = targetDb.getSiblingDB(test.dbName)[aggOutputCollName];
const aggWhenMatchedOutputCollName = `aggwhenmatched-${test.outputCollName}`;
const aggWhenMatchOutputColl = targetDb.getSiblingDB(test.dbName)[aggWhenMatchedOutputCollName];
const timeoutMs = 10 * 1000 * 60;
const pollingMs = 200;
const baseDocCount = 100000;

function generateInput({docSize, docCount}) {
    // 3 integers, one _id.
    const approxSizeNonStrFields = 3 * 8 + 12;
    const strSize = docSize - approxSizeNonStrFields;
    const str = makeRandomString(strSize);
    const makeDoc = (idx) => {
        return {
            str: str,
            int1: idx,
            int2: 2 * idx,
            int3: 3 * idx,
        };
    };
    let input = [];
    for (let idx = 0; idx < docCount; idx += 1) {
        input.push(makeDoc(idx));
    }
    return input;
}

// Insert the data into the input collection. Returns an operationTimestamp before the inserts.
function setup(input) {
    const startTime = targetDb.hello().$clusterTime.clusterTime;
    const batchSize = 1000;
    let batch = [];
    const flush = () => {
        if (batch.length > 0) {
            inputColl.insertMany(batch);
            batch = [];
        }
    };
    for (const doc of input) {
        batch.push(doc);
        if (batch.length == batchSize) {
            flush();
        }
    }
    flush();
    assert.eq(input.length, inputColl.count());
    return startTime;
}

// Run a $source, $merge streamProcessor beginning from clusterTime.
function runStreamProcessor(clusterTime, inputLength, parallelism) {
    const collName = test.outputCollName + parallelism ? `-${parallelism}` : "";
    const outputColl = targetDb.getSiblingDB(test.dbName)[collName];
    outputColl.drop();
    let pipeline = [
        {
            $source: {
                connectionName: test.atlasConnection,
                db: test.dbName,
                coll: test.inputCollName,
                config: {startAtOperationTime: clusterTime}
            }
        },
        {$match: {operationType: "insert"}},
        {$project: {_id: 0}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {$merge: {into: {connectionName: test.atlasConnection, db: test.dbName, coll: collName}}},
    ];
    if (parallelism) {
        pipeline[pipeline.length - 1]["$merge"]["parallelism"] = NumberInt(parallelism);
    }
    sp.createStreamProcessor(spName, pipeline);
    const startTime = new Date();
    sp[spName].start();
    assert.soon(() => { return outputColl.count() == inputLength; },
                "waiting for expected output",
                timeoutMs,
                pollingMs);
    const endTime = new Date();
    sp[spName].stop();
    return {time: endTime - startTime, outputColl: outputColl};
}

// Run aggregate with default $merge.
function runAggregate() {
    const startTime = new Date();
    inputColl.aggregate([{$merge: {into: {db: test.dbName, coll: aggOutputCollName}}}]);
    return new Date() - startTime;
}

// Run aggregate with $merge whenMatched="fail".
function runAggregateWithWhenMatchedFailed() {
    const startTime = new Date();
    inputColl.aggregate([
        {$merge: {into: {db: test.dbName, coll: aggWhenMatchedOutputCollName}, whenMatched: "fail"}}
    ]);
    return new Date() - startTime;
}

function getAvgResults(results) {
    let avg = {};
    for (let result of results) {
        for (let prop in result) {
            if (result.hasOwnProperty(prop)) {
                let time = result[prop];
                if (!avg.hasOwnProperty(prop)) {
                    avg[prop] = {sum: 0, count: 0};
                }
                avg[prop] = {sum: avg[prop].sum + time, count: avg[prop].count + 1};
            }
        }
    }

    jsTestLog(`running avg ${tojson(avg)}`);
    for (let prop in avg) {
        avg[prop] = avg[prop].sum / avg[prop].count;
    }

    return avg;
}

function benchmarkChangestreamSource(reps) {
    // Set the oplog to 16GB.
    targetDb.adminCommand({replSetResizeOplog: 1, size: 16000});

    let results = [];
    for (let i = 0; i < reps; i += 1) {
        // Set the input of 1million 1kB documents, about 1GB of data.
        inputColl.drop();
        aggOutputColl.drop();
        aggWhenMatchOutputColl.drop();
        Random.setRandomSeed(42);
        let input = generateInput({docSize: 1000, docCount: baseDocCount});
        const inputLength = input.length;
        const clusterTime = setup(input);
        input = [];

        // Run the workloads.
        let aggResult = runAggregate();
        let insertAggResult = runAggregateWithWhenMatchedFailed();
        let spResults = {};
        if (sp != null) {
            for (let parallelism of [1, 2, 4, 8]) {
                const {time, outputColl} =
                    runStreamProcessor(clusterTime, inputLength, parallelism);
                assert.eq(inputLength, outputColl.count());
                spResults["spparallelism-" + parallelism] = time;
                if (sp != null) {
                    sp[spName].drop(false /*assertWorked*/);
                }
                outputColl.drop();
            }
        }

        let iteration = {aggResult: aggResult, insertAggResult: insertAggResult};
        iteration = Object.assign(iteration, spResults);
        results.push(iteration);

        assert.eq(inputLength, aggOutputColl.count());
        assert.eq(inputLength, aggWhenMatchOutputColl.count());
        inputColl.drop();
        aggOutputColl.drop();
        aggWhenMatchOutputColl.drop();
    }

    jsTestLog(`results benchmarkChangestreamSource: ${tojson(results)}`);
    jsTestLog(`final avg benchmarkChangestreamSource: ${tojson(getAvgResults(results))}`);
}

function benchmarkUnwindDuplication(reps) {
    // Set the oplog to 16GB.
    targetDb.adminCommand({replSetResizeOplog: 1, size: 16000});

    let docCount = 2 * baseDocCount;
    let outputDocsPerDoc = 1000;
    let arr = Array.from(Array(outputDocsPerDoc).keys());
    let inputDocs = docCount / outputDocsPerDoc;
    let input = Array.from(Array(inputDocs).keys()).map(i => { return {i: i, arr: arr}; });

    let results = [];
    for (let i = 0; i < reps; i += 1) {
        // Set the input of 1million 1kB documents, about 1GB of data.
        inputColl.drop();
        const clusterTime = setup(input);

        // Run the workloads.
        let spResults = {};
        if (sp != null) {
            for (let parallelism of [1, 2, 4, 8]) {
                const collName = test.outputCollName + parallelism ? `-${parallelism}` : "";
                const outputColl = targetDb.getSiblingDB(test.dbName)[collName];
                outputColl.drop();
                let pipeline = [
                    {
                        $source: {
                            connectionName: test.atlasConnection,
                            db: test.dbName,
                            coll: test.inputCollName,
                            config: {
                                startAtOperationTime: clusterTime,
                            }
                        }
                    },
                    {$match: {operationType: "insert"}},
                    {$replaceRoot: {newRoot: "$fullDocument"}},
                    {$project: {_id: 0}},
                    {$unwind: "$arr"},
                    {
                        $merge: {
                            into: {
                                connectionName: test.atlasConnection,
                                db: test.dbName,
                                coll: collName
                            }
                        }
                    },
                ];
                if (parallelism) {
                    pipeline[pipeline.length - 1]["$merge"]["parallelism"] = NumberInt(parallelism);
                }
                sp.createStreamProcessor(spName, pipeline);
                const startTime = new Date();
                sp[spName].start();

                assert.soon(() => { return outputColl.count() == docCount; },
                            "waiting for expected output",
                            timeoutMs,
                            pollingMs);

                const endTime = new Date();
                sp[spName].stop();
                let time = endTime - startTime;

                assert.eq(docCount, outputColl.count());
                spResults["spparallelism-" + parallelism] = time;
                if (sp != null) {
                    sp[spName].drop(false /*assertWorked*/);
                }
            }
        }

        let iteration = {};
        iteration = Object.assign(iteration, spResults);
        results.push(iteration);

        inputColl.drop();
    }

    jsTestLog(`results benchmarkUnwindDuplication: ${tojson(results)}`);
    jsTestLog(`final benchmarkUnwindDuplication: ${tojson(getAvgResults(results))}`);
}

const buildInfo = targetDb.getServerBuildInfo();
if (!buildInfo.isDebug() && !buildInfo.isAddressSanitizerActive() &&
    !buildInfo.isThreadSanitizerActive() && !buildInfo.isLeakSanitizerActive()) {
    // Don't run this on debug builds or sanitizer builds in Evergreen, it takes too long.
    const reps = 1;
    benchmarkChangestreamSource(reps);
    benchmarkUnwindDuplication(reps);
}