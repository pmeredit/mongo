import {
    getAtlasStreamProcessingInstance,
    getDefaultSp,
    test
} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {makeRandomString} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

let sp = null;

// by default use the local mongod as the source/merge target
let targetConn = new Mongo(db.getMongo().host);

let targetClusterURI = _getEnv("ATLAS_CLUSTER_URI");
if (targetClusterURI && targetClusterURI !== null && targetClusterURI !== "") {
    // Use a remote Atlas cluster as the source/merge target.
    targetConn = new Mongo(targetClusterURI);
    jsTestLog("Running against target Atlas cluster");
}
let targetDb = targetConn.getDB(test.dbName);
const spName = "simple_merge_benchmark";
const inputColl = targetDb.getSiblingDB(test.dbName)[test.inputCollName];
const aggOutputCollName = `agg-${test.outputCollName}`;
const aggOutputColl = targetDb.getSiblingDB(test.dbName)[aggOutputCollName];
const aggWhenMatchedOutputCollName = `aggwhenmatched-${test.outputCollName}`;
const aggWhenMatchOutputColl = targetDb.getSiblingDB(test.dbName)[aggWhenMatchedOutputCollName];

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

// Wait for the expected numbers of messages to exist in the target coll.
function waitForDone(coll, expectedMessages, pollingIntervalMs = 200) {
    while (coll.count() != expectedMessages) {
        sleep(pollingIntervalMs);
    }
}

// Run a $source, $merge streamProcessor beginning from clusterTime.
function runStreamProcessor(clusterTime, inputLength, parallelism) {
    const collName = test.outputCollName + parallelism ? `-${parallelism}` : "";
    const outputColl = targetDb.getSiblingDB(test.dbName)[collName];
    let pipeline = [
        {
            $source: {
                connectionName: test.atlasConnection,
                db: test.dbName,
                coll: test.inputCollName,
                config: {
                    startAtOperationTime: clusterTime,
                    fullDocumentOnly: true,
                    fullDocument: "required"
                }
            }
        },
        {$merge: {into: {connectionName: test.atlasConnection, db: test.dbName, coll: collName}}},
    ];
    if (parallelism) {
        pipeline[pipeline.length - 1]["$merge"]["parallelism"] = NumberInt(parallelism);
    }
    sp.createStreamProcessor(spName, pipeline);
    const startTime = new Date();
    sp[spName].start();
    waitForDone(outputColl, inputLength);
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

function benchmark() {
    sp = getDefaultSp(targetClusterURI);
    let aspUri = _getEnv("ASP_URI");
    if (aspUri != "" && aspUri != null) {
        sp = getAtlasStreamProcessingInstance(aspUri);
    }

    inputColl.drop();
    aggOutputColl.drop();
    aggWhenMatchOutputColl.drop();
    Random.setRandomSeed(42);
    // Set the oplog to 16GB.
    targetDb.adminCommand({replSetResizeOplog: 1, size: 16000});

    // The input is 1million 1kB documents, about 1GB of data.
    let input = generateInput({docSize: 1000, docCount: 1000000});
    const inputLength = input.length;
    const clusterTime = setup(input);
    input = [];

    // Run the workloads.
    let aggResult = runAggregate();
    let insertAggResult = runAggregateWithWhenMatchedFailed();
    let spResults = [];
    if (sp != null) {
        for (let parallelism of [1, 2, 4, 8]) {
            const {time, outputColl} = runStreamProcessor(clusterTime, inputLength, parallelism);
            assert.eq(inputLength, outputColl.count());
            outputColl.drop();
            spResults.push({time: time, parallelism: parallelism});
        }
    }

    jsTestLog("simple_merge_benchmark results:");
    jsTestLog({"sp": spResults, "agg": aggResult, "insertAgg": insertAggResult});

    if (sp != null) {
        sp[spName].drop(false /*assertWorked*/);
    }
    assert.eq(inputLength, aggOutputColl.count());
    assert.eq(inputLength, aggWhenMatchOutputColl.count());
    inputColl.drop();
    aggOutputColl.drop();
    aggWhenMatchOutputColl.drop();
}

const buildInfo = targetDb.getServerBuildInfo();
if (!buildInfo.isDebug() && !buildInfo.isAddressSanitizerActive() &&
    !buildInfo.isThreadSanitizerActive() && !buildInfo.isLeakSanitizerActive()) {
    // Don't run this on debug builds or sanitizer builds in Evergreen, it takes too long.
    benchmark();
}