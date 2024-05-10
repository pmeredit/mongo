import {
    getAtlasStreamProcessingHandle,
    getDefaultSp,
    test
} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {makeRandomString} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

function benchmark() {
    const dbUri = db.getMongo().host;
    const runningOnAtlas = dbUri.includes("mongodb.net");
    let sp = null;
    if (runningOnAtlas) {
        let aspUri = _getEnv("ASP_URI");
        if (aspUri != "" && aspUri != null) {
            sp = getAtlasStreamProcessingHandle(aspUri);
        }
    } else {
        // Running locally.
        sp = getDefaultSp();
    }

    const spName = "simple_merge_benchmark";
    const windowSpName = "window_simple_merge_benchmark";
    const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];
    const spOutputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
    const spWindowOutputCollName = "window-" + test.outputCollName;
    const spWindowOutputColl = db.getSiblingDB(test.dbName)[spWindowOutputCollName];
    const aggOutputCollName = `agg-${test.outputCollName}`;
    const aggOutputColl = db.getSiblingDB(test.dbName)[aggOutputCollName];
    const aggWhenMatchedOutputCollName = `aggwhenmatched-${test.outputCollName}`;
    const aggWhenMatchOutputColl = db.getSiblingDB(test.dbName)[aggWhenMatchedOutputCollName];
    inputColl.drop();
    spWindowOutputColl.drop();
    spOutputColl.drop();
    aggOutputColl.drop();
    aggWhenMatchOutputColl.drop();
    Random.setRandomSeed(42);
    // Set the oplog to 16GB.
    db.adminCommand({replSetResizeOplog: 1, size: 16000});

    // The input is 10k 1kB documents, about 10MB of data.
    let input = generateInput({docSize: 1000, docCount: 10000});
    const inputLength = input.length;
    const clusterTime = setup(input);
    input = [];

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
        const startTime = db.hello().$clusterTime.clusterTime;
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
    function runStreamProcessor(clusterTime) {
        sp.createStreamProcessor(spName, [
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
            {
                $merge: {
                    into: {
                        connectionName: test.atlasConnection,
                        db: test.dbName,
                        coll: test.outputCollName
                    }
                }
            },
        ]);
        const startTime = new Date();
        sp[spName].start();
        waitForDone(spOutputColl, inputLength);
        return new Date() - startTime;
    }

    // Run a $source,$tumblingWindow,$merge streamProcessor beginning from clusterTime.
    function runStreamProcessorWithWindow(clusterTime) {
        sp.createStreamProcessor(windowSpName, [
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
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    idleTimeout: {size: NumberInt(10), unit: "second"},
                    allowedLateness: {size: NumberInt(10), unit: "second"},
                    pipeline: [
                        {$group: {_id: '$$ROOT', count: {$sum: 1}}},
                        {$project: {_id: 0, data: "$_id", count: 1}},
                    ]
                }
            },
            {
                $merge: {
                    into: {
                        connectionName: test.atlasConnection,
                        db: test.dbName,
                        coll: spWindowOutputCollName
                    }
                }
            },
        ]);
        const startTime = new Date();
        sp[windowSpName].start();
        waitForDone(spWindowOutputColl, inputLength);
        return new Date() - startTime;
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
        inputColl.aggregate([{
            $merge:
                {into: {db: test.dbName, coll: aggWhenMatchedOutputCollName}, whenMatched: "fail"}
        }]);
        return new Date() - startTime;
    }

    // Run the workloads.
    let aggResult = runAggregate();
    let insertAggResult = runAggregateWithWhenMatchedFailed();
    let spResult = null;
    let spWindowResult = null;
    if (sp != null) {
        spResult = runStreamProcessor(clusterTime);
        spWindowResult = runStreamProcessorWithWindow(clusterTime);
    }

    jsTestLog("simple_merge_benchmark results:");
    jsTestLog({
        "sp": spResult,
        "spWithWindow": spWindowResult,
        "agg": aggResult,
        "insertAgg": insertAggResult
    });

    if (sp != null) {
        sp[spName].drop(false /*assertWorked*/);
        sp[windowSpName].drop(false /*assertWorked*/);
        assert.eq(inputLength, spOutputColl.count());
        assert.eq(inputLength, spWindowOutputColl.count());
    }
    assert.eq(inputLength, aggOutputColl.count());
    assert.eq(inputLength, aggWhenMatchOutputColl.count());
    inputColl.drop();
    spWindowOutputColl.drop();
    spOutputColl.drop();
    aggOutputColl.drop();
    aggWhenMatchOutputColl.drop();
}

const debugBuild = db.adminCommand("buildInfo").debug;
if (!debugBuild) {
    // Don't run this on debug builds in Evergreen, it takes too long.
    benchmark();
}