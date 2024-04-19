import {Thread} from "jstests/libs/parallelTester.js";
import {
    connectionName,
    dbName,
    generateDocs,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorWindowTest,
    sanitizeDoc,
    windowPipelines
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "windowGroupMultiThreadingTest";
const coll = db.project_coll;

function getWindowPipeline(pipeline) {
    return {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: "second"},
            allowedLateness: {size: NumberInt(3), unit: "second"},
            pipeline: pipeline
        }
    };
}

// multiple tests with different size of documents with various fields

// This function inserts the documents and computes the aggregate results and compares them
// with passing the same set of documents into a tumbling window and run the same aggregation.
const windowGroupMultithreading = function(
    docs, pipeline, batchSize, comparisonFunc = assert.eq, stripIds = false) {
    coll.drop();
    coll.insert(docs.slice(0, docs.length - 1));
    let terminateStats = false;
    let counter = new CountDownLatch(1);
    const asyncStatsFunction = async function(spName, _counter) {
        while (_counter.getCount() > 0) {
            jsTestLog("calling getStats");
            let getStatsCmd = {streams_getStats: '', name: spName, verbose: true};
            let result = db.runCommand(getStatsCmd);
            sleep(100);
        }
    };

    const asyncMetricsFunction = async function(_counter) {
        while (_counter.getCount() > 0) {
            jsTestLog("calling getMetrics");
            let getMetricsCmd = {streams_getMetrics: ''};
            let result = db.runCommand(getMetricsCmd);
            sleep(100);
        }
    };

    const asyncMoreFunction = async function(spName, _counter) {
        // Start a sample on the stream processor.
        let startSampleCmd = {streams_startStreamSample: '', name: 'sampleTest', limit: 4};
        let result = db.runCommand(startSampleCmd);
        let cursorId = result["id"];
        while (_counter.getCount() > 0) {
            jsTestLog("calling getMore");
            let getSampleCmd = {streams_getMoreStreamSample: cursorId, name: spName, verbose: true};
            result = db.runCommand(getSampleCmd);
            sleep(100);
        }
    };

    // we trust the aggregation to work correctly in the db layer and use it to compute expected
    // results
    const expectedResults =
        coll.aggregate(pipeline, {cursor: {batchSize: batchSize}})._batch.reverse();
    runStreamProcessorWindowTest({
        spName: spName,
        pipeline: [
            {
                $source: {
                    connectionName: "kafka1",
                    topic: "test1",
                    timeField: {$dateFromString: {"dateString": "$ts"}},
                    testOnlyPartitionCount: NumberInt(1)
                }
            },
            getWindowPipeline(pipeline),
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
        ],
        verifyAction: () => {
            let statsThread = new Thread(asyncStatsFunction, spName, counter);
            statsThread.start();
            let metricsThread = new Thread(asyncMetricsFunction, counter);
            metricsThread.start();
            let sampleThread = new Thread(asyncMoreFunction, spName, counter);
            sampleThread.start();
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            var fieldNames = ['_ts', '_stream_meta'];
            if (stripIds) {
                fieldNames.push('_id');
            }
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc, fieldNames));
            // doing it in a loop just to make comparison easier in case of test failure
            for (let i = 0; i < results.length; i++) {
                comparisonFunc(expectedResults[i], results[i]);
            }
            terminateStats = true;
            counter.countDown();
            counter = null;
            statsThread.join();
            metricsThread.join();
            sampleThread.join();
        }
    });
};

Random.setRandomSeed(20230328);

(function testWindowGrouMultiThreaded() {
    const docs = generateDocs(10000);

    for (const pipelineDef of windowPipelines) {
        windowGroupMultithreading(
            docs,
            pipelineDef.pipeline,
            docs.length,
            pipelineDef.compareFunction ? pipelineDef.compareFunction : assert.eq);
    }
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);