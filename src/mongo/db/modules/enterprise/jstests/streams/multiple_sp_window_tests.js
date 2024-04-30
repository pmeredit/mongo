import {Thread} from "jstests/libs/parallelTester.js";
import {
    startStreamProcessorForThread
} from 'src/mongo/db/modules/enterprise/jstests/streams/multithreading_utils.js';
import {
    generateDocs,
    listStreamProcessors
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const spName = "windowGroupTest";

function getWindowPipeline(pipeline) {
    return [{
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: "second"},
            allowedLateness: {size: NumberInt(3), unit: "second"},
            pipeline: pipeline
        }
    }];
}

// multiple tests with different size of documents with various fields
// Because it is run with threads, need to flatten the code. (no access to functions defined in
// another class) This function inserts the documents and computes the aggregate results and
// compares them with passing the same set of documents into a tumbling window and run the same
// aggregation.
const windowGroupMTFunc = function testWindowGroup(
    docs, pipeline, batchSize, spName, threadId, comparisonFunc = assert.eq, stripIds = false) {
    const dbName = `${jsTestName()}${threadId}`;
    jsTestLog(`dbName=${dbName} spName=${spName}`);
    const coll = db.getSiblingDB(dbName).project_coll;
    const outColl = db.getSiblingDB(dbName).outColl;
    jsTestLog(`outColl name=${outColl.getName()}`);
    coll.drop();
    coll.insert(docs.slice(0, docs.length - 1));
    // we trust the aggregation to work correctly in the db layer and use it to compute expected
    // results
    const expectedResults =
        coll.aggregate(pipeline, {cursor: {batchSize: batchSize}})._batch.reverse();

    jsTestLog(`expecting ${expectedResults.length}`);
    let insertCmd = {
        streams_testOnlyInsert: '',
        name: `${spName}${threadId}`,
        documents: docs,
    };

    let result = db.runCommand(insertCmd);
    jsTestLog(result);
    assert.commandWorked(result);
    assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                () => { jsTestLog(`${outColl.find().itcount()}`); });
    var fieldNames = ['_ts', '_stream_meta'];
    if (stripIds) {
        fieldNames.push('_id');
    }
    const sanitizeDoc = function(doc, fieldNames) {
        let clone = Object.assign({}, doc);
        for (let fieldName of fieldNames) {
            delete clone[fieldName];
        }
        return clone;
    };
    let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc, fieldNames));
    // doing it in a loop just to make comparison easier in case of test failure
    for (let i = 0; i < results.length; i++) {
        comparisonFunc(expectedResults[i], results[i]);
    }
    let stopCmd = {
        streams_stopStreamProcessor: '',
        name: `${spName}${threadId}`,
    };
    jsTestLog(`Stopping ${spName} - \n${tojson(stopCmd)}`);
    return assert.commandWorked(db.runCommand(stopCmd));
};

Random.setRandomSeed(20230328);
let minThreadId = 1;

function runMultiThreadedTest(pipeline, docs, batchSize) {
    let threads = [];
    const maxThreadId = minThreadId + 20;
    for (; minThreadId < maxThreadId; minThreadId++) {
        startStreamProcessorForThread(
            {pipeline: getWindowPipeline(pipeline), spName: spName, threadId: minThreadId});
        // windowGroupMTFunc(docs, [{$sort: {b: 1, d: 1}}, {$limit: 50}], 50, spName, 1);
        let thread = new Thread(windowGroupMTFunc, docs, pipeline, batchSize, spName, minThreadId);
        threads.push(thread);
        thread.start();
    }
    for (let threadIndex = 0; threadIndex < threads.length; threadIndex++) {
        threads[threadIndex].join();
    }
}

// each test is spawning 20 threads so limiting the number window tests we run for now.
(function runMultiThreadedTests() {
    let docs = generateDocs(5000);
    // _id is included in the sort keys because none of a, b, or d is guaranteed to be unique, and
    // we need a unique field to break ties so that we can match against an expected ordering.
    runMultiThreadedTest([{$sort: {b: 1, d: 1, _id: 1}}, {$limit: 50}], docs, 50);
    runMultiThreadedTest([{$sort: {a: 1, b: 1, _id: 1}}], docs, docs.length);
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);