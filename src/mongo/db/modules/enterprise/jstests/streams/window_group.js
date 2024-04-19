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
const spName = "windowGroupTest";
const coll = db.project_coll;

function getGroupPipeline(pipeline) {
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
const windowPipelineFunc = function(
    docs, pipeline, batchSize, comparisonFunc = assert.eq, stripIds = false) {
    coll.drop();
    coll.insert(docs.slice(0, docs.length - 1));
    // we trust the aggregation to work correctly in the db layer and use it to compute expected
    // results
    const expectedResults =
        coll.aggregate(pipeline, {cursor: {batchSize: batchSize}})._batch.reverse();
    jsTestLog(`expectedResults.size=${expectedResults.length}, batch size=${batchSize}`);
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
            getGroupPipeline(pipeline),
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
        }
    });
};

Random.setRandomSeed(20230328);

(function testWindowGroup() {
    const sizes = [10, 100, 1000, 10000];

    for (let x = 0; x < sizes.length; x++) {
        const docs = generateDocs(sizes[x]);
        for (const pipelineDef of windowPipelines) {
            windowPipelineFunc(
                docs,
                pipelineDef.pipeline,
                docs.length,
                pipelineDef.compareFunction ? pipelineDef.compareFunction : assert.eq);
        }
    }
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);

// TODO write a test for $lookup, $mergeObjects inside of $group.