import {
    dbName,
    insertDocs,
    logState,
    runStreamProcessorWindowTest,
    sanitizeDoc
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "windowGroupTest";
const coll = db.project_coll;

function getGroupPipeline(pipeline) {
    return [{
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: "second"},
            allowedLateness: {size: NumberInt(3), unit: "second"},
            pipeline: pipeline
        }
    }];
}

// multiple tests with different size of documents with various fields

// This function inserts the documents and computes the aggregate results and compares them
// with passing the same set of documents into a tumbling window and run the same aggregation.
const windowGroupFunc = function testWindowGroup(
    docs, pipeline, batchSize, comparisonFunc = assert.eq, stripIds = false) {
    coll.drop();
    coll.insert(docs.slice(0, docs.length - 1));
    // we trust the aggregation to work correctly in the db layer and use it to compute expected
    // results
    const expectedResults =
        coll.aggregate(pipeline, {cursor: {batchSize: batchSize}})._batch.reverse();
    runStreamProcessorWindowTest({
        spName: spName,
        pipeline: getGroupPipeline(pipeline),
        dateField: "date",
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

var startingDate = new Date('2023-03-03T20:42:30.000Z').getTime();

function makeid(length) {
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

function generateDocs(docSize) {
    const docs = [];
    for (let i = 0; i < docSize; i++) {
        var curDate = new Date(startingDate + i / docSize * 1000);
        docs.push({
            _id: i,
            a: Random.randInt(Math.max(docSize / 10, 10)),
            b: Random.randInt(docSize),
            c: Random.randInt(docSize),
            d: makeid(10),
            date: curDate.toISOString()
        });
    }
    // add one document which is 4 seconds from the starttime.
    docs.push({
        id: docSize,
        date: new Date(startingDate + 4001).toISOString()
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
            assert.eq(Array.sort(expectedValue), Array.sort(resultValue));
        } else {
            assert.eq(expectedValue, resultValue);
        }
    }
}

(function
     testWindowGroup() {
         const sizes = [10, 100, 1000, 10000];

         for (let x = 0; x < sizes.length; x++) {
             const docs = generateDocs(sizes[x]);
             windowGroupFunc(
                 docs,
                 [
                     {
                         $group: {
                             _id: "$a",
                             avg_a: {$avg: "$b"},
                             bottom_a:
                                 {$bottom: {output: ["$b", "$c"], sortBy: {"c": -1, "b": -1}}},
                             bottomn_a: {
                                 $bottomN: {output: ["$b", "$c"], sortBy: {"c": -1, "b": 1}, n: 3}
                             },
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
                             percentile_c:
                                 {$percentile: {input: "$c", p: [0.95], method: 'approximate'}},
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
                 ],
                 Math.max(docs.length / 10, 10));
             windowGroupFunc(docs,
                             [
                                 {
                                     $group: {
                                         _id: "$a",
                                         stddev_b: {$stdDevSamp: "$c"},
                                         stddevpop_b: {$stdDevPop: "$c"},
                                     }
                                 },
                                 {$sort: {_id: 1}}
                             ],
                             Math.max(docs.length / 10, 10),
                             approximateDataEquality);
             windowGroupFunc(docs,
                             [{$group: {_id: "$a", result: {$addToSet: "$b"}}}, {$sort: {_id: 1}}],
                             Math.max(docs.length / 10, 10),
                             ignoreOrderInSetCompare);
             windowGroupFunc(docs, [{$sort: {a: 1, b: 1}}], docs.length);
             windowGroupFunc(docs, [{$sort: {a: 1, c: 1}}], docs.length);
             windowGroupFunc(docs, [{$sort: {b: 1, c: 1}}], docs.length);
             windowGroupFunc(docs, [{$sort: {d: 1}}], docs.length);
             windowGroupFunc(docs, [{$sort: {b: 1, d: 1}}], docs.length);
             windowGroupFunc(docs, [{$sort: {d: 1}}, {$limit: 100}], 100);
             windowGroupFunc(docs, [{$sort: {b: 1, d: 1}}, {$limit: 50}], 50);
         }
     }());

// TODO write a test for $lookup, $mergeObjects inside of $group.