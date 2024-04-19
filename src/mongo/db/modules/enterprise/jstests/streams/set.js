import {
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "setTest";

const setFunc = function(docs, setString, expectedResults) {
    const pipeline = [{$project: setString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$set: setString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results =
                outColl.find().toArray().map((doc) => sanitizeDoc(doc, ['_ts', '_stream_meta']));
            assert.eq(expectedResults, results);
        }
    });
};

setFunc(
    [
        {_id: 1, type: "car", specs: {doors: 4, wheels: 4}},
        {_id: 2, type: "motorcycle", specs: {doors: 0, wheels: 2}},
        {_id: 3, type: "jet ski"}
    ],
    {"specs.fuel_type": "unleaded"},
    [
        {_id: 1, specs: {doors: 4, wheels: 4, fuel_type: "unleaded"}, type: "car"},
        {_id: 2, specs: {doors: 0, wheels: 2, fuel_type: "unleaded"}, type: "motorcycle"},
        {_id: 3, specs: {fuel_type: "unleaded"}, type: "jet ski"}
    ]);

setFunc([{_id: 1, dogs: 10, cats: 15}], {"cats": 20}, [{_id: 1, cats: 20, dogs: 10}]);

setFunc(
    [
        {"_id": 1, "item": "tangerine", "type": "citrus"},
        {"_id": 2, "item": "lemon", "type": "citrus"},
        {"_id": 3, "item": "grapefruit", "type": "citrus"}
    ],
    {_id: "$item", item: "fruit"},
    [
        {"_id": "tangerine", "item": "fruit", "type": "citrus"},
        {"_id": "lemon", "item": "fruit", "type": "citrus"},
        {"_id": "grapefruit", "item": "fruit", "type": "citrus"}
    ]);

setFunc(
    [
        {_id: 1, student: "Maya", homework: [10, 5, 10], quiz: [10, 8], extraCredit: 0},
        {_id: 2, student: "Ryan", homework: [5, 6, 5], quiz: [8, 8], extraCredit: 8}
    ],
    {quizAverage: {$avg: "$quiz"}},
    [
        {
            _id: 1,
            extraCredit: 0,
            homework: [10, 5, 10],
            quiz: [10, 8],
            quizAverage: 9,
            student: 'Maya',
        },
        {
            _id: 2,
            extraCredit: 8,
            homework: [5, 6, 5],
            quiz: [8, 8],
            quizAverage: 8,
            student: 'Ryan',
        }
    ]);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);