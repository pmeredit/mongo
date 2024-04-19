import {
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "unwindTest";

const unwindFunc = function(docs, unwindString, expectedResults) {
    const pipeline = [{$project: unwindString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$unwind: unwindString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map(
                (doc) => sanitizeDoc(doc, ['_ts', '_stream_meta', '_id']));
            assert.eq(expectedResults, results);
        }
    });
};

unwindFunc(
    [
        {_id: 1},
        {_id: 2, x: null},
        {_id: 3, x: []},
        {_id: 4, x: [1, 2]},
        {_id: 5, x: [3]},
        {_id: 6, x: 4}
    ],
    "$x",
    [{x: 2}, {x: 3}, {x: 4}]);

const unwind2Func = function unwind2(docs, unwindString, expectedResults) {
    runStreamProcessorOperatorTest({
        pipeline: [{$unwind: unwindString}, {$project: {_id: 0}}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() >= expectedResults.length; },
                        logState(spName));
            let results = outColl.find().toArray().map(
                (doc) => sanitizeDoc(doc, ['_ts', '_stream_meta', '_id']));
            assert.eq(expectedResults, results);
        }
    });
};

unwind2Func(
    [
        {_id: 1},
        {_id: 2, x: null},
        {_id: 3, x: []},
        {_id: 4, x: [1, 2]},
        {_id: 5, x: [3]},
        {_id: 6, x: 4}
    ],
    "$x",
    [{x: 1}, {x: 2}, {x: 3}, {x: 4}]);

unwind2Func(
    [
        {"_id": 1, "item": "Shirt", "sizes": ["S", "M", "L"]},
        {"_id": 2, "item": "Shorts", "sizes": []},
        {"_id": 3, "item": "Hat", "sizes": "M"},
        {"_id": 4, "item": "Gloves"},
        {"_id": 5, "item": "Scarf", "sizes": null}
    ],
    "$sizes",
    [
        {item: 'Shirt', sizes: 'S'},
        {item: 'Shirt', sizes: 'M'},
        {item: 'Shirt', sizes: 'L'},
        {item: 'Hat', sizes: 'M'}
    ]);

unwind2Func(
    [
        {"_id": 1, "item": "ABC", price: NumberDecimal("80"), "sizes": ["S", "M", "L"]},
        {"_id": 2, "item": "EFG", price: NumberDecimal("120"), "sizes": []},
        {"_id": 3, "item": "IJK", price: NumberDecimal("160"), "sizes": "M"},
        {"_id": 4, "item": "LMN", price: NumberDecimal("10")},
        {"_id": 5, "item": "XYZ", price: NumberDecimal("5.75"), "sizes": null}
    ],
    {path: "$sizes", includeArrayIndex: "arrayIndex"},
    [
        {"arrayIndex": NumberLong(0), "item": "ABC", "price": NumberDecimal("80"), "sizes": "S"},
        {"arrayIndex": NumberLong(1), "item": "ABC", "price": NumberDecimal("80"), "sizes": "M"},
        {"arrayIndex": NumberLong(2), "item": "ABC", "price": NumberDecimal("80"), "sizes": "L"},
        {"arrayIndex": null, "item": "IJK", "price": NumberDecimal("160"), "sizes": "M"},
    ]);

// This test verifies STREAMS-738
const maxFields = 10;
var doc = {};
const maxStringLen = 1600000;
var a = [];
var expectedDocs = [];
const largeString = new Array(maxStringLen + 1).join('a');
for (let i = 0; i < maxFields; i++) {
    a.push(largeString);
    var expectedDoc = {};
    expectedDoc['a'] = largeString;
    expectedDocs.push(expectedDoc);
}
doc['a'] = a;
unwind2Func([doc], "$a", expectedDocs);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);