import {
    dbName,
    generate16MBDoc,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "matchOperatorTest";
// tests from /jstests/core/match_numeric_components.js
const kDocs = [
    {_id: 0, "a": 42},
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 3, "a": [[42]]},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 6, "a": {"0": {"0": 42}}},
    {_id: 7, "a": [[[42]]]},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 9, "a": [{"0": [42]}]},
    {_id: 10, "a": [{"0": {"0": 42}}]},
    {_id: 11, "a": {"0": [[42]]}},
    {_id: 12, "a": {"0": [{"0": 42}]}},
    {_id: 13, "a": {"0": {"0": [42]}}},
    {_id: 14, "a": {"0": {"0": {"0": 42}}}},
    {_id: 15, "a": [[[[42]]]]},
    {_id: 16, "a": [[[{"0": 42}]]]},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 18, "a": [[{"0": {"0": 42}}]]},
    {_id: 19, "a": [{"0": [[42]]}]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 21, "a": [{"0": {"0": [42]}}]},
    {_id: 22, "a": [{"0": {"0": {"0": 42}}}]},
    {_id: 23, "a": {"0": [[[42]]]}},
    {_id: 24, "a": {"0": [[{"0": 42}]]}},
    {_id: 25, "a": {"0": [{"0": [42]}]}},
    {_id: 26, "a": {"0": [{"0": {"0": 42}}]}},
    {_id: 27, "a": {"0": {"0": [[42]]}}},
    {_id: 28, "a": {"0": {"0": [{"0": 42}]}}},
    {_id: 29, "a": {"0": {"0": {"0": [42]}}}},
    {_id: 30, "a": {"0": {"0": {"0": {"0": 42}}}}},
];

const matchFunc = function(docs, matchString, expectedResults) {
    runStreamProcessorOperatorTest({
        pipeline: [
            {$match: matchString},
        ],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
        }
    });
};

matchFunc(kDocs, {'a.0': 42}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

// Using $ne.
matchFunc(kDocs, {"a.0": {$ne: 42}}, [
    {_id: 0, "a": 42},
    {_id: 3, "a": [[42]]},
    {_id: 6, "a": {"0": {"0": 42}}},
    {_id: 7, "a": [[[42]]]},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 10, "a": [{"0": {"0": 42}}]},
    {_id: 11, "a": {"0": [[42]]}},
    {_id: 12, "a": {"0": [{"0": 42}]}},
    {_id: 13, "a": {"0": {"0": [42]}}},
    {_id: 14, "a": {"0": {"0": {"0": 42}}}},
    {_id: 15, "a": [[[[42]]]]},
    {_id: 16, "a": [[[{"0": 42}]]]},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 18, "a": [[{"0": {"0": 42}}]]},
    {_id: 19, "a": [{"0": [[42]]}]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 21, "a": [{"0": {"0": [42]}}]},
    {_id: 22, "a": [{"0": {"0": {"0": 42}}}]},
    {_id: 23, "a": {"0": [[[42]]]}},
    {_id: 24, "a": {"0": [[{"0": 42}]]}},
    {_id: 25, "a": {"0": [{"0": [42]}]}},
    {_id: 26, "a": {"0": [{"0": {"0": 42}}]}},
    {_id: 27, "a": {"0": {"0": [[42]]}}},
    {_id: 28, "a": {"0": {"0": [{"0": 42}]}}},
    {_id: 29, "a": {"0": {"0": {"0": [42]}}}},
    {_id: 30, "a": {"0": {"0": {"0": {"0": 42}}}}},
]);

// Using equality with 2 level deep.
matchFunc(kDocs, {"a.0.0": 42}, [
    {_id: 3, "a": [[42]]},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 6, "a": {"0": {"0": 42}}},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 9, "a": [{"0": [42]}]},
    {_id: 10, "a": [{"0": {"0": 42}}]},
    {_id: 12, "a": {"0": [{"0": 42}]}},
    {_id: 13, "a": {"0": {"0": [42]}}},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 21, "a": [{"0": {"0": [42]}}]},
    {_id: 25, "a": {"0": [{"0": [42]}]}}
]);

// Using a comparison.
matchFunc(kDocs, {"a.0": {$gt: 41}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

// Using $in
matchFunc(kDocs, {"a.0": {$in: [41, 42, 43]}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

// using $nin
matchFunc(kDocs, {"a.0": {$nin: [41, 42, 43]}}, [
    {_id: 0, "a": 42},
    {_id: 3, "a": [[42]]},
    {_id: 6, "a": {"0": {"0": 42}}},
    {_id: 7, "a": [[[42]]]},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 10, "a": [{"0": {"0": 42}}]},
    {_id: 11, "a": {"0": [[42]]}},
    {_id: 12, "a": {"0": [{"0": 42}]}},
    {_id: 13, "a": {"0": {"0": [42]}}},
    {_id: 14, "a": {"0": {"0": {"0": 42}}}},
    {_id: 15, "a": [[[[42]]]]},
    {_id: 16, "a": [[[{"0": 42}]]]},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 18, "a": [[{"0": {"0": 42}}]]},
    {_id: 19, "a": [{"0": [[42]]}]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 21, "a": [{"0": {"0": [42]}}]},
    {_id: 22, "a": [{"0": {"0": {"0": 42}}}]},
    {_id: 23, "a": {"0": [[[42]]]}},
    {_id: 24, "a": {"0": [[{"0": 42}]]}},
    {_id: 25, "a": {"0": [{"0": [42]}]}},
    {_id: 26, "a": {"0": [{"0": {"0": 42}}]}},
    {_id: 27, "a": {"0": {"0": [[42]]}}},
    {_id: 28, "a": {"0": {"0": [{"0": 42}]}}},
    {_id: 29, "a": {"0": {"0": {"0": [42]}}}},
    {_id: 30, "a": {"0": {"0": {"0": {"0": 42}}}}},
]);

// Using $elemMatch.
matchFunc(kDocs, {a: {$elemMatch: {"0.0": {$eq: 42}}}}, [
    {_id: 7, "a": [[[42]]]},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 9, "a": [{"0": [42]}]},
    {_id: 10, "a": [{"0": {"0": 42}}]},
    {_id: 16, "a": [[[{"0": 42}]]]},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 21, "a": [{"0": {"0": [42]}}]},
]);

// Using top-level $and.
matchFunc(kDocs, {_id: {$lt: 15}, "a.0": {$gt: 41}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]},
]);

// $all with equality
matchFunc(kDocs, {"a.0": {$all: [42]}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

// $all with $elemMatch
matchFunc(kDocs, {"a.0": {$all: [{$elemMatch: {0: 42}}]}}, [
    {_id: 7, "a": [[[42]]]},
    {_id: 8, "a": [[{"0": 42}]]},
    {_id: 11, "a": {"0": [[42]]}},
    {_id: 12, "a": {"0": [{"0": 42}]}},
    {_id: 15, "a": [[[[42]]]]},
    {_id: 17, "a": [[{"0": [42]}]]},
    {_id: 19, "a": [{"0": [[42]]}]},
    {_id: 20, "a": [{"0": [{"0": 42}]}]},
    {_id: 23, "a": {"0": [[[42]]]}},
    {_id: 25, "a": {"0": [{"0": [42]}]}},
]);

// Using an expression.
matchFunc(kDocs, {"a.0": {$type: "number"}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

matchFunc(kDocs, {"a.0": {$mod: [42, 0]}}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

// using $or
matchFunc(kDocs, {$or: [{"a.0": 42}, {"notARealField": 123}]}, [
    {_id: 1, "a": [42]},
    {_id: 2, "a": {"0": 42}},
    {_id: 4, "a": [{"0": 42}]},
    {_id: 5, "a": {"0": [42]}},
    {_id: 9, "a": [{"0": [42]}]}
]);

const kRegexDocs =
    [{_id: 1, "b": "hello"}, {_id: 2, "b": {"0": "hello"}}, {_id: 3, "b": ["hello", "abc", "abc"]}];

matchFunc(kRegexDocs,
          {"b.0": {$regex: "hello"}},
          [{_id: 2, "b": {"0": "hello"}}, {_id: 3, "b": ["hello", "abc", "abc"]}]);
matchFunc(kRegexDocs,
          {"b.0": {$all: [/^hello/]}},
          [{_id: 2, "b": {"0": "hello"}}, {_id: 3, "b": ["hello", "abc", "abc"]}]);
matchFunc(kRegexDocs, {"b.0": {$not: /^h/}}, [{_id: 1, "b": "hello"}]);

const matchLargeDocFunc = function(docs, matchString, expectedResults) {
    runStreamProcessorOperatorTest({
        pipeline: [{$match: matchString}, {$project: {b: 1}}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
        }
    });
};

// we are able to run match stage on a document that has approximately 16MB
matchLargeDocFunc([generate16MBDoc()], {b: 0}, [{b: 0}]);

const matchVLargeDocFunc = function(docs, matchString, expectedResults) {
    runStreamProcessorOperatorTest({
        pipeline: [
            {
                $set: {
                    b0: "$a0",
                    b1: "$a1",
                    b2: "$a2",
                    b3: "$a3",
                    b4: "$a4",
                    b5: "$a5",
                    b6: "$a6",
                    b7: "$a7",
                    b8: "$a8",
                    b9: "$a9"
                }
            },
            {
                $set: {
                    c0: "$a0",
                    c1: "$a1",
                    c2: "$a2",
                    c3: "$a3",
                    c4: "$a4",
                    c5: "$b5",
                    c6: "$b6",
                    c7: "$b7",
                    c8: "$b8",
                    c9: "$b9"
                }
            },
            {
                $set: {
                    d0: "$a0",
                    d1: "$a1",
                    d2: "$a2",
                    d3: "$a3",
                    d4: "$a4",
                    d5: "$b5",
                    d6: "$b6",
                    d7: "$b7",
                    d8: "$b8",
                    d9: "$b9"
                }
            },
            {$match: matchString},
            {$project: {b: 1}}
        ],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
        }
    });
};

// attempt to double the 16MB document size before trimming
matchVLargeDocFunc([generate16MBDoc()], {b: 0}, [{b: 0}]);

// testing that match/project can process > 64MB documents
const matchVBLargeDocFunc = function testMatchVLargeDoc(docs, matchString, expectedResults) {
    runStreamProcessorOperatorTest({
        pipeline: [
            {
                $set: {
                    b0: "$a0",
                    b1: "$a1",
                    b2: "$a2",
                    b3: "$a3",
                    b4: "$a4",
                    b5: "$a5",
                    b6: "$a6",
                    b7: "$a7",
                    b8: "$a8",
                    b9: "$a9"
                }
            },
            {
                $set: {
                    c0: "$a0",
                    c1: "$a1",
                    c2: "$a2",
                    c3: "$a3",
                    c4: "$a4",
                    c5: "$b5",
                    c6: "$b6",
                    c7: "$b7",
                    c8: "$b8",
                    c9: "$b9"
                }
            },
            {
                $set: {
                    d0: "$a0",
                    d1: "$a1",
                    d2: "$a2",
                    d3: "$a3",
                    d4: "$a4",
                    d5: "$b5",
                    d6: "$b6",
                    d7: "$b7",
                    d8: "$b8",
                    d9: "$b9"
                }
            },
            {
                $set: {
                    e0: "$a0",
                    e1: "$a1",
                    e2: "$a2",
                    e3: "$a3",
                    e4: "$a4",
                    e5: "$a5",
                    e6: "$a6",
                    e7: "$a7",
                    e8: "$a8",
                    e9: "$a9"
                }
            },
            {
                $set: {
                    f0: "$a0",
                    f1: "$a1",
                    f2: "$a2",
                    f3: "$a3",
                    f4: "$a4",
                    f5: "$b5",
                    f6: "$b6",
                    f7: "$b7",
                    f8: "$b8",
                    f9: "$b9"
                }
            },
            {
                $set: {
                    g0: "$a0",
                    g1: "$a1",
                    g2: "$a2",
                    g3: "$a3",
                    g4: "$a4",
                    g5: "$b5",
                    g6: "$b6",
                    g7: "$b7",
                    g8: "$b8",
                    g9: "$b9"
                }
            },
            {$match: matchString},
            {$project: {b: 1}}
        ],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
        }
    });
};

matchVBLargeDocFunc([generate16MBDoc()], {b: 0}, [{b: 0}]);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);