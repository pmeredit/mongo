import {
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "projectOperatorTest";

const redactFunc = function(docs, redactString, expectedResults) {
    const pipeline = [{$project: redactString}];
    runStreamProcessorOperatorTest({
        pipeline: [{$redact: redactString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docs);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc));
            assert.eq(expectedResults, results);
        }
    });
};

const docs = [
    {
        _id: 1,
        level: 1,
        // b will present on level 3, 4, and 5
        b: {
            level: 3,
            c: 5,  // always included when b is included
            // the contents of d test that if we cannot see a document then we cannot see its
            // array-nested subdocument even if we have permissions to see the subdocument.
            // it also tests arrays containing documents we cannot see
            d: [
                {level: 1, e: 4},
                {f: 6},
                {level: 5, g: 9},
                "NOT AN OBJECT!!11!",  // always included when b is included
                [2, 3, 4, {level: 1, r: 11}, {level: 5, s: 99}]
                // nested array should always be included once b is
                // but the second object should only show up at level 5
            ]
        },
        // the contents of h test that in order to see a subdocument (j) we must be able to see all
        // parent documents (h and i) even if we have permissions to see the subdocument
        h: {level: 2, i: {level: 4, j: {level: 1, k: 8}}},
        // l checks that we get an empty document when we can see a document but none of its fields
        l: {m: {level: 3, n: 12}},
        // o checks that we get an empty array when we can see a array but none of its entries
        o: [{level: 5, p: 19}],
        // q is a basic field check and should always be included
        q: 14
    },
    {
        _id: 2,
        level: 4,
    }
];

let a1 = {$cond: [{$lte: ['$level', 1]}, "$$DESCEND", "$$PRUNE"]};
let a2 = {$cond: [{$lte: ['$level', 2]}, "$$DESCEND", "$$PRUNE"]};
let a3 = {$cond: [{$lte: ['$level', 3]}, "$$DESCEND", "$$PRUNE"]};
let a4 = {$cond: [{$lte: ['$level', 4]}, "$$DESCEND", "$$PRUNE"]};
let a5 = {$cond: [{$lte: ['$level', 5]}, "$$DESCEND", "$$PRUNE"]};

let a1result = [{_id: 1, l: {}, level: 1, o: [], q: 14}];

let a2result = [{
    _id: 1,
    h: {
        level: 2,
    },
    l: {},
    level: 1,
    o: [],
    q: 14
}];

let a3result = [{
    _id: 1,
    b: {
        level: 3,
        c: 5,
        d: [{level: 1, e: 4}, {f: 6}, "NOT AN OBJECT!!11!", [2, 3, 4, {level: 1, r: 11}]]
    },
    h: {
        level: 2,
    },
    l: {m: {level: 3, n: 12}},
    level: 1,
    o: [],
    q: 14
}];

let a4result = [
    {
        _id: 1,
        b: {
            level: 3,
            c: 5,
            d: [{level: 1, e: 4}, {f: 6}, "NOT AN OBJECT!!11!", [2, 3, 4, {level: 1, r: 11}]]
        },
        h: {level: 2, i: {level: 4, j: {level: 1, k: 8}}},
        l: {m: {level: 3, n: 12}},
        level: 1,
        o: [],
        q: 14
    },
    {
        _id: 2,
        level: 4,
    }
];

let a5result = [
    {
        _id: 1,
        b: {
            level: 3,
            c: 5,
            d: [
                {level: 1, e: 4},
                {f: 6},
                {level: 5, g: 9},
                "NOT AN OBJECT!!11!",
                [2, 3, 4, {level: 1, r: 11}, {level: 5, s: 99}]
            ]
        },
        h: {level: 2, i: {level: 4, j: {level: 1, k: 8}}},
        l: {m: {level: 3, n: 12}},
        level: 1,
        o: [{level: 5, p: 19}],
        q: 14
    },
    {
        _id: 2,
        level: 4,
    }
];

redactFunc(docs, a1, a1result);
redactFunc(docs, a2, a2result);
redactFunc(docs, a3, a3result);
redactFunc(docs, a4, a4result);
redactFunc(docs, a5, a5result);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);