import {sequentialIds} from "jstests/query_golden/libs/example_data.js";
import {
    getIdProjectionDocs,
    getProjectionDocs
} from "jstests/query_golden/libs/projection_helpers.js";
import {
    dbName,
    insertDocs,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "projectOperatorTest";
const coll = db.project_coll;

const projectFunc = function testProjectNumbers(docs, matchString, stripIds = false) {
    const docsWithIds = sequentialIds(docs);
    coll.drop();
    coll.insert(docsWithIds);
    const pipeline = [{$project: matchString}];
    const expectedResults = coll.aggregate(pipeline)._batch.reverse();
    jsTestLog(expectedResults);
    runStreamProcessorOperatorTest({
        pipeline: [{$project: matchString}],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docsWithIds);
            assert.soon(() => { return outColl.find().itcount() == expectedResults.length; },
                        logState());
            var fieldNames = ['_ts', '_stream_meta'];
            if (stripIds) {
                fieldNames.push('_id');
            }
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc, fieldNames));
            assert.eq(expectedResults, results);
        }
    });
};

// tests from jstests/query_golden/inclusion_projection.js
projectFunc(getProjectionDocs(), {a: 1});
projectFunc(getProjectionDocs(), {a: 1, _id: 1});
projectFunc(getProjectionDocs(), {a: 1, _id: 0}, true);
projectFunc(getProjectionDocs(), {a: 1, x: 1});
projectFunc(getProjectionDocs(), {"a.b": 1});
projectFunc(getProjectionDocs(), {"a.b": 1, _id: 0}, true);
projectFunc(getProjectionDocs(), {"a.b": 1, "a.c": 1});
projectFunc(getProjectionDocs(), {"a.b.c": 1});
projectFunc(getProjectionDocs(), {"a.b.c": 1, _id: 0}, true);
projectFunc(getProjectionDocs(), {"a.b.c": 1, "a.b.d": 1});
projectFunc(getProjectionDocs(), {a: {b: 1}});
projectFunc(getProjectionDocs(), {a: {"b.c": {d: 1, e: 1}}});
projectFunc(getIdProjectionDocs(), {_id: 1});

/*
Selecting part of _id will leave duplicate entries, need to find a better strategy.
projectFunc(getIdProjectionDocs(), {"_id.a": 1});
projectFunc(getIdProjectionDocs(), {"_id.a": 1, "_id.b": 1});
projectFunc(getIdProjectionDocs(), {"_id.a.b": 1});
*/
/* These tests break if the document has fields like "a.b.c"
TODO: STREAMS-729
const exclusionProjSpecs = [
    {a: 0},
    {a: 0, _id: 0},
    {a: 0, x: 0},

    {"a.b": 0},
    {"a.b": 0, "a.c": 0},

    {"a.b.c": 0},
    {"a.b.c": 0, "a.b.d": 0},

    // This syntax is permitted and equivalent to the dotted notation.
    {a: {b: 0}},
    // A mix of dotted syntax and nested syntax is permitted as well.
    {a: {"b.c": {d: 0, e: 0}}},
];
*/

const newDocList = [
    //
    // Simple documents without any arrays along "a.b.c".
    //

    // "a" is missing/null.
    {},
    {a: null},
    {a: undefined},
    {x: "str", y: "str"},

    // "a.b" is missing/null.
    {a: "str", b: "str", x: "str", y: "str"},
    {a: {}},
    {a: {b: null, c: 1}},
    {a: {b: undefined, c: 1}},
    {a: {c: 1, d: 1}},
    {a: {d: 1}},
    {a: {c: {b: 1}}},

    // "a.b.c" is missing/null
    {a: {b: 1, c: 1, d: 1}, x: {y: 1, z: 1}},
    {a: {b: 1, _id: 1}},
    {a: {b: {}}},
    {a: {b: {c: null}}},
    {a: {b: {c: undefined}}},
    {a: {b: {x: 1, y: 1}}},

    // All path components along "a.b.c" exist.
    {a: {b: {c: "str", d: "str", e: "str"}, f: "str"}, x: "str"},

    //
    // Documents with arrays on the "a.b.c" path.
    //

    // "a.b" is missing/null
    {a: [], x: "str"},
    {a: ["str"]},
    {a: [null]},
    {a: [null, null]},
    {a: [null, "str"]},
    {a: [[], []]},
    {a: [[1, 2]]},
    {a: [{}, {}]},
    {a: [{x: "str"}]},
    {a: [{c: "str"}]},
    {a: [{b: null, c: 1}, {c: 1}, {d: 1}, "str"]},

    // "a.b.c" is missing/null
    {a: {b: [], x: "str"}},
    {a: {b: ["str"]}},
    {a: {b: [[]]}},
    {a: {b: [{}]}},
    {a: {b: [{c: null}]}},
    {a: [{b: {x: 1}}]},
    {a: [{b: [{}]}]},
    {a: [[], [[], [], [1], [{c: 1}]], {b: 1}]},

    // Fields with similar names to the projected fields.
    {
        a: [
            // No "a.b".
            {bNot: [{c: "str"}, {c: "str"}]},
            // No "a.b.c".
            {b: [{cNot: "str", d: 1}, {cNot: "str", d: 2}]},
            // Only some "a.b.c"s.
            {b: [{c: 3, d: 3}, {cNot: "str", d: 4}, {c: 5}]},
        ]
    },

    //
    // All path components along "a.b.c" exist.
    //

    // Exactly one array along the "a.b.c" path.
    {a: [{b: {c: 2, d: 3}, e: 4}, {b: {c: 5, d: 6}, e: 7}]},
    {a: {b: [{c: 1, d: 1}, {c: 2, d: 2}]}},
    {a: {b: {c: [1, 2, {d: 3}]}}},
    {a: ["str", {b: 1}, {c: 1}, {b: 1, c: 1, d: 1}], x: "str"},

    // Two arrays along the "a.b.c" path.
    {a: [{b: [{c: 1, d: 1}, {c: 2, d: 2}]}, {b: [{c: 3, d: 3}, {c: 4, d: 4}]}]},
    {a: [{b: {c: [1, {d: 1}]}}, {b: {c: []}}]},
    {a: {b: [{c: [1, {d: 1}]}, {c: []}]}},

    // "a", "a.b", and "a.b.c" are arrays.
    {
        a: [
            {b: [{c: [1, 2, 3], d: 1}, {c: [2, 3, 4], d: 2}]},
            {b: [{c: [3, 4, 5], d: 3}, {c: [4, 5, 6], d: 4}]}
        ]
    },

    // Multiple nested arrays encountered between field path components.
    {a: [[1, {b: 1}, {b: 2, c: 2}, "str"]]},
    {a: [[[{b: [[[{c: [[["str"]]], d: "str"}]]]}]]]},
    {
        a: [
            ["str", {b: 1}, {b: 2, c: 2}, "str"],
            [[{b: 1}]],
            [{b: 1}, [{b: 2}], [[{b: [2]}]]],
        ]
    },

];

/// modifed tests from  jstests/query_golden/exclusion_projection.js
const exclusionProjSpecs = [
    {a: 0},
    //   {a: 0, _id: 0},
    {a: 0, x: 0},

    {"a.b": 0},
    {"a.b": 0, "a.c": 0},

    {"a.b.c": 0},
    {"a.b.c": 0, "a.b.d": 0},

    // This syntax is permitted and equivalent to the dotted notation.
    {a: {b: 0}},
    // A mix of dotted syntax and nested syntax is permitted as well.
    {a: {"b.c": {d: 0, e: 0}}},
];

for (const projectionSpec of exclusionProjSpecs) {
    projectFunc(newDocList, projectionSpec);
}
