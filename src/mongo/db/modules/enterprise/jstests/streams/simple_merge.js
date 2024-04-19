import {sequentialIds} from "jstests/query_golden/libs/example_data.js";
import {
    getIdProjectionDocs,
    getProjectionDocs
} from "jstests/query_golden/libs/projection_helpers.js";
import {
    dbName,
    insertDocs,
    listStreamProcessors,
    logState,
    runStreamProcessorOperatorTest,
    sanitizeDoc,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const outColl = db.getSiblingDB(dbName).outColl;
const spName = "simpleMergeOperatorTest";

const simpleMergeFunc = function(docs, whenMatchedOption) {
    const docsWithIds = sequentialIds(docs);
    runStreamProcessorOperatorTest({
        pipeline: [],
        spName: spName,
        verifyAction: () => {
            insertDocs(spName, docsWithIds);
            assert.soon(() => { return outColl.find().itcount() >= docsWithIds.length; },
                        logState());
            var fieldNames = ['_ts', '_stream_meta'];
            let results = outColl.find().toArray().map((doc) => sanitizeDoc(doc, fieldNames));
            assert.eq(results, docsWithIds);
        },
        whenMatchedOption: whenMatchedOption ? whenMatchedOption : undefined
    });
};

simpleMergeFunc([
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
    {a: {b: {c: "str", d: "str", e: "str"}, f: "str"}, x: "str"}
]);

/* TODO: STREAMS-729 needs whenMatched: "replace"
 * fails because merge does not seem to allow documents with fieldnames containing .
 */
simpleMergeFunc(
    [
        {"a.b.c": 1},
        {a: "str", abc: "str", "a.b": "str"},
        {a: {b: 1}, "a.b": "str"},
        {a: {bNot: {c: 1}}}
    ],
    "replace");

simpleMergeFunc([
    {a: [{b: {c: 2, d: 3}, e: 4}, {b: {c: 5, d: 6}, e: 7}]},
    {a: {b: [{c: 1, d: 1}, {c: 2, d: 2}]}},
    {a: {b: {c: [1, 2, {d: 3}]}}},
    {a: ["str", {b: 1}, {c: 1}, {b: 1, c: 1, d: 1}], x: "str"},

    // Two arrays along the "a.b.c" path.
    {a: [{b: [{c: 1, d: 1}, {c: 2, d: 2}]}, {b: [{c: 3, d: 3}, {c: 4, d: 4}]}]},
    {a: [{b: {c: [1, {d: 1}]}}, {b: {c: []}}]},
    {a: {b: [{c: [1, {d: 1}]}, {c: []}]}}
]);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);