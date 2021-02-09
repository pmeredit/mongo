/**
 * Test that $setWindowFields and various window functions are accessible by mqlrun.
 */
(function() {
"use strict";
const kInputBsonFile = "src/mongo/db/modules/enterprise/jstests/mqlrun/window_functions.bson";
load("src/mongo/db/modules/enterprise/jstests/mqlrun/mql_run_exec.js");

const pipeline = [
    {$sort: {_id: 1}},
    {
        $setWindowFields: {
            sortBy: {_id: 1},
            output: {a: {"$sum": {input: "$first", documents: ['unbounded', 'current']}}}
        }
    },
];

const result = mqlrunExec(kInputBsonFile, pipeline);

assert.eq(result, [
    "{ _id: 0.0, first: 0.0, second: 0.0, outer: { inner: 0.0 }, a: 0.0 }",
    "{ _id: 1.0, first: 2.0, second: 3.0, outer: { inner: 4.0 }, a: 2.0 }",
    "{ _id: 2.0, first: 4.0, second: 6.0, outer: { inner: 8.0 }, a: 6.0 }",
    "{ _id: 3.0, first: 6.0, second: 9.0, outer: { inner: 12.0 }, a: 12.0 }",
    "{ _id: 4.0, first: 8.0, second: 12.0, outer: { inner: 16.0 }, a: 20.0 }",
    "{ _id: 5.0, first: 10.0, second: 15.0, outer: { inner: 20.0 }, a: 30.0 }",
    "{ _id: 6.0, first: 12.0, second: 18.0, outer: { inner: 24.0 }, a: 42.0 }",
    "{ _id: 7.0, first: 14.0, second: 21.0, outer: { inner: 28.0 }, a: 56.0 }",
    "{ _id: 8.0, first: 16.0, second: 24.0, outer: { inner: 32.0 }, a: 72.0 }",
    "{ _id: 9.0, first: 18.0, second: 27.0, outer: { inner: 36.0 }, a: 90.0 }",
]);
})();
