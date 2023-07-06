/**
 * Tests whether mqlrun does not spill to disk by default, and spills to disk if -t option is
 * specified.
 */
import {mqlrunExec} from "src/mongo/db/modules/enterprise/jstests/mqlrun/mql_run_exec.js";

// The file contains a document {largeField: "AAA..A" (128 characters), a: [0, 1, .., 9], b: [0, 1,
// .., 9], c: [0, 1, .., 9], d: [0, 1, .., 9], e: [0, 1, .., 9], f: [0, 1, .., 9]}.
const kInputBsonFile = "src/mongo/db/modules/enterprise/jstests/mqlrun/seed_document.bson";

// Build a pipeline that replicates the seed document by leveraging the "$unwind" stage so the size
// of buffered documents by the "$sort" stage is larger than 100MB (thus triggering spilling to
// disk).
const pipeline = [
    {$unwind: {path: "$a"}},
    {$unwind: {path: "$b"}},
    {$unwind: {path: "$c"}},
    {$unwind: {path: "$d"}},
    {$unwind: {path: "$e"}},
    {$unwind: {path: "$f"}},
    {$sort: {f: 1}},
    // Minimize mqlrun output size.
    {$skip: 999999},
    {$project: {f: 1, _id: 0}}
];

// Verify that when option -t is specified the mqlrun succeeds returning the result which
// computation requires spilling.
let mqlrunOutput = mqlrunExec(kInputBsonFile, pipeline);
assert.eq(mqlrunOutput, ["{ f: 9 }"]);

// Verify that when option -t is not specified the mqlrun fails a computation that requires
// spilling.
mqlrunOutput =
    mqlrunExec(kInputBsonFile, pipeline, {allowSpillToDisk: false, expectedReturnCode: 1});
assert.eq(1, mqlrunOutput.length, "Expected at least one line of output");
assert(mqlrunOutput[0].indexOf("exceeded memory limit") !== -1);