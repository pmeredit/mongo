// Tests that we can use 'mqlrun' to execute a few simple queries directly against a BSON file.
//
// These tests use ny_phone_sales.bson as input. This data was derived from the
// "sample_supplies.sales" collection that we offer as sample Atlas data. In order to make the data
// size small enough to check in, this file contains only the documents with "storeLocation" equal
// to "New York" and purchaseMethod equal to "Phone".
(function() {
"use strict";

const kInputBsonFile = "src/mongo/db/modules/enterprise/jstests/mqlrun/ny_phone_sales.bson";

// Given 'inputFileName', a path to a BSON file, and the array 'pipeline', executes the pipeline
// against the BSON file using mqlrun and returns the result set.
//
// If 'outputBson' is false (the default), then the results are returned as an array of relaxed JSON
// strings, one per document in the result set. These strings are relaxed in that they do not have
// quotes around property names, and may use strings like "ObjectId('5bd761dcae323e45a93ccfe8') or
// "new Date(1427144809506)" to represent special BSON types. As a future improvement (see
// SERVER-45247), we could change mqlrun to produce output as strict extended JSON strings.
//
// If 'outputBson' is true, instructs mqlrun to return the result set as BSON. Asserts that mqlrun
// returns with a successful exit code, and returns null on success. As a future improvement, we
// could improve the test machinery to be able to process BSON-formatted output produced by mqlrun.
function mqlrunExec(inputFileName, pipeline, {outputBson = false} = {}) {
    clearRawMongoProgramOutput();

    // Convert 'pipeline' to a JSON string so that it can be passed to mqlrun as command line
    // argument. On Windows, the shell isn't happy with double quotes, so we replace them with
    // single quotes.
    const stringifiedPipeline = JSON.stringify(pipeline).replace(/"/g, "'");

    const mqlrunExecutableName = _isWindows() ? "mqlrun.exe" : "mqlrun";
    const outputFormatFlag = outputBson ? "-b" : "-j";
    assert.eq(0,
              runMongoProgram(
                  mqlrunExecutableName, outputFormatFlag, "-e", stringifiedPipeline, inputFileName),
              "mqlrun exited with non-ok status");

    if (outputBson) {
        return null;
    }

    const rawOutput = rawMongoProgramOutput();

    // Split the raw output into an array containing the output of each line. We expect the output
    // to end in a newline, so we need to remove the final array element after splitting on
    // newlines.
    const outputByLine = rawOutput.split("\n");
    assert.eq("", outputByLine.pop(), rawOutput);

    // Each line has a prefix like "sh2311702|" which needs to be stripped.
    return outputByLine.map(function(textOfLine) {
        return textOfLine.substr(textOfLine.indexOf("|") + 1).trim();
    });
}

// Test that mqlrun correctly executes a query to find the 5 most common tags, by quantity of items
// sold.
let pipeline = [
    {$unwind: "$items"},
    {$unwind: "$items.tags"},
    {$group: {_id: "$items.tags", totalQuantity: {$sum: "$items.quantity"}}},
    {$sort: {totalQuantity: -1}},
    {$limit: 5}
];
let output = mqlrunExec(kInputBsonFile, pipeline);
assert.eq(output, [
    "{ _id: \"office\", totalQuantity: 895 }",
    "{ _id: \"school\", totalQuantity: 766 }",
    "{ _id: \"stationary\", totalQuantity: 619 }",
    "{ _id: \"general\", totalQuantity: 535 }",
    "{ _id: \"writing\", totalQuantity: 369 }"
]);

// Verify that mqlrun succeeds when running the same query and returning BSON.
mqlrunExec(kInputBsonFile, pipeline, {outputBson: true});

// Find customer information associated with sales which totalled >= $5000. But exclude email
// address so that PII is omitted.
pipeline = [
    {$addFields: {
        totalCost: {
            $reduce: {
                input: "$items",
                initialValue: 0,
                in: {$add: ["$$value", {$multiply: ["$$this.quantity", "$$this.price"]}]}
            }
        }
    }},
    {$match: {totalCost: {$gte: 5000}}},
    {$sort: {totalCost: -1}},
    {$project: {_id: 0, totalCost: 1, customer: 1}},
    {$project: {"customer.email": 0}},
];

output = mqlrunExec(kInputBsonFile, pipeline);
assert.eq(output, [
    "{ customer: { gender: \"M\", age: 42, satisfaction: 5 }, totalCost: 8238.67 }",
    "{ customer: { gender: \"M\", age: 37, satisfaction: 2 }, totalCost: 7893.00 }",
    "{ customer: { gender: \"M\", age: 56, satisfaction: 5 }, totalCost: 7823.10 }",
    "{ customer: { gender: \"M\", age: 24, satisfaction: 4 }, totalCost: 7295.61 }",
    "{ customer: { gender: \"M\", age: 46, satisfaction: 5 }, totalCost: 7068.92 }",
    "{ customer: { gender: \"M\", age: 39, satisfaction: 5 }, totalCost: 5904.47 }",
    "{ customer: { gender: \"F\", age: 37, satisfaction: 3 }, totalCost: 5227.63 }"
]);

// Verify that mqlrun succeeds when running the same query and returning BSON.
mqlrunExec(kInputBsonFile, pipeline, {outputBson: true});
}());
