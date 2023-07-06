// Tests that we can use 'mqlrun' to execute a few simple queries directly against a BSON file.
//
// These tests use ny_phone_sales.bson as input. This data was derived from the
// "sample_supplies.sales" collection that we offer as sample Atlas data. In order to make the data
// size small enough to check in, this file contains only the documents with "storeLocation" equal
// to "New York" and purchaseMethod equal to "Phone".
const kInputBsonFile = "src/mongo/db/modules/enterprise/jstests/mqlrun/ny_phone_sales.bson";
import {mqlrunExec} from "src/mongo/db/modules/enterprise/jstests/mqlrun/mql_run_exec.js";

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