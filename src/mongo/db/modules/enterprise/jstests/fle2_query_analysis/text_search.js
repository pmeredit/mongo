/**
 * Test that mongocryptd can correctly mark the $encStrStartsWith aggregation expression with
 * intent-to-encrypt placeholders.
 * * @tags: [
 * featureFlagQETextSearchPreview,
 * ]
 */
import {
    PrefixField,
    SubstringField,
    SuffixAndPrefixField,
    SuffixField
} from "jstests/fle2/libs/qe_text_search_util.js";
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.text_search;

// Create the encrypted collection schema
const firstNameField = new SubstringField(1000, 10, 100, false, false, 1);
const lastNameField = new PrefixField(2, 48, false, true, 1);
const locationField = new SuffixField(2, 48, true, false, 1);
const ssnField = new SuffixAndPrefixField(3, 30, 2, 20, true, true, 1);

const schema = {
    encryptionInformation: {
        type: 1,
        schema: {
            "test.text_search": {
                "fields": [
                    {
                        path: "firstName",
                        bsonType: "string",
                        queries: firstNameField.createQueryTypeDescriptor(),
                        keyId: UUID()
                    },
                    {
                        path: "lastName",
                        bsonType: "string",
                        queries: lastNameField.createQueryTypeDescriptor(),
                        keyId: UUID()

                    },
                    {
                        path: "location",
                        bsonType: "string",
                        queries: locationField.createQueryTypeDescriptor(),
                        keyId: UUID()

                    },
                    {
                        path: "ssn",
                        bsonType: "string",
                        queries: ssnField.createQueryTypeDescriptor(),
                        keyId: UUID()
                    },
                    {
                        path: "age",
                        bsonType: "int",
                        queries: {
                            queryType: "range",
                            sparsity: 1,
                            min: NumberInt(0),
                            max: NumberInt(200),
                        },
                        keyId: UUID()
                    },
                    {
                        path: "rating",
                        bsonType: "int",
                        queries: {
                            queryType: "equality",
                        },
                        keyId: UUID()
                    },
                ]
            }
        }
    }
};

/**
 * queryCommandsTestFuncs: This map provides a mapping for each supported command name, to its
 * corresponding test methods for generating the command object and for getting the result query
 * body from the command response object.
 *
 * Each command may specify the query document under a different path of the command object.
 * getResultQueryBody will return the element corresponding to the query object in the command
 * object, which can be used in the custom test function.
 */
const queryCommandsTestFuncs = {
    "aggregate": {
        generateCommand: function(collName, pipeline, schemas) {
            return Object.assign({aggregate: collName, pipeline: pipeline, cursor: {}}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.pipeline;
        }
    },
    "find": {
        generateCommand: function(collName, query, schemas) {
            return Object.assign({find: collName, filter: query}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.filter;
        }
    },
    "count": {
        generateCommand: function(collName, query, schemas) {
            return Object.assign({count: collName, query: query}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.query;
        }
    },
    "bulkWrite": {
        generateCommand: function(collName, filter, schemas) {
            let nsInfo = Object.assign({ns: `test.${collName}`}, schemas);
            return Object.assign({
                bulkWrite: 1,
                ops: [{update: 0, filter: filter, updateMods: {$set: {foo: "baz", bar: "foo"}}}],
                nsInfo: [nsInfo]
            });
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.ops[0].filter;
        }
    },
    "update": {
        generateCommand: function(collName, query, schemas) {
            return Object.assign(
                {update: collName, updates: [{q: query, u: {$set: {foo: "updated"}}}]}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.updates[0].q;
        }
    },
    "delete": {
        generateCommand: function(collName, query, schemas) {
            return Object.assign({delete: collName, deletes: [{q: query, limit: 1}]}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.deletes[0].q;
        }
    },
    "findAndModify": {
        generateCommand: function(collName, query, schemas) {
            return Object.assign(
                {findAndModify: collName, query: query, update: {$set: {foo: "updated"}}}, schemas);
        },
        getResultQueryBody: function(cmdRes) {
            return cmdRes.result.query;
        }
    }
};

function runTest(test, index) {
    // Validate the test contents.
    const expectedResults = function() {
        assert(test.schemas != undefined && typeof test.schemas === "object", "Expected schemas.");

        let getValidatedExpectedResults = function(expectedRes) {
            const isExpectedFail = expectedRes.failCodes != undefined &&
                Array.isArray(expectedRes.failCodes) && expectedRes.failCodes.length;

            assert(
                isExpectedFail ||
                    (expectedRes.hasEncryptionPlaceholders != undefined &&
                     typeof expectedRes.hasEncryptionPlaceholders === "boolean" &&
                     expectedRes.schemaRequiresEncryption != undefined &&
                     typeof expectedRes.schemaRequiresEncryption === "boolean"),
                "Expected failCodes list, or hasEncryptionPlaceholders (bool) and schemaRequiresEncryption (bool)");

            if (isExpectedFail) {
                return {expectedFail: true, failCodes: expectedRes.failCodes};
            }
            return {
                expectedFail: false,
                hasEncryptionPlaceholders: expectedRes.hasEncryptionPlaceholders,
                schemaRequiresEncryption: expectedRes.schemaRequiresEncryption
            };
        };
        return getValidatedExpectedResults(test);
    }();

    jsTestLog("Running test index " + index);
    if (test.customTestSetup) {
        test.customTestSetup();
    }

    let performTest = function(db, cmdObj, customTestFunc, getResultBody) {
        if (!expectedResults.expectedFail) {
            let cmdRes = assert.commandWorked(db.runCommand(cmdObj));
            assert.eq(expectedResults.hasEncryptionPlaceholders,
                      cmdRes.hasEncryptionPlaceholders,
                      cmdRes);
            assert.eq(
                expectedResults.schemaRequiresEncryption, cmdRes.schemaRequiresEncryption, cmdRes);

            if (customTestFunc) {
                customTestFunc(cmdRes, getResultBody);
            }
        } else {
            assert.commandFailedWithCode(db.runCommand(cmdObj), expectedResults.failCodes);
        }
    };

    // We have an aggregate command.
    if (test.pipeline != undefined) {
        assert(test.pipeline instanceof Array, "Pipeline should be an array.");
        let command = queryCommandsTestFuncs["aggregate"].generateCommand(
            test.collectionName, test.pipeline, test.schemas);
        jsTestLog("Testing aggregate command!");
        performTest(testDB,
                    command,
                    test.customTestFunc,
                    queryCommandsTestFuncs["aggregate"].getResultQueryBody);
    } else {
        // If we didn't have an agg command test, it must be a query (i.e match expression)
        // test. Iterate over all supported commands which use the query syntax.
        assert(test.query != undefined && test.query instanceof Object,
               "Expected a query filter test.");
        for (const [commandName, funcs] of Object.entries(queryCommandsTestFuncs)) {
            if (commandName == "aggregate") {
                continue;
            }
            jsTestLog(`Testing query filter in ${commandName} command!`);
            let command = funcs.generateCommand(test.collectionName, test.query, test.schemas);
            // bulkWrite must be run against adminDB.
            let db = commandName == "bulkWrite" ? conn.getDB("admin") : testDB;
            performTest(db, command, test.customTestFunc, funcs.getResultQueryBody);
        }
    }
    if (test.customTestTeardown) {
        test.customTestTeardown();
    }
}

/**
 * A test must specify either a pipeline (i.e aggregation test) or a query (tests find, count,
 * update, etc.).
 *
 * A test must either be:
 * - An expected failure -> failCodes is specified
 * - Expected success -> hasEncryptionPlaceholders and schemaRequiresEncryption must be specified.
 *
 * Additional testing of the result payload can be achieved by providing a customTestFunc.
 *
 * Test case format:
 * {
 *  collectionName: string (required),
 *  pipeline: [] (optional),
 *  query: {} (optional),
 *  customTestFunc: function (optional),
 *  schemas: {} (required),
 *  failCodes: [] (optional),
 *  hasEncryptionPlaceholders: boolean (optional),
 *  schemaRequiresEncryption: boolean (optional),
 *  customTestSetup: function (optional),
 *  customTestTeardown: function (optional)
 *  }
 */
const testList = [
    {
        collectionName: coll.getName(),
        pipeline: [{$match: {$expr: {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}}}],
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, pipelineFetcher) {
            let pipeline = pipelineFetcher(cmdRes);
            assert(pipeline[0].$match.$expr["$encStrStartsWith"].prefix.$const instanceof BinData,
                   cmdRes);
        }
    },  // 0) Basic $encStrStartsWith test, constant literal replaced with placeholder (agg).
    {
        collectionName: coll.getName(),
        query: {"$expr": {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}},
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, resultFetcher) {
            let query = resultFetcher(cmdRes);
            assert(query["$expr"]["$encStrStartsWith"].prefix.$const instanceof BinData, cmdRes);
        }
    },  // 1) Basic $encStrStartsWith test, constant literal replaced with placeholder (query).
    {
        collectionName: coll.getName(),
        pipeline: [{
            $match: {
                $expr: {
                    $and: [
                        {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}},
                        {$eq: ["$rating", NumberInt(5)]},
                        {$gt: ["$age", NumberInt(3)]},
                    ]
                }
            }
        }],
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, pipelineFetcher) {
            let pipeline = pipelineFetcher(cmdRes);
            assert(pipeline[0].$match.$expr.$and[0]["$encStrStartsWith"].prefix.$const instanceof
                       BinData,
                   cmdRes);
            assert(pipeline[0].$match.$expr.$and[1]["$eq"][1].$const instanceof BinData, cmdRes);
            assert(pipeline[0].$match.$expr.$and[2].$and[0].$gt[1].$const instanceof BinData,
                   cmdRes);
            assert(pipeline[0].$match.$expr.$and[2].$and[1].$lte[1].$const instanceof BinData,
                   cmdRes);
        }
    },  // 2) $encStrStartsWith in agg command with range and equality (top level $expr).
    {
        collectionName: coll.getName(),
        query: {
            $and: [
                {$expr: {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}},
                {$expr: {$eq: ["$rating", NumberInt(5)]}},
                {$expr: {$gt: ["$age", NumberInt(3)]}},
            ]
        },
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, resultFetcher) {
            let query = resultFetcher(cmdRes);
            assert(query.$and[0].$expr["$encStrStartsWith"].prefix.$const instanceof BinData,
                   cmdRes);
            assert(query.$and[1].$expr["$eq"][1].$const instanceof BinData, cmdRes);
            assert(query.$and[2].$expr.$and[0].$gt[1].$const instanceof BinData, cmdRes);
            assert(query.$and[2].$expr.$and[1].$lte[1].$const instanceof BinData, cmdRes);
        }
    },  // 3) $encStrStartsWith in query command with range and equality (all nested $expr).
    {
        collectionName: coll.getName(),
        query: {
            $and: [
                {$expr: {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}},
                {rating: {"$eq": NumberInt(5)}},
                {age: {"$gt": NumberInt(3)}},
            ]
        },
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, resultFetcher) {
            let query = resultFetcher(cmdRes);
            assert(query.$and[0].$expr["$encStrStartsWith"].prefix.$const instanceof BinData,
                   cmdRes);
            assert(query.$and[1].rating.$eq instanceof BinData, cmdRes);
            assert(query.$and[2].age.$gt instanceof BinData, cmdRes);
        }
    },  // 4) $encStrStartsWith in query command with range and equality (single $expr with match
        // expressions).
    {
        collectionName: coll.getName(),
        query: {
            $and: [
                {$expr: {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}},
                {$expr: {$in: ["$rating", [NumberInt(1), NumberInt(2), NumberInt(3)]]}},
                {$expr: {$gt: ["$age", NumberInt(3)]}},
            ]
        },
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, resultFetcher) {
            let query = resultFetcher(cmdRes);
            assert(query.$and[0].$expr["$encStrStartsWith"].prefix.$const instanceof BinData,
                   cmdRes);
            assert(query.$and[1].$expr["$in"][1][0].$const instanceof BinData, cmdRes);
            assert(query.$and[1].$expr["$in"][1][1].$const instanceof BinData, cmdRes);
            assert(query.$and[1].$expr["$in"][1][2].$const instanceof BinData, cmdRes);
            assert(query.$and[2].$expr.$and[0]["$gt"][1].$const instanceof BinData, cmdRes);
            assert(query.$and[2].$expr.$and[1]["$lte"][1].$const instanceof BinData, cmdRes);
        }
    },  // 5) $encStrStartsWith in query command with range and $in (all nested $expr).
    {
        collectionName: coll.getName(),
        query: {
            $and: [
                {$expr: {$encStrStartsWith: {input: "$lastName", prefix: "PREFIX"}}},
                {rating: {"$in": [NumberInt(1), NumberInt(2), NumberInt(3)]}},
                {age: {$gt: NumberInt(3)}},
            ]
        },
        schemas: schema,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        customTestFunc: function(cmdRes, resultFetcher) {
            let query = resultFetcher(cmdRes);
            assert(query.$and[0].$expr["$encStrStartsWith"].prefix.$const instanceof BinData,
                   cmdRes);
            assert(query.$and[1].rating["$in"][0] instanceof BinData, cmdRes);
            assert(query.$and[1].rating["$in"][1] instanceof BinData, cmdRes);
            assert(query.$and[1].rating["$in"][2] instanceof BinData, cmdRes);
            assert(query.$and[2].age["$gt"] instanceof BinData, cmdRes);
        }
    },  // 6) $encStrStartsWith in query command with range and $in (single $expr with match
        // expressions).
    {
        collectionName: coll.getName(),
        pipeline: [{$match: {$expr: {$encStrStartsWith: {input: "$foo", prefix: "PREFIX"}}}}],
        schemas: schema,
        failCodes: [10112204]
    },  // 7) Test $encStrStartsWith on unencrypted field fails (agg).
    {
        collectionName: coll.getName(),
        pipeline: [{$match: {$expr: {$encStrStartsWith: {input: "$location", prefix: "PREFIX"}}}}],
        schemas: schema,
        failCodes: [10248500]
    },  // 8) Test $encStrStartsWith on unsupported encrypted text field fails (agg).
    {
        collectionName: coll.getName(),
        query: {
            $and: [
                {$expr: {$encStrStartsWith: {input: "$location", prefix: "PREFIX"}}},
                {rating: {"$in": [NumberInt(1), NumberInt(2), NumberInt(3)]}},
                {age: {$gt: NumberInt(3)}},
            ]
        },
        schemas: schema,
        failCodes: [10248500]
    }  // 9) Test $encStrStartsWith on unsupported encrypted text field fails (find).
];
testList.forEach(runTest);

mongocryptd.stop();
