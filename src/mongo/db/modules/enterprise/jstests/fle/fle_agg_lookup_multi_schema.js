/**
 * Test that mongocryptd can correctly mark the $lookup agg stage with intent-to-encrypt
 * placeholders.
 * * @tags: [
 *   requires_fcv_81
 * ]
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchemasFromSchemaMap
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_lookup_multi_schema;
const foreignColl = testDB.fle_agg_lookup_multi_schema_foreign;

const encryptedStringSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}});

// Create encryption schema map with two collections, both encrypted.
let twoEncryptedCollectionsSchemaMap = {};
twoEncryptedCollectionsSchemaMap[coll.getFullName()] = {
    enc_item: encryptedStringSpec(),
    enc_sku: encryptedStringSpec()
};
twoEncryptedCollectionsSchemaMap[foreignColl.getFullName()] = {
    r_enc_item: encryptedStringSpec(),
    r_enc_sku: encryptedStringSpec()
};

const twoEncryptedCollEncryptionSchemas =
    generateSchemasFromSchemaMap(twoEncryptedCollectionsSchemaMap);

// Create encryption schema map with two collections, both with encrypted children below an
// unencrypted parent.
let nestedEncryptedChildSchemaMap = {};
nestedEncryptedChildSchemaMap[coll.getFullName()] = {
    "parent.enc_item": encryptedStringSpec(),
    "parent.enc_sku": encryptedStringSpec()
};
nestedEncryptedChildSchemaMap[foreignColl.getFullName()] = {
    "r_parent.r_enc_item": encryptedStringSpec(),
    "r_parent.r_enc_sku": encryptedStringSpec()
};
const nestedEncryptedChildEncryptionSchemas =
    generateSchemasFromSchemaMap(nestedEncryptedChildSchemaMap);

// Create encryption schema map with two collections, one encrypted and one unencrypted.
let encryptedUnencryptedSchemaMap = {};
encryptedUnencryptedSchemaMap[coll.getFullName()] = {
    enc_item: encryptedStringSpec(),
    enc_sku: encryptedStringSpec()
};
encryptedUnencryptedSchemaMap[foreignColl.getFullName()] = {};
const encryptedAndUnencryptedEncryptionSchemas =
    generateSchemasFromSchemaMap(encryptedUnencryptedSchemaMap);

// Create another collection, to be used as the nested foreign collection for our nested lookup
// tests. Create an encryption schema map with three encrypted collections.
const nestedForeignColl = testDB.fle_agg_lookup_multi_schema_nested_foreign;
let schemaMapNestedLookup = {};
schemaMapNestedLookup[coll.getFullName()] = {
    enc_item: encryptedStringSpec(),
    enc_sku: encryptedStringSpec()
};
schemaMapNestedLookup[foreignColl.getFullName()] = {
    r_enc_item: encryptedStringSpec(),
    r_enc_sku: encryptedStringSpec()
};
schemaMapNestedLookup[nestedForeignColl.getFullName()] = {
    l_enc_item: encryptedStringSpec(),
    l_enc_sku: encryptedStringSpec()
};

const nestedLookupEncryptionSchemas = generateSchemasFromSchemaMap(schemaMapNestedLookup);

function runTest(test, index) {
    // Validate the test contents.
    const expectedResults = function() {
        assert(test.description != undefined && typeof test.description === "string",
               "Expected test decription.");
        assert(test.command != undefined && typeof test.command === "object", "Expected command.");
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

        // Get the validated expected results. If we had an expected results functor, we invoke it
        // to get the runtime expected results which could differ between FLE1 and FLE2. Otherwise,
        // we pass the original test fixture.
        const expectedRes = getValidatedExpectedResults(function() {
            if (test.runtimeExpectedResults != undefined) {
                assert(typeof test.runtimeExpectedResults === "function");
                return test.runtimeExpectedResults();
            }
            return test;
        }());

        assert(test.runWithFle2 != undefined && typeof test.runWithFle2 === "boolean",
               "Expected runWithFle2 (bool)");
        return expectedRes;
    }();

    jsTestLog("Running test index " + index + " : " + test.description);
    if (fle2Enabled() && !test.runWithFle2) {
        jsTestLog("Skipping unsupported fle2 test");
        return;
    }

    if (test.customTestSetup) {
        test.customTestSetup();
    }
    let command = Object.assign(test.command, test.schemas);

    if (!expectedResults.expectedFail) {
        let cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(
            expectedResults.hasEncryptionPlaceholders, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(
            expectedResults.schemaRequiresEncryption, cmdRes.schemaRequiresEncryption, cmdRes);

        if (test.customTestFunc) {
            test.customTestFunc(cmdRes);
        }
    } else {
        assert.commandFailedWithCode(testDB.runCommand(command), expectedResults.failCodes);
    }
    if (test.customTestTeardown) {
        test.customTestTeardown();
    }
}

/**
 * Test case format:
 * {
 *  description: "" (required),
 *  cmd: {} (required),
 *  schemas: {} (required),
 *  failCodes: [] (optional),
 *  hasEncryptionPlaceholders: boolean (optional),
 *  schemaRequiresEncryption: boolean (optional),
 *  runWithFle2: boolean (required),
 *  customTestSetup: function (optional),
 *  customTestFunc: function (optional),
 *  customTestTeardown: function (optional),
 *  runtimeExpectedResults: function(optional)
 *  }
 *
 * */

const testList = [
    {
        description:
            "Test foreign join $lookup without subpipeline, join on unencrypted fields works",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 0
    {
        description:
            "Test foreign join $lookup without subpipeline, join on encrypted fields fails (local field).",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "enc_sku", foreignField: "r_enc_sku"}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [51206],
        runWithFle2: true
    },// 1
    {
        description:
            "Test foreign join $lookup without subpipeline, join on encrypted fields fails (foreign field).",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_enc_sku"}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [51207],
        runWithFle2: true
    },// 2
    {
        description:
            "Test foreign join on field with encrypted child fails (local field).",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup:
                    {from: foreignColl.getName(), as: "docs", localField: "parent.enc_item", foreignField: "r_foo"}
            }],
            cursor: {}
        },
        schemas: nestedEncryptedChildEncryptionSchemas,
        failCodes: [51206],
        runWithFle2: true
    },// 3
    {
        description:
            "Test foreign join on field with encrypted child fails (foreign field).",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup:
                    {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_parent.r_enc_item"}
            }],
            cursor: {}
        },
        schemas: nestedEncryptedChildEncryptionSchemas,
        failCodes: [51207],
        runWithFle2: true
    },// 4
    {
        description:
            "Test joining encrypted collection with unencrypted collection, join on unencrypted fields.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}}
            ],
            cursor: {}
        },
        schemas: encryptedAndUnencryptedEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 5
    {
        description:
            "Test joining encrypted collection with unencrypted collection, join on unencrypted fields, referencing \"as\" array works, since it is unencrypted.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$match: {docs: []}}
            ],
            cursor: {}
        },
        schemas: encryptedAndUnencryptedEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 6
    {
        description:
            "Test joining encrypted collection with unencrypted collection, join on unencrypted fields, referencing fields within \"as\" array works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$match: {"docs.r_sku": "banana"}}
            ],
            cursor: {}
        },
        schemas: encryptedAndUnencryptedEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 7
    // Begin tests for local field/foreign field $lookup, two encrypted collections without pipeline.
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test using unwind after foreign lookup works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    }, // 8
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test referencing encrypted fields after $unwind works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}},
                {$match: {"docs.r_enc_item": "banana"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[2].$match["docs.r_enc_item"].$eq instanceof BinData, cmdRes);
        }
    }, // 9
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test referencing unencrypted fields after $unwind works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}},
                {$match: {"docs.r_foo": "banana"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    }, // 10
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test referencing encrypted fields without $unwind fails (encrypted array).",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$match: {"docs.r_enc_item": "banana"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 11
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test referencing encrypted array without $unwind fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$match: {"docs": "[{r_enc_item: 123, r_enc_sku: 456}]"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 12
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test reference unencrypted field from encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$match: {"docs.r_unencryptedStr": "banana"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 13
    // Begin tests for local field/foreign field $lookup, two encrypted collections with pipeline.
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) with pipeline. Test lookup when join field has been excluded in projection works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$project: {"r_unencryptedStr": 1}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true,
    },// 14
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) with pipeline. Test subpipeline without encrypted fields referenced.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_unencryptedStr": "banana"}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 15
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) with pipeline. Test subpipeline with encrypted fields referenced has encryption placeholders.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
        }
    },// 16
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) with pipeline. Test that joining encrypted field from rhs that has been projected out in sub-pipeline fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_enc_item", pipeline: [{$project: {"r_enc_sku": 1}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [51207],
        runWithFle2: true
    },// 17
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) with pipeline. Test that joining encrypted field from rhs that has not been projected out in sub-pipeline fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_enc_item", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [51207],
        runWithFle2: true
    },// 18
    {
        description:
            "Local field/foreign field $lookup followed by $unwind, subpipeline with encrypted fields referenced has encryption placeholders, and referencing encrypted field in subsequent stages works and has encryption placeholders.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes){
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.r_enc_sku"].$eq instanceof BinData, cmdRes);
        }
    },// 19
    {
        description:
            "Local field/foreign field $lookup followed by $unwind, subpipeline with encrypted fields referenced has encryption placeholders, and referencing unencrypted field in subsequent stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes){
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[2].$match["docs.r_foo"].$eq instanceof BinData), cmdRes);
        }
    },// 20
    {
        description:
            "Local field/foreign field $lookup without $unwind, subpipeline with encrypted fields, reference encrypted array fields fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 21
    {
        description:
            "Local field/foreign field $lookup without $unwind, subpipeline with encrypted fields, reference encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs": "[{r_enc_item: 123, r_enc_sku: 456}]"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 22
    {
        description:
            "Local field/foreign field $lookup without $unwind, subpipeline with encrypted fields, reference unencrypted field from encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs.r_unencryptedStr": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 23
    {
        description:
            "Local field/foreign field $lookup followed by $sort and then an $unwind on the as field, subpipeline with encrypted \
fields referenced has encryption placeholders, and referencing unencrypted field in subsequent \
stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[3].$match["docs.r_foo"].$eq instanceof BinData), cmdRes);
        }
    },// 24
    {
        description:
            "Local field/foreign field $lookup followed by $sort and then an $unwind on the as field, subpipeline with \
            encrypted fields referenced has encryption placeholders, and referencing unencrypted \
            field in subsequent stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$projected_docs"}},
            {$match: {"projected_docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[4].$match["projected_docs.r_foo"].$eq instanceof BinData),
                cmdRes);
        }
    },// 25
    {
        description:
            "Local field/foreign field $lookup followed by $sort, a $project, and then an $unwind on the projected as field, \
subpipeline with encrypted fields referenced has encryption placeholders, and referencing \
encrypted field in subsequent stages works, has placeholders marked.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$projected_docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[4].$match["projected_docs.r_enc_item"].$eq instanceof BinData,
                cmdRes);
        }
    },// 26
    {
        description:
            "Local field/foreign field $lookup followed by $sort, $project and then an $unwind on a field that is not the \
             \"as\" field (due to the projection), subpipeline with encrypted fields referenced has \
            encryption placeholders, fails when referencing encrypted field.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 27
    {
        description:
            "Local field/foreign field $lookup followed by $sort, $project and then an $unwind on a field that is not the \
            \"as\" field (due to the projection), subpipeline with encrypted fields referenced has \
            encryption placeholders, fails when referencing unencrypted field.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$docs"}},
            {$match: {"projected_docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 28
    {
        description:
            "Local field/foreign field $lookup followed by $sort, $match on encrypted field in encrypted lookup array, \
            $project and then an $unwind on \"as\" field fails when referencing encrypted field before the unwind.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo", pipeline: [{$match: {"r_foo": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}},
            {$unwind: {path: "$projected_docs"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 29
    // Begin nested lookup tests.
    {
        description:
            "Nested lookup has subpipeline with encrypted placeholders (no unwind). \
            Test that nested subpipeline has encrypted placeholder.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{
                                $lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo",
                                    pipeline: [
                                        {$match: {"l_enc_item": "banana"}}]
                                    }
                                }, 
                                {$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            // Nested subpipeline must have encryption placeholders
            assert(cmdRes.result.pipeline[0]
                        .$lookup.pipeline[0]
                        .$lookup.pipeline[0]
                        .$match["l_enc_item"]
                        .$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[1].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.r_enc_sku"].$eq instanceof BinData, cmdRes);
        }
    },// 30
    {
        description:
            "Nested lookup has subpipeline with encrypted placeholders (with unwind). \
             Test that nested subpipeline has encrypted placeholder, and that we can reference the \
             encrypted field resulting from the join.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: foreignColl.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: [
                            {
                                $lookup: {
                                        from: nestedForeignColl.getName(),
                                        as: "inner_docs",
                                        localField: "r_foo",
                                        foreignField: "l_foo",
                                        pipeline: [{$match: {"l_enc_item": "banana"}}]}
                            },
                            {$unwind: {path: "$inner_docs"}},
                            {$match: {"inner_docs.l_enc_sku": "banana"}}
                        ]
                    }},
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0]
                        .$lookup.pipeline[0]
                        .$lookup.pipeline[0]
                        .$match["l_enc_item"]
                        .$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[2].$match["inner_docs.l_enc_sku"].$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData,
                cmdRes);
        }
    },// 31
    {
        description:
            "Nested lookup has subpipeline with encrypted placeholders (no unwind). \
             Test that referencing encrypted field fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo",
                                    pipeline: [{$match: {"l_enc_item": "banana"}}]}},
                                {$match: {"inner_docs.l_enc_sku": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 32
    {
        description:
            "Nested lookup has subpipeline with encrypted placeholders (no unwind). \
             Test that referencing unencrypted field fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo",
                                    pipeline: [{$match: {"l_enc_item": "banana"}}]}},
                                {$match: {"inner_docs.l_unencrypted_str": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 33
    {
        description:
            "Nested lookup has no subpipeline. Test that pipeline has encrypted placeholder, and we \
             succeed as long as the encrypted fields from nested join are not referenced.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}}, 
                                {$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[1].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.r_enc_sku"].$eq instanceof BinData, cmdRes);
        }
    },// 34
    {
        description:
            "Nested lookup has no subpipeline (with unwind). Test that we can reference the \
             encrypted field resulting from the nested join.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}},
                                {$unwind: {path: "$inner_docs"}},
                                {$match: {"inner_docs.l_enc_sku": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[2].$match["inner_docs.l_enc_sku"].$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData,
                cmdRes);
        }
    },// 35
    {
        description:
            "Nested lookup has no subpipeline (with unwind), match stage before $lookup. \
             Test that we can reference the encrypted field resulting from the nested join.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {$match: {"unencrypted_field": 1}},
                {
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}},
                                {$unwind: {path: "$inner_docs"}},
                                {$match: {"inner_docs.l_enc_sku": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[1].$lookup.pipeline[2].$match["inner_docs.l_enc_sku"].$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[3].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData,
                cmdRes);
        }
    },// 36
    {
        description: 
            "Nested lookup has no subpipeline (no unwind). Test that referencing encrypted field fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}},
                                {$match: {"inner_docs.l_enc_sku": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 37
    {
        description: 
            "Nested lookup has subpipeline (no unwind). Test that referencing unencrypted field fails due to encrypted array.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}},
                                {$match: {"inner_docs.l_unencrypted_str": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 38
    {
        description: 
            "Nested lookup has no pipeline. Test that referencing unencrypted field at the top level pipeline fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}}, 
                                {$match: {"r_foo": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 39
    {
        description: 
            "Nested lookup has no subpipeline (with unwind), match stage before $lookup. \
             Test that we can reference the encrypted field resulting from the nested join, and array \
             index is unencrypted and referenceable.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {$match: {"unencrypted_field": 1}},
                {
                $lookup: {
                    from: foreignColl.getName(), 
                    as: "docs",
                    localField: "foo",
                    foreignField: "r_foo", 
                    pipeline: [{$lookup: {
                                    from: nestedForeignColl.getName(),
                                    as: "inner_docs",
                                    localField: "r_foo",
                                    foreignField: "l_foo"}},
                                {$unwind: {path: "$inner_docs", includeArrayIndex: "arrIndex"}},
                                {$match: {$or: [{"inner_docs.l_enc_sku": "banana"},{"inner_docs.arrIndex": 1} ]}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(
                cmdRes.result.pipeline[1].$lookup.pipeline[2].$match.$or[0]["inner_docs.l_enc_sku"].$eq instanceof
                    BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[1]
                         .$lookup.pipeline[2]
                         .$match.$or[1]["inner_docs.arrIndex"]
                         .$eq instanceof
                     BinData),
                   cmdRes);
            assert(cmdRes.result.pipeline[3].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData, cmdRes);
        }
    },// 40
    {
        description: 
            "Pattern properties test, only relevant to FLE1. Test foreign join $lookup without subpipeline, join on unencrypted fields works, reference an encrypted pattern property should add an encryption placeholder.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup:
                    {from: foreignColl.getName(), as: "test", localField: "foo", foreignField: "r_foo"}
            },
            {$unwind: {path: "$test"}},
            {
                $match:
                    {"test.test_a": "banana"}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: false, // patternProperties only relevant to fle1.
        customTestSetup: function() {
            let csfleEncryptionSchemas = twoEncryptedCollEncryptionSchemas["csfleEncryptionSchemas"];
            let foreignSchema = csfleEncryptionSchemas[foreignColl.getFullName()];
            foreignSchema.jsonSchema["patternProperties"] = {};
            foreignSchema.jsonSchema["patternProperties"].test = encryptedStringSpec();
        },
        customTestTeardown: function() {
            delete twoEncryptedCollEncryptionSchemas.csfleEncryptionSchemas[foreignColl.getFullName()].jsonSchema.patternProperties;    
        }
    },// 41
    // start no foreign join tests
    {
        description:
            "$lookup (both collections encrypted) with pipeline. Test lookup when join field has been excluded in projection works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$project: {"r_unencryptedStr": 1}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true,
    },// 42
    {
        description:
            "$lookup (both collections encrypted) with pipeline. Test subpipeline without encrypted fields referenced.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_unencryptedStr": "banana"}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    },// 43
    {
        description:
            "$lookup (both collections encrypted) with pipeline. Test subpipeline with encrypted fields referenced has encryption placeholders.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            }],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
        }
    },// 44
    {
        description:
            "$lookup followed by $unwind, subpipeline with encrypted fields referenced has encryption placeholders, and referencing encrypted field in subsequent stages works and has encryption placeholders.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes){
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.r_enc_sku"].$eq instanceof BinData, cmdRes);
        }
    },// 45
    {
        description:
            "$lookup followed by $unwind, subpipeline with encrypted fields referenced has encryption placeholders, and referencing unencrypted field in subsequent stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes){
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[2].$match["docs.r_foo"].$eq instanceof BinData), cmdRes);
        }
    },// 46
    {
        description:
            "$lookup without $unwind, subpipeline with encrypted fields, reference encrypted array fields fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs.r_enc_sku": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 47
    {
        description:
            "$lookup without $unwind, subpipeline with encrypted fields, reference encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs": "[{r_enc_item: 123, r_enc_sku: 456}]"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 48
    {
        description:
            "$lookup without $unwind, subpipeline with encrypted fields, reference unencrypted field from encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$match: {"docs.r_unencryptedStr": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 49
    {
        description:
            "$lookup followed by $sort and then an $unwind on the as field, subpipeline with encrypted \
            fields referenced has encryption placeholders, and referencing unencrypted field in subsequent \
            stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$unwind: {path: "$docs"}},
            {$match: {"docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[3].$match["docs.r_foo"].$eq instanceof BinData), cmdRes);
        }
    },// 50
    {
        description:
            "$lookup followed by $sort and then an $unwind on the as field, subpipeline with \
            encrypted fields referenced has encryption placeholders, and referencing unencrypted \
            field in subsequent stages works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$projected_docs"}},
            {$match: {"projected_docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: true,
        schemaRequiresEncryption: true,
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(!(cmdRes.result.pipeline[4].$match["projected_docs.r_foo"].$eq instanceof BinData),
                cmdRes);
        }
    },// 51
    {
        description:
            "Test lookup followed by $sort, a $project, and then an $unwind on the projected as field, \
            subpipeline with encrypted fields referenced has encryption placeholders, and referencing \
            encrypted field in subsequent stages works, has placeholders marked.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$projected_docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0].$lookup.pipeline[0].$match["r_enc_item"].$eq instanceof BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[4].$match["projected_docs.r_enc_item"].$eq instanceof BinData,
                cmdRes);
        }
    },// 52
    {
        description:
            "Test lookup followed by $sort, $project and then an $unwind on a field that is not the \
            \"as\" field (due to the projection), subpipeline with encrypted fields referenced has \
            encryption placeholders, fails when referencing encrypted field.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 53
    {
        description:
            "Test lookup followed by $sort, $project and then an $unwind on a field that is not the \
            \"as\" field (due to the projection), subpipeline with encrypted fields referenced has \
            encryption placeholders, fails when referencing unencrypted field.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_enc_item": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$unwind: {path: "$docs"}},
            {$match: {"projected_docs.r_foo": "banana"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 54
    {
        description:
            "Test lookup followed by $sort, $match on encrypted field in encrypted lookup array, \
            $project and then an $unwind on \"as\" field fails when referencing encrypted field before the unwind.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                $lookup: {from: foreignColl.getName(), as: "docs", pipeline: [{$match: {"r_foo": "banana"}}]}
            },
            {$sort: {bar : 1 }},
            {$project: {"projected_docs" : "$docs"}},
            {$match: {"projected_docs.r_enc_item": "banana"}},
            {$unwind: {path: "$projected_docs"}}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes: [31133],
        runWithFle2: true
    },// 55
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test projecting an encrypted array works.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$project: {"projected_docs" : "$docs"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    }, // 56
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test projecting an unencrypted field from encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$project: {"projected_docs" : "$docs.r_foo"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes:[31133],
        runWithFle2: true
    }, // 57
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test projecting an encrypted field from encrypted array fails.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$project: {"projected_docs" : "$docs.r_enc_sku"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        failCodes:[31133],
        runWithFle2: true
    }, // 58
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test projecting an unencrypted field from encrypted array works after an unwind.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}},
                {$project: {"projected_docs" : "$docs.r_foo"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    }, // 59
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test projecting an encrypted field from encrypted array works after an unwind, but not for FLE2.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}},
                {$project: {"projected_docs" : "$docs.r_enc_sku"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [6331102, 31133]};
            }
            return {hasEncryptionPlaceholders: false, schemaRequiresEncryption: true};
        },
        runWithFle2: true
    }, // 60
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test project encrypted array, then unwind it, then project an unencrypted field",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$project: {"projected_docs" : "$docs"}},
                {$unwind: {path: "$projected_docs"}},
                {$project: {"projected_internal" : "$projected_docs.r_foo"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        hasEncryptionPlaceholders: false,
        schemaRequiresEncryption: true,
        runWithFle2: true
    }, // 61
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test project encrypted array, then unwind it, then project an encrypted field (should fail for FLE2.",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$project: {"projected_docs" : "$docs"}},
                {$unwind: {path: "$projected_docs"}},
                {$project: {"projected_internal" : "$projected_docs.r_enc_sku"}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [6331102,31133]};
            }
            return {hasEncryptionPlaceholders: false, schemaRequiresEncryption: true};
        },
        runWithFle2: true
    }, // 62
    {
        description:
            "Local field/foreign field $lookup (both collections encrypted) without pipeline. Test project encrypted array, then unwind it, then equality match on an encrypted field (should fail for FLE2).",
        command: {
            aggregate: coll.getName(),
            pipeline: [
                {
                    $lookup:
                        {from: foreignColl.getName(), as: "docs", localField: "foo", foreignField: "r_foo"}
                },
                {$unwind: {path: "$docs"}},
                {$match: {$expr: {$and: [{$eq: ["$docs.r_enc_sku", "banana"]}, {$eq:["$docs.r_foo", "banana"]}]}}}
            ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true
    }, // 63
    {
        description:
            "Nested lookup has subpipeline with encrypted placeholders (with unwind). \
             Test that nested subpipeline has encrypted placeholder, and that we can reference the \
             encrypted field resulting from the join. Should fail for FLE2.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: foreignColl.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: [
                            {
                                $lookup: {
                                        from: nestedForeignColl.getName(),
                                        as: "inner_docs",
                                        localField: "r_foo",
                                        foreignField: "l_foo",
                                        pipeline: [{$match: {"l_enc_item": "banana"}}]}
                            },
                            {$unwind: {path: "$inner_docs"}}
                        ]
                    }},
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[0]
                        .$lookup.pipeline[0]
                        .$lookup.pipeline[0]
                        .$match["l_enc_item"]
                        .$eq instanceof
                    BinData,
                cmdRes);
            assert(cmdRes.result.pipeline[2].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData,
                cmdRes);
        }
    }, // 64
    {
        description:
            "Nested lookup has subpipeline (with unwind). Test that we can reference the \
             encrypted field resulting from the join. Should fail for FLE2. This is a simplified \
             pipeline for testing the relaxed tassert in EncryptionSchemaEncryptedNode::markEncryptedObjectArrayElements",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: foreignColl.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: [
                            {
                                $lookup: {
                                        from: nestedForeignColl.getName(),
                                        as: "inner_docs",
                                        localField: "r_foo",
                                        foreignField: "l_foo"}
                            },
                            {$unwind: {path: "$inner_docs"}}
                        ]
                    }},
            {$unwind: {path: "$docs"}},
            {$match: {"docs.inner_docs.l_enc_sku": "apple"}}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            if (fle2Enabled()) {
                return {failCodes: [31133]};
            }
            return {hasEncryptionPlaceholders: true, schemaRequiresEncryption: true};
        },
        runWithFle2: true,
        customTestFunc: function(cmdRes) {
            assert(cmdRes.result.pipeline[2].$match["docs.inner_docs.l_enc_sku"].$eq instanceof BinData,
                cmdRes);
        }
    }, // 65
    {
        description:
            "Test for $graphLookup in a $lookup multiple collections.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: foreignColl.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: [
                            {
                                $graphLookup: {
                                        from: foreignColl.getName(),
                                        as: "reportingHierarchy",
                                        connectToField: "name",
                                        connectFromField: "reportsTo",
                                        startWith: "$reportsTo"
                                    }
                            }
                        ]
                    }}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            return {hasEncryptionPlaceholders: false, schemaRequiresEncryption: true};
        },
        runWithFle2: true
    }, // 66
    {
        description:
            "Test for $graphLookup over multiple collections. The feature flag for $lookup support removes an important check which $graphLookup relies on to block foreign graphlookups.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: coll.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: [
                            {
                                $graphLookup: {
                                        from: foreignColl.getName(),
                                        as: "reportingHierarchy",
                                        connectToField: "name",
                                        connectFromField: "reportsTo",
                                        startWith: "$reportsTo"
                                    }
                            }
                        ]
                    }}
        ],
            cursor: {}
        },
        schemas: twoEncryptedCollEncryptionSchemas,
        runtimeExpectedResults: function() {
            return {failCodes: [9894800]};
        },
        runWithFle2: true
    }, // 67
    {
        description:
            "Test for foreign lookup with empty sub-pipeline. Required to test an empty pipeline.",
        command: {
            aggregate: coll.getName(),
            pipeline: [{
                    $lookup: {
                        from: foreignColl.getName(), 
                        as: "docs",
                        localField: "foo",
                        foreignField: "r_foo", 
                        pipeline: []
                    }}
        ],
            cursor: {}
        },
        schemas: nestedLookupEncryptionSchemas,
        runtimeExpectedResults: function() {
            return {hasEncryptionPlaceholders: false, schemaRequiresEncryption: true};
        },
        runWithFle2: true
    } // 68
];

testList.forEach(runTest);

mongocryptd.stop();
