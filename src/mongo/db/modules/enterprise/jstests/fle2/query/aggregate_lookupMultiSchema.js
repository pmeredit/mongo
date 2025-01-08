/**
 * End to end test for $lookup involving multiple encrypted collections.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   featureFlagLookupEncryptionSchemasFLE,
 * ]
 */

import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {
    EncryptedClient,
    isEnterpriseShell,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    generateSchemaV1,
    kDeterministicAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    fleAggTestData
} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/agg_utils.js";

if (!isEnterpriseShell()) {
    jsTestLog("Skipping test as it requires the enterprise module");
    quit();
}

const dbName = "aggregateLookupMultiSchemaDB";
const collNameUsers = "aggregateLookupMultiSchemaUsers";
const collNamePasswords = "aggregateLookupMultiSchemaCollPasswords";
const collNameAccounts = "aggregateLookupMultiSchemaCollAccounts";

const encryptedStringSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}});
const encryptedLongSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}});

const docsCollAccounts = [
    {_id: 0, accountNumber: 51353, PIN: "0000", name: "A"},
    {_id: 1, accountNumber: 26254, PIN: "1111", name: "B"},
    {_id: 2, accountNumber: 89255, PIN: "2222", name: "C"},
    {_id: 3, accountNumber: 85435, PIN: "3333", name: "D"},
];

const docsCollPasswords = [
    {_id: 0, password: "abc123", name: "A"},
    {_id: 1, password: "def456", name: "B"},
    {_id: 2, password: "ghi789", name: "C"},
    {_id: 3, password: "jkl012", name: "D"},
];

const tests = [
    // 1) $lookup with a filter on an encrypted field in sub-pipeline.
    {
        pipeline: [
            {
                $lookup: {
                    from: collNamePasswords,
                    as: "passwords",
                    localField: "name",
                    foreignField: "name",
                    pipeline: [
                        {$match: {password: {$in: ["abc123", "ghi789"]}}}
                    ]
                }
            }
        ],
        expected: [
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0], passwords: [
                                                                                                    {_id: 0, password: "abc123", name: "A"}
                                                                                                            ]},
            {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1], passwords: []},
            {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2], passwords: [
                                                                                                    {_id: 2, password: "ghi789", name: "C"}
                                                                                                            ]},
            {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3], passwords: []}
        ]
    }, // 2) Nested $lookup with a filter on an encrypted field in sub-pipeline.
    {
        pipeline: [
            {
                $lookup: {
                    from: collNamePasswords,
                    as: "passwords",
                    localField: "name",
                    foreignField: "name",
                    pipeline: [
                        {
                            $lookup: {
                                from: collNameAccounts,
                                as: "accounts",
                                localField: "name",
                                foreignField: "name",
                                pipeline: [
                                    {$match: {PIN: {$in: ["0000", "2222"]}}}
                                ]
                            }
                        },
                        {$match: {password: {$in: ["abc123", "ghi789"]}}}
                    ]
                }
            }
        ],
        expected: [
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0], passwords: [
                                                                                                    {
                                                                                                        _id: 0, 
                                                                                                        password: "abc123",
                                                                                                        name: "A",
                                                                                                        accounts: [
                                                                                                            {_id: 0, accountNumber: 51353, PIN: "0000", name: "A"}
                                                                                                        ]
                                                                                                    }
                                                                                                ]},
            {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1], passwords: []},
            {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2], passwords: [
                                                                                                    {
                                                                                                        _id: 2, 
                                                                                                        password: "ghi789",
                                                                                                        name: "C",
                                                                                                        accounts: [
                                                                                                            {_id: 2, accountNumber: 89255, PIN: "2222", name: "C"}
                                                                                                        ]
                                                                                                    }
                                                                                                ]},
            {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3], passwords: []}
        ]
    }
];

// Test CSFLE
execTest(false);
// Test FLE2
execTest(true);

function createCSFLEEncryptedCollection(client, edb, collName, schemaObj) {
    client.runEncryptionOperation(() => {
        for (let value of Object.values(schemaObj)) {
            let testkeyId = client._keyVault.createKey("local", "ignored");
            value["encrypt"].keyId = [testkeyId];
        }
    });

    // In FLE 1, encrypted collections are defined by their jsonSchema validator.
    assert.commandWorked(edb.runCommand({
        create: collName,
        validator: {$jsonSchema: generateSchemaV1(schemaObj).jsonSchema},
    }));
}

function execTest(testFle2) {
    jsTestLog("Starting test execution for " + (testFle2 ? "FLE2" : "CSFLE"));
    const docsCollUsers = fleAggTestData.docs;

    // Set up the encrypted collections.
    const dbTest = db.getSiblingDB(dbName);
    dbTest.dropDatabase();
    let client = new EncryptedClient(db.getMongo(), dbName);
    let edb = client.getDB();

    // Create and populate "Users" collection
    if (testFle2) {
        const schemaCollUsers = fleAggTestData.schema;
        assert.commandWorked(client.createEncryptionCollection(collNameUsers, schemaCollUsers));
    } else {
        createCSFLEEncryptedCollection(
            client, edb, collNameUsers, {ssn: encryptedStringSpec(), age: encryptedLongSpec()});
    }

    const collUsers = edb[collNameUsers];
    for (const doc of docsCollUsers) {
        assert.commandWorked(collUsers.einsert(doc));
    }

    // Create and populate "Passwords" collection.
    if (testFle2) {
        const schemaCollPasswords = {
            encryptedFields:
                {fields: [{path: "password", bsonType: "string", queries: {queryType: "equality"}}]}
        };
        assert.commandWorked(
            client.createEncryptionCollection(collNamePasswords, schemaCollPasswords));
    } else {
        createCSFLEEncryptedCollection(
            client, edb, collNamePasswords, {password: encryptedStringSpec()});
    }
    const collPasswords = edb[collNamePasswords];
    for (const doc of docsCollPasswords) {
        assert.commandWorked(collPasswords.einsert(doc));
    }

    // Create and populate "Accounts" collection.
    if (testFle2) {
        const schemaCollAccounts = {
            encryptedFields:
                {fields: [{path: "PIN", bsonType: "string", queries: {queryType: "equality"}}]}
        };
        assert.commandWorked(
            client.createEncryptionCollection(collNameAccounts, schemaCollAccounts));
    } else {
        createCSFLEEncryptedCollection(client, edb, collNameAccounts, {PIN: encryptedStringSpec()});
    }

    const collAccounts = edb[collNameAccounts];
    for (const doc of docsCollAccounts) {
        assert.commandWorked(collAccounts.einsert(doc));
    }

    // Run the pipeline on the provided collection, and assert that the results are equivalent to
    // 'expected'. The pipeline is appended with a $project stage to project out safeContent data
    // and other fields that are inconvenient to have in the output.
    const runTestFunc = (pipeline, collection, expected, extraInfo) => {
        const aggPipeline = pipeline.slice();
        aggPipeline.push({
            $project: {
                [kSafeContentField]: 0,
                [`passwords.${kSafeContentField}`]: 0,
                [`passwords.accounts.${kSafeContentField}`]: 0
            }
        });
        const result = collection.aggregate(aggPipeline).toArray();
        assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
    };

    // Run all of the tests.
    client.runEncryptionOperation(() => {
        for (const testData of tests) {
            const extraInfo = Object.assign({transaction: false}, testData);
            runTestFunc(testData.pipeline, collUsers, testData.expected, extraInfo);
        }
    });
}
