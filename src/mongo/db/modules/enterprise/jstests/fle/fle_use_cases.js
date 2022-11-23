/**
 * Test correctness of Field Level Encryption for different commands in realistic use cases.
 *
 * The majority of this test refers to encrypted fields in aggregation pipelines, which is not
 * allowed in FLE 2.
 * @tags: [unsupported_fle_2]
 */

(function() {
"use strict";

load('jstests/ssl/libs/ssl_helpers.js');
load('jstests/aggregation/extras/utils.js');
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

// Set up key management and encrypted shell.
const x509_options = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT,
    vvvvv: ""
};

const conn = MongoRunner.runMongod(x509_options);

let localKMS = {
    key: BinData(
        0,
        "tu9jUCBqZdwCelwE/EAm/4WqdxrSMi04B8e9uAV+m30rI1J2nhKZZtQjdvsSCwuI4erR6IEcEK+5eGUAODv43NDNIR9QheT2edWFewUfHKsl9cnzTc86meIzOmYl6drp"),
};

const clientSideRemoteSchemaFLEOptions = {
    kmsProviders: {
        local: localKMS,
    },
    keyVaultNamespace: "test.keystore",
    schemaMap: {},
};

var encryptedShell = Mongo(conn.host, clientSideRemoteSchemaFLEOptions);
var keyVault = encryptedShell.getKeyVault();

keyVault.createKey("local", ['key1']);
keyVault.createKey("local", ['key2']);
const defaultKeyId = keyVault.getKeyByAltName("key1").toArray()[0]._id;
const insuranceKeyId = keyVault.getKeyByAltName("key2").toArray()[0]._id;

Random.setRandomSeed();

/**
 * Run a given sequence of commands against a collection with FLE enabled and a collection
 * without FLE. If FLE is working properly when both encrypting and decrypting ciphertext coming
 * from mongod, then there should be no difference between the documents returned from the two
 * collections when any aggregation or query is run against them.
 *
 * sequenceFunc is a function which performs actions on a collection and incrementally returns
 * results. sequenceFunc is run twice, once with the FLE-enabled collection, and once
 * with the non-encrypted collection. Results are compared to ensure that identical documents
 * are returned.
 *
 * In order to enable this, sequenceFunc should take in a collection and a callback. All
 * operations should be executed on the given collection. Any time the sequence wants to emit
 * results that should be compared between collections, it should call the callback with the
 * cursor.
 */
function testSequence({sequenceFunc, encryptedSchema}) {
    const encryptedDatabase = encryptedShell.getDB("crypt");
    const unEncryptedDatabase = conn.getDB("unEncrypt");

    encryptedDatabase.coll.drop();
    unEncryptedDatabase.coll.drop();

    // For FLE to be used in the shell, need to add the schema to the collection as a validator.
    assert.commandWorked(
        encryptedDatabase.createCollection("coll", {validator: {$jsonSchema: encryptedSchema}}));

    const encryptedColl = encryptedDatabase.coll;
    const unEncryptedColl = unEncryptedDatabase.coll;

    let encryptedResults = [];
    let unEncryptedResults = [];

    const seed = Random.rand();

    // Since documents are generated randomly, it's important to set the seed before running the
    // sequence so that the same documents are generated in both executions.
    Random.setRandomSeed(seed);
    sequenceFunc(encryptedColl, result => encryptedResults.push(result));
    Random.setRandomSeed(seed);
    sequenceFunc(unEncryptedColl, result => unEncryptedResults.push(result));

    for (let i = 0; i < encryptedResults.length; i++) {
        assert(resultsEq(encryptedResults[i], unEncryptedResults[i]),
                   `Mismatch in the ${i}th result in the sequence.
From encrypted collection: ${tojson(encryptedResults[i])}
From unencrypted collection: ${tojson(unEncryptedResults[i])}`);
    }
}

/*
 * Schema for medical test data, where personally identifiable information about the doctor
 * performing the test and the patient is encrypted.
 */
const medTestSchema = {
    encryptMetadata: {
        algorithm: kDeterministicAlgo,
        keyId: [defaultKeyId],
    },
    type: "object",
    patternProperties: {
        'secret_.*': {
            encrypt: {bsonType: "string"},
        }
    },
    properties: {
        testType: {encrypt: {bsonType: "string"}},
        insuranceStatus: {encrypt: {bsonType: "string"}},
        patient: {
            type: "object",
            properties: {
                name: {encrypt: {bsonType: "string"}},
                age: {encrypt: {bsonType: "int"}},
                insurance: {
                    encrypt: {algorithm: kRandomAlgo, bsonType: "string", keyId: [insuranceKeyId]}
                },
                loc: {
                    type: "object",
                    patternProperties: {
                        'zip': {encrypt: {bsonType: "string"}},
                    },
                    properties: {
                        city: {encrypt: {bsonType: "string"}},
                        state: {encrypt: {bsonType: "string"}}
                    }
                }
            }
        },
        doctor: {
            type: "object",
            properties: {
                name: {encrypt: {bsonType: "string"}},
                insurance: {
                    encrypt: {algorithm: kRandomAlgo, bsonType: "string", keyId: [insuranceKeyId]}
                },
                loc: {
                    type: "object",
                    patternProperties: {
                        'zip': {encrypt: {bsonType: "string"}},
                    },
                    properties: {
                        city: {encrypt: {bsonType: "string"}},
                        state: {encrypt: {bsonType: "string"}}
                    }
                }
            }
        }
    }
};

// Return a random element from the given array.
function randomChoice(array) {
    return array[Random.randInt(array.length)];
}

// Randomly generates a document fitting the medTestSchema above. To simulate incomplete/sparse
// information, some fields are randomly deleted from the document before it is returned to the
// caller to be inserted into the collection.
function generateRandomMedTestDocument(id) {
    // Arrays of options instead of randomly generating strings, so that there's overlap in
    // $group stages.
    const names = ['A1', 'B1', 'A2', 'B2', 'C5', 'D6', 'AA', 'BA'];
    const tests = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'a'];
    const insuranceCompanies = ['W', 'X', 'Y', 'Z', 'ZZA'];
    const zips = ['10024', '10128', '10028', '10023', '10011', '10101', '12345'];
    const cities = [
        'New York',
        'Houston',
        'Atlanta',
        'Chicago',
        'Cleveland',
        'Kansas City',
        'San Francisco',
        'Denver'
    ];
    const states = ['NY', 'NJ', 'CT', 'CA', 'TX', 'GA', 'IL', 'OH', 'KS', 'CO'];
    let doc = {
        _id: id,
        test: randomChoice(tests),
        score: NumberInt(Random.randInt(100)),
        positive: Random.rand() > .5,
        testType: Random.rand() > .5 ? "scalar" : "binary",
        insuranceStatus: randomChoice(["paid", "unpaid", "cantpay"]),
        secret_id: String(Random.randInt(10)),
        patient: {
            name: randomChoice(names),
            age: NumberInt(Random.randInt(100)),
            insurance: randomChoice(insuranceCompanies),
            loc: {zip: randomChoice(zips), city: randomChoice(cities), state: randomChoice(states)}
        },
        doctor: {
            name: randomChoice(names),
            insurance: randomChoice(insuranceCompanies),
            loc: {zip: randomChoice(zips), city: randomChoice(cities), state: randomChoice(states)}
        }
    };
    // Remove location fields randomly to simulate incomplete data.
    const topfields = ['patient', 'doctor'];
    const locFields = [randomChoice(['zip', 'zipCode']), 'city', 'state'];

    for (let i = 0; i < topfields.length; i++) {
        for (let j = 0; j < locFields.length; j++) {
            if (Random.rand() < .1) {
                delete doc[topfields[i]].loc[locFields[j]];
            }
        }
    }
    return doc;
}

function medTestSequence(coll, pushResults) {
    const N_RECORDS = 500;
    const N_RECORDS_TO_UPDATE = 100;

    // Make sure to specify IDs so that documents in successive runs of the sequence will be
    // identical.
    for (let i = 0; i < N_RECORDS; i++)
        coll.insertOne(generateRandomMedTestDocument(i));

    // Helper function which generates an ifNull chain to get some location information.
    const locFallback = root => ({
        $ifNull: [
            `${root}.loc.zip`,
            {$ifNull: [`${root}.loc.city`, {$ifNull: [`${root}.loc.state`, "XXXXX"]}]}
        ]
    });

    const pipelines = [
        // Count up how many doctors and patients had the same name.
        [
            {$addFields: {selfPrescribed: {$eq: ["$patient.name", "$doctor.name"]}}},
            {$match: {selfPrescribed: true}},
            {$group: {_id: null, count: {$sum: 1}}}
        ],
        // Average results on tests, by doctor's location. Make sure to use the proper test
        // field: if it's a negative/positive test, make sure that that's reflected in the
        // average (which then will be the % of tests which are positive).
        [
            {$match: {$expr: {$ne: ["$doctor.name", "AA"]}}},
            {
                $group: {
                    _id: {test: "$test", loc: locFallback("$doctor")},
                    score: {
                        $avg: {
                            $cond: {
                                "if": {$eq: ["$testType", "scalar"]},
                                "then": "$score",
                                "else": {$cond: {"if": "$positive", "then": 1, "else": 0}}
                            }
                        }
                    }
                },
            }
        ],
        // Get doctors and patients living in the same location.
        [
            {$match: {$expr: {$eq: [locFallback("$patient"), locFallback("$doctor")]}}},
        ],
        // Get counts of patients per location in two different locations.
        [
            {$group: {_id: {loc: locFallback("$patient")}, pop: {$sum: 1}}},
            {$match: {"_id.loc": {"$in": ["10024", "NY"]}}}
        ],
        // Make sure sort by count on a deterministically encrypted field works.
        [
            {$sortByCount: locFallback("$patient")},
        ],
        // $switch and conditionally projecting 'insurer' to a randomly encrypted.
        [
            {
                $project: {
                    'doctor.name': 1,
                    'patient.name': 1,
                    // Although this isn't at all how insurance works, it's a good example of
                    // projecting different randomly encrypted fields from a switch on a
                    // deterministically encrypted field.
                    'insurer': {
                        '$switch': {
                            branches: [
                                {
                                    'case': {$eq: ['paid', '$insuranceStatus']},
                                    'then': '$patient.insurance'
                                },
                                {
                                    'case': {$eq: ['unpaid', '$insuranceStatus']},
                                    'then': '$patient.insurance'
                                },
                                {
                                    'case': {$eq: ['$insuranceStatus', 'cantpay']},
                                    'then': '$doctor.insurance'
                                },

                            ],
                            'default': '$$REMOVE'
                        }
                    }
                }
            },
        ],
        // Group by billing location and average up scores. TODO: After SERVER-41991 is
        // completed, this test will pass.
        /*
        [
          {
            $project: {
                'doctor.name': 1,
                'patient.name': 1,
                'test': 1,
                'score': {
                    $avg: {
                        $cond: {
                            "if": {$eq: ["$testType", "scalar"]},
                            "then": "$score",
                            "else": {$cond: {"if": "$positive", "then": 1, "else": 0}}
                        }
                    }
                },
                'billingAddress': {
                    '$switch': {
                        branches: [
                            {
                              'case': {$eq: ['paid', '$insuranceStatus']},
                              'then': locFallback('$patient'),
                            },
                            {
                              'case': {$eq: ['unpaid', '$insuranceStatus']},
                              'then': locFallback('$patient'),
                            },
                            {
                              'case': {$eq: ['$insuranceStatus', 'cantpay']},
                              'then': locFallback('$doctor'),
                            },

                        ],
                        'default': '$$REMOVE'
                    }
                }
            }
          },
          {
            $group: {
                _id: {test: '$test', loc: '$billingAddress'},
                avgScore: {$avg: '$score'},
            }
          }
        ],
        */
        // Group by test type, get all patients who have taken that test.
        [
            {$group: {_id: '$test', patients: {$push: '$patient.name'}}},
            {$match: {_id: {'$in': ['A', 'E', 'G']}}},
        ],
        // Group on secret_id, and test, averaging scores along the way.
        [
            {
                $group: {
                    _id: {'secret': '$secret_id', 'test': '$test'},
                    score: {
                        $avg: {
                            $cond: {
                                "if": {$eq: ["$testType", "scalar"]},
                                "then": "$score",
                                "else": {$cond: {"if": "$positive", "then": 1, "else": 0}}
                            }
                        }
                    }
                }
            },
        ]
    ];
    // Wrap the call to aggregate in a try/catch so that errors can be compared across
    // encrypted/unencrypted collections.
    function aggregate(pipeline) {
        try {
            return coll.aggregate(pipeline).toArray();
        } catch (e) {
            return [{code: e.code}];
        }
    }
    for (let i = 0; i < pipelines.length; i++) {
        pushResults(aggregate(pipelines[i]));
    }
    const updatableId = i => 'u' + i;
    // Add in new (incomplete) test records after tests are conducted, but before scores
    // come back.
    for (let i = 0; i < N_RECORDS_TO_UPDATE; i++) {
        let doc = generateRandomMedTestDocument(updatableId(i));
        delete doc.score;
        delete doc.positive;
        delete doc.insuranceStatus;
        coll.insertOne(doc);
    }
    // Make sure all aggregations still match across encrypted/unencrypted.
    for (let i = 0; i < pipelines.length; i++) {
        pushResults(aggregate(pipelines[i]));
    }
    // Fill in missing fields on the new documents.
    for (let i = 0; i < N_RECORDS_TO_UPDATE; i++) {
        coll.updateOne({_id: updatableId(i)}, {
            $set: {
                score: NumberInt(Random.randInt(100)),
                positive: Random.rand() > .5,
                insuranceStatus: randomChoice(["paid", "unpaid", "cantpay"])
            }
        });
    }
    // Once again verify that results match across.
    for (let i = 0; i < pipelines.length; i++) {
        pushResults(aggregate(pipelines[i]));
    }
}

testSequence({sequenceFunc: medTestSequence, encryptedSchema: medTestSchema});

MongoRunner.stopMongod(conn);
}());
