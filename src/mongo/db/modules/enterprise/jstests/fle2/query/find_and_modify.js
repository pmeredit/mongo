/**
 * Test encrypted find and modify correctly rewrites the filter portion.
 *
 * @tags: [
 * ]
 */
(function() {
'use strict';

load("jstests/fle2/libs/encrypted_client_util.js");

const dbName = 'query_find_and_modify';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const collName = "test";

const testCases = [
    {
        // Querying on a top-level encrypted field.
        command: {
            findAndModify: collName,
            query: {secretString: "1337"},
            update: {$set: {is1337: true}},
        }
    },
    {
        // Querying on a nested encrypted field.
        command: {
            findAndModify: collName,
            query: {'nested.secretInt': NumberInt(5)},
            update: {$set: {isNestedFive: true}}
        }
    },
    {
        // Querying on a nested encrypted field.
        command: {
            findAndModify: collName,
            query: {'nested.secretInt': NumberInt(5)},
            update: {$set: {isNestedFive: true}}
        },
        after: () => {
            client.assertEncryptedCollectionDocuments(collName, [
                {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}, is1337: true},
                {_id: 2, secretString: "5", nested: {secretInt: NumberInt(5)}, isNestedFive: true}
            ]);
        },
    },
    {
        // Query over both a top level and nested encrypted field.
        command: {
            findAndModify: collName,
            query: {secretString: "5", 'nested.secretInt': NumberInt(5)},
            update: {$set: {bothFive: true}}
        }
    },
    {
        // Query over an encrypted field which matches no documents.
        command: {
            findAndModify: collName,
            query: {secretString: "6"},
            update: {_id: 3, secretString: "6", nested: {secretInt: NumberInt(6)}},
            upsert: true
        },
        after: () => {
            client.assertEncryptedCollectionDocuments(collName, [
                {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}, is1337: true},
                {
                    _id: 2,
                    secretString: "5",
                    nested: {secretInt: NumberInt(5)},
                    isNestedFive: true,
                    bothFive: true
                },
                {_id: 3, secretString: "6", nested: {secretInt: NumberInt(6)}}
            ]);
        }
    },
    {
        // Query over one encrypted field and $unset another.
        command: {
            findAndModify: collName,
            query: {secretString: "5"},
            update: {$unset: {'nested.secretInt': 1}}
        },
        after: () => {
            assert.eq([{_id: 2, secretString: "5", nested: {}, isNestedFive: true, bothFive: true}],
                      coll.find({_id: 2}, {[kSafeContentField]: 0}).toArray());
        }
    },
    {
        // Verify that a user can specify a writeConcern without failing within the
        // internally-created transaction. This is expected to fail if the command is running in
        // a transaction created by the user.
        command: {
            findAndModify: collName,
            query: {secretString: "6"},
            update: {$set: {isSix: true}},
            writeConcern: {w: 2},
        },
        skipIfUserTxn: true
    }
];

let coll;

const populateColl = () => {
    db.getSiblingDB(dbName).dropDatabase();
    let client = new EncryptedClient(db.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [
                {
                    "path": "secretString",
                    "bsonType": "string",
                    "queries": {"queryType": "equality"}
                },
                {
                    "path": "nested.secretInt",
                    "bsonType": "int",
                    "queries": {"queryType": "equality"}
                }
            ]
        }
    }));

    coll = client.getDB()[collName];

    assert.commandWorked(
        coll.insert({_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}}));
    assert.commandWorked(
        coll.insert({_id: 2, secretString: "5", nested: {secretInt: NumberInt(5)}}));

    const docs = coll.find().toArray();
    assert(docs.length == 2 && docs[0].hasOwnProperty(kSafeContentField));

    client.assertEncryptedCollectionCounts(coll.getName(), 2, 4, 0, 4);
    return client;
};

// Run all of the tests.
let client = populateColl();

for (const test of testCases) {
    assert.commandWorked(coll.runCommand(test.command), tojson(test.command));

    if (test.after) {
        test.after();
    }
}

// Run the same tests, this time in a transaction.
client = populateColl();
let edb = client.getDB();
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
coll = sessionDB.getCollection(collName);
for (const test of testCases) {
    if (test.skipIfUserTxn) {
        continue;
    }

    session.startTransaction();
    assert.commandWorked(coll.runCommand(test.command), tojson(test.command));
    session.commitTransaction();

    if (test.after) {
        test.after();
    }
}
}());
