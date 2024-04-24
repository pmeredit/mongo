/**
 * Test queries against an encrypted collection after the data collection or esc/ecoc
 * collections are dropped.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_70,
 *   requires_fle2_in_always,
 *   requires_fastcount,
 *   assumes_no_implicit_collection_creation_after_drop,
 * ]
 * NOTE:
 * - requires_fle2_in_always - This test assumes that if the state collections are dropped, then
 * queries against encrypted fields can't find matching documents, encrypted collscan
 * succeeds without the state collections.
 * - assumes_no_implicit_collection_creation_after_drop - The test assumes that collections don't
 * exist after they are dropped.
 *
 */

import {
    EncryptedClient,
    kSafeContentField,
    runWithEncryption
} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = "findAfterCollDropDB";
const collName = "findAfterCollDropColl";
let coll;
let edb;

// Set up the encrypted collection, populating it with two documents and verifying the esc/ecoc
// collections have the expected documents.
const populateColl = () => {
    db.getSiblingDB(dbName).dropDatabase();
    let client = new EncryptedClient(db.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [
                {
                    "path": "secretString",
                    "bsonType": "string",
                    "queries": {"queryType": "equality"},
                },
            ]
        }
    }));

    edb = client.getDB();
    coll = edb[collName];

    assert.commandWorked(coll.einsert({_id: 1, secretString: "1337", nested: {foo: "bar"}}));
    assert.commandWorked(coll.einsert({_id: 2, secretString: "5", nested: {foo: "baz"}}));
    assert.eq(coll.find().count(), 2);

    client.assertEncryptedCollectionCounts(coll.getName(), 2, 2, 2);
    return client;
};

// Show that initially queries against encrypted fields work correctly.
populateColl();

runWithEncryption(edb, () => {
    assert.eq(coll.find({secretString: "5"}).count(), 1);

    // Drop the esc/ecoc collections and verify that there are no state collections remaining.
    const hasStateCollections = () => {
        let colls = assert.commandWorked(edb.runCommand({listCollections: 1})).cursor.firstBatch;
        for (let coll of colls) {
            if (coll.name.includes("esc") || coll.name.includes("ecoc")) {
                return true;
            }
        }
        return false;
    };

    assert(hasStateCollections());
    assert(edb.enxcol_[collName].esc.drop());
    assert(edb.enxcol_[collName].ecoc.drop());
    assert(!hasStateCollections());

    // Queries against non-encrypted fields still work.
    assert.eq(coll.find().count(), 2);
    assert.eq(coll.find({'nested.foo': 'bar'}).count(), 1);

    // However, queries against encrypted fields can't find matching documents. This makes sense,
    // since the server rewrites rely on the state collections to find matching documents.
    assert.eq(coll.find({secretString: "5"}).count(), 0);
});

// Reset all collections. Again, show that initial queries against encrypted fields work correctly.
let client = populateColl();
client.runEncryptionOperation(() => { assert.eq(coll.find({secretString: "5"}).count(), 1); });

// Now, drop JUST the data collection, before any of the state collections. Note there is a warning
// about this transmitted to the client (6491401).
assert(edb[collName].drop());

// Recreate the data collection.
assert.commandWorked(edb.createCollection(collName, {
    encryptedFields: {
        "fields": [
            {
                "path": "secretString",
                "bsonType": "string",
                "queries": {"queryType": "equality"},
                "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            },
        ]
    }
}));
coll = edb[collName];

// All queries correctly return zero documents, even though the state collections are stale.
assert.eq(coll.find().count(), 0);
assert.eq(coll.find({'nested.foo': 'bar'}).count(), 0);
assert.eq(coll.find({secretString: "5"}).count(), 0);
client.assertEncryptedCollectionCounts(coll.getName(), 0, 2, 2);

// Insert a new document. We should get correct results back, even if the state colls are stale.
const newDoc = {
    _id: 3,
    secretString: "1337",
    nested: {foo: "foo"}
};
assert.commandWorked(coll.einsert(newDoc));
client.assertEncryptedCollectionCounts(coll.getName(), 1, 3, 3);

assert.eq(coll.find().count(), 1);
assert.eq(coll.find({'nested.foo': 'bar'}).count(), 0);
assert.eq(coll.find({'nested.foo': 'foo'}).count(), 1);
assert.eq(coll.find({secretString: "5"}).count(), 0);

// Verify that the queries go through auto encryption and decryption.
client.runEncryptionOperation(() => {
    const res = coll.find({secretString: "1337"}, {[kSafeContentField]: 0}).toArray();
    assert.eq(res.length, 1, tojson(res));
    assert.eq(res[0], newDoc, tojson(res));
});

let explain = assert.commandWorked(edb.erunCommand({
    explain: {
        find: collName,
        filter: {secretString: "1337"},
    },
    verbosity: "queryPlanner"
}));

let parsedQuery;
if (explain.queryPlanner.winningPlan.shards) {
    parsedQuery = explain.queryPlanner.winningPlan.shards[0].parsedQuery;
} else {
    parsedQuery = explain.queryPlanner.parsedQuery;
}
assert(parsedQuery.hasOwnProperty(kSafeContentField), explain);
assert(!parsedQuery.hasOwnProperty("secretString"), explain);
