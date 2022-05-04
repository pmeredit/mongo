// Verify compact collection capability in client side

/**
 * @tags: [
 * requires_fcv_60,
 * assumes_read_concern_unchanged,
 * directly_against_shardsvrs_incompatible,
 * assumes_unsharded_collection
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'compact_collection_db';
const collName = 'encrypted';
const ecocName = 'enxcol_.' + collName + '.ecoc';
const ecocCompactName = ecocName + '.compact';
const ecocExistsAfterCompact = false;

const sampleEncryptedFields = {
    fields: [
        {path: "first", bsonType: "string", queries: {"queryType": "equality"}},
        {path: "ssn", bsonType: "string", queries: {"queryType": "equality"}},
    ]
};

function insertInitialTestData(client, coll) {
    // Populate the EDC with sample data
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger", "alias": "rog", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "roderick", "alias": "rod", "ctr": i}));
    }
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({"first": "ruben", "alias": "ben", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "reginald", "alias": "reg", "ctr": i}));
    }
    assert.commandWorked(coll.insert({"first": "rudolf", "alias": "rudy", "ctr": 1}));
    assert.commandWorked(coll.insert({"first": "brian", "alias": "bri", "ctr": 1}));
    client.assertEncryptedCollectionCounts(coll.getName(), 32, 32, 0, 32);
}

jsTestLog("Test compact on unencrypted collection fails");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    assert.commandWorked(edb.createCollection("unencrypted"));
    assert.commandFailedWithCode(edb.unencrypted.compact(), 6346807);
});

jsTestLog("Test compact on empty encrypted collection");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const res = edb[collName].compact();
    assert.commandWorked(res);
    assert(res.hasOwnProperty("stats"));
    assert(res.stats.hasOwnProperty("esc"));
    assert(res.stats.hasOwnProperty("ecc"));
    assert(res.stats.hasOwnProperty("ecoc"));
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test normal compaction of inserts (ESC) only");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // Compact each distinct value where no null doc is present yet
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 32, 6, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 16, 0, 10);

    // Compact the latest insertions, but now with null doc present
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 42, 6, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    client.assertEncryptedCollectionCounts(collName, 47, 16, 0, 10);

    // Compact only squashes the 5 insertions for ssn
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 47, 12, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test normal compaction of deletes");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // delete 5 out of 10 'reg' (entries 1-5, in-order)
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.deleteOne({"alias": "reg", "ctr": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 27, 32, 5, 37);

    // ECC entries squashed as the deletes are all adjacent; null doc added
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 27, 6, 2, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // delete the even numbered 'ben's
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.deleteOne({"alias": "ben", "ctr": i * 2}));
    }
    client.assertEncryptedCollectionCounts(collName, 22, 6, 7, 5);

    // no ECC entries squashed as no adjacent deletes to merge; no null doc added
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 22, 6, 7, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // delete the odd numbered 'ben's except 1
    for (let i = 3; i < 10; i += 2) {
        assert.commandWorked(coll.deleteOne({"alias": "ben", "ctr": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 18, 6, 11, 4);

    // ECC entries 2-10 for 'ben' are merged to a single entry; null doc added
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 18, 6, 4, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact of 1:1 insert and deletes");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 200, 0, 200);

    // squash inserts into 23 null docs for "first" and 11 for "ssn"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 100, 34, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // deleting all entries and compacting leaves only a null doc and a single
    // compacted entry per unique pair in ECC.
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 34, 200, 200);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 34, 68, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // insert the same entries again, but don't compact before delete
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 234, 68, 200);
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 234, 268, 400);
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 34, 68, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where ecoc and ecoc.compact both exist and ecoc.compact is empty");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Pre-create the renamed ecoc collection, so that both enxcol_.encrypted.ecoc and
    // enxcol_.encrypted.ecoc.compact exist before compaction
    assert.commandWorked(edb.createCollection(ecocCompactName));

    // Insert & delete some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas", "ctr": i}));
        assert.commandWorked(coll.deleteOne({"ctr": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5, 10);

    // First compact should be no-op because the ecoc.compact is empty;
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5, 10);
    // The current ecoc is never renamed; so must still exist after compact
    client.assertStateCollectionsAfterCompact(collName, true);

    // Second compact compacts "first"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 1, 2, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where ecoc and ecoc.compact both exist and ecoc.compact is non-empty");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"; then delete 1-5
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({"first": "silas", "ctr": i}));
        if (i <= 5) {
            assert.commandWorked(coll.deleteOne({"ctr": i}));
        }
    }
    client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 15);

    // Rename the ecoc collection to a enxcol_.encrypted.ecoc.compact, and recreate the ecoc
    // collection
    assert.commandWorked(db.adminCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    assert.commandWorked(edb.createCollection(ecocName));
    client.assertEncryptedCollectionCounts(collName, 5, 10, 5, 0);

    // Insert non-unique values for "ssn"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"ssn": "987-98-9876"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 15, 5, 5);

    // First compact should only compact values inserted/deleted for "first"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 6, 2, 5);
    client.assertStateCollectionsAfterCompact(collName, true);

    // Second compact compacts the values inserted for "ssn"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 2, 2, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where ecoc.compact exists but ecoc does not");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);

    // Rename the ecoc collection to a enxcol_.encrypted.ecoc.compact
    assert.commandWorked(db.adminCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 0);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 1, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where both ecoc & ecoc.compact do not exist");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);

    // Drop the ecoc collection
    edb[ecocName].drop();

    // Compact doesn't compact, but still creates the ECOC (unless sharded)
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact with missing compaction tokens");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const tokens = {
        first: HexData(0, "00".repeat(32)),
    };
    // empty compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: {}}),
        6346806);

    // incomplete compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: tokens}),
        6346806);
});

jsTestLog("Test compact with malformed compaction tokens");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const badTokens1 = {
        first: "token",
        ssn: "token",
    };
    const badTokens2 = {
        first: HexData(6, "060102030405060708091011121314151602"),
        ssn: HexData(6, "060102030405060708091011121314151602"),
    };
    const badTokens3 = {
        first: HexData(0, "060102030405060708091011121314151602"),
        ssn: HexData(0, "060102030405060708091011121314151602"),
    };
    // token is not bindata
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: badTokens1}),
        6346801);
    // token has wrong bindata type
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: badTokens2}),
        6346801);
    // token has wrong bindata length
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: badTokens3}),
        6373501);
});

jsTestLog("Test compact with wrong compaction tokens");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    const bogusToken = HexData(0, "00".repeat(32));
    const tokens = {
        first: bogusToken,
        ssn: bogusToken,
        extra: bogusToken,
    };
    insertInitialTestData(client, coll);

    // compact still succeeds, but no ESC/ECC entries are compacted, and ECOC is still dropped
    assert.commandWorked(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: tokens}));
    client.assertEncryptedCollectionCounts(collName, 32, 32, 0, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});
}());
