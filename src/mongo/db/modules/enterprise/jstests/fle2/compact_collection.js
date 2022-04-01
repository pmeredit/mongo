// Verify compact collection capability in client side

/**
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2ReplicationEnabled()) {
    return;
}

const dbName = 'compact_collection_db';
const collName = 'encrypted';
const ecocName = 'fle2.' + collName + '.ecoc';
const ecocCompactName = ecocName + '.compact';

const admin = db.getMongo().getDB("admin");

const sampleEncryptedFields = {
    fields: [
        {path: "first", bsonType: "string", queries: {"queryType": "equality"}},
        {path: "ssn", bsonType: "string", queries: {"queryType": "equality"}},
    ]
};

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
    client.assertStateCollectionsAfterCompact(collName);
});

function insertInitialTestData(client, coll) {
    // Populate the EDC with sample data
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "roderick", "ctr": i}));
    }
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.insert({"first": "ruben", "ctr": i}));
        assert.commandWorked(coll.insert({"first": "reginald", "ctr": i}));
    }
    assert.commandWorked(coll.insert({"first": "rudolf", "ctr": 1}));
    assert.commandWorked(coll.insert({"first": "brian", "ctr": 1}));
    client.assertEncryptedCollectionCounts("encrypted", 32, 32, 0, 32);
}

jsTestLog("Test normal compaction of inserts (ESC) only");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // Compact each distinct value where no null doc is present yet
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 32, 6, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 16, 0, 10);

    // Compact the latest insertions, but now with null doc present
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 42, 6, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    client.assertEncryptedCollectionCounts(collName, 47, 16, 0, 10);

    // Compact only squashes the 5 insertions for ssn
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 47, 12, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);
});

jsTestLog("Test compact where ecoc and ecoc.compact both exist and ecoc.compact is empty");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Pre-create the renamed ecoc collection, so that both fle2.encrypted.ecoc and
    // fle2.encrypted.ecoc.compact exist before compaction
    assert.commandWorked(edb.createCollection(ecocCompactName));

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);

    // First compact should be no-op because the ecoc.compact is empty
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);
    client.assertStateCollectionsAfterCompact(collName);

    // Second compact compacts "first"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 1, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);
});

jsTestLog("Test compact where ecoc and ecoc.compact both exist and ecoc.compact is non-empty");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);

    // Rename the ecoc collection to a fle2.encrypted.ecoc.compact, and recreate the ecoc collection
    assert.commandWorked(admin.runCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    assert.commandWorked(edb.createCollection(ecocName));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 0);

    // Insert non-unique values for "ssn"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"ssn": "987-98-9876"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 10, 0, 5);

    // First compact should only compact values inserted for "first"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 6, 0, 5);
    client.assertStateCollectionsAfterCompact(collName);

    // Second compact compacts the values inserted for "ssn"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 2, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);
});

jsTestLog("Test compact where ecoc.compact exists but ecoc does not");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 5);

    // Rename the ecoc collection to a fle2.encrypted.ecoc.compact
    assert.commandWorked(admin.runCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 0);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 1, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);
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

    // Compact doesn't compact, but still creates the ECOC
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0, 0);
    client.assertStateCollectionsAfterCompact(collName);
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
    };  // token is not bindata
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
});
}());
