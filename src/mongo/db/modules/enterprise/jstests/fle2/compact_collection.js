// Verify compact collection capability in client side

/**
 * @tags: [
 * assumes_read_concern_unchanged,
 * directly_against_shardsvrs_incompatible,
 * assumes_unsharded_collection,
 * assumes_read_preference_unchanged,
 * requires_fcv_71
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'compact_collection_db';
const collName = 'encrypted';
const ecocName = 'enxcol_.' + collName + '.ecoc';
const ecocCompactName = ecocName + '.compact';

const ecocExistsAfterCompact = true;

const sampleEncryptedFields = {
    fields: [
        {path: "first", bsonType: "string", queries: {"queryType": "equality", contention: 0}},
        {path: "ssn", bsonType: "string", queries: {"queryType": "equality", contention: 0}},
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
    client.assertEncryptedCollectionCounts(coll.getName(), 32, 32, 32);
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
    assert(res.stats.hasOwnProperty("ecoc"));
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test normal compaction of inserts (ESC) only");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // Compact each distinct value where no anchor is present yet
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 32, 6, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 16, 10);

    // Compact the latest insertions, but now with anchors present
    let expectedEsc = 8;
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 42, expectedEsc, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    expectedEsc += 10;
    client.assertEncryptedCollectionCounts(collName, 47, expectedEsc, 10);

    // Compact only squashes the 5 insertions for ssn
    expectedEsc -= 4;
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 47, expectedEsc, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact of 1:1 insert and deletes");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 200, 200);

    // squash inserts into 23 anchors for "first" and 11 for "ssn"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 100, 34, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // deleting all entries and compacting does not affect the ESC or ECOC.
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 34, 0);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 34, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // insert the same entries again, but don't compact before delete
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 234, 200);
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 234, 200);
    assert.commandWorked(coll.compact());
    let expectedEsc = 68;
    client.assertEncryptedCollectionCounts(collName, 0, expectedEsc, 0);
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
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5);

    // First compact should be no-op because the ecoc.compact is empty;
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5);
    // The current ecoc is never renamed; so must still exist after compact
    client.assertStateCollectionsAfterCompact(collName, true);

    // Second compact compacts "first"
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 0, 1, 0);
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
    client.assertEncryptedCollectionCounts(collName, 5, 10, 10);

    // Rename the ecoc collection to a enxcol_.encrypted.ecoc.compact, and recreate the ecoc
    // collection
    assert.commandWorked(db.adminCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    assert.commandWorked(edb.createCollection(ecocName));
    client.assertEncryptedCollectionCounts(collName, 5, 10, 0);

    // Insert non-unique values for "ssn"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"ssn": "987-98-9876"}));
    }
    client.assertEncryptedCollectionCounts(collName, 10, 15, 5);

    // (v2) First compact adds 1 anchor (for "first") and does not delete from ESC
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 16, 5);
    client.assertStateCollectionsAfterCompact(collName, true);

    // (v2) Second compact adds 1 anchor (for "ssn"), and deletes the 15 non-anchors
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 10, 2, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where ecoc.compact exists but ecoc does not");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    // Rename the ecoc collection to a enxcol_.encrypted.ecoc.compact
    assert.commandWorked(db.adminCommand(
        {renameCollection: dbName + "." + ecocName, to: dbName + "." + ecocCompactName}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0);

    // (v1) Compacts all ESC entries into a null document
    // (v2) Adds an anchor for "first", and does not delete from ESC
    let expectedEsc = 5 + 1;
    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, expectedEsc, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test compact where both ecoc & ecoc.compact do not exist");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    // Drop the ecoc collection
    edb[ecocName].drop();

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0);

    // if neither the ecoc or ecoc.compact exist, then compact does not create the ecoc
    client.assertStateCollectionsAfterCompact(collName, false);
});

jsTestLog("Test compact with missing compaction tokens");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const tokens = {
        first: HexData(0, "00".repeat(32)),
    };
    // empty compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: {}}),
        7294900);

    // incomplete compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: tokens}),
        7294900);
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
        9616300);
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

    assert.commandFailedWithCode(
        edb.runCommand({"compactStructuredEncryptionData": collName, compactionTokens: tokens}),
        7666502);

    client.assertEncryptedCollectionCounts(collName, 32, 32, 0);
    client.assertStateCollectionsAfterCompact(
        collName, ecocExistsAfterCompact, true /* ecocTempExists */);
});

jsTestLog("Test compact with max ESC deletes capped at 5 entries");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // Setting this parameter lowers the maximum number of ESC non-anchor documents
    // removed per compaction operation.
    const oldMemoryLimit =
        assert.commandWorked(edb.adminCommand({getClusterParameter: "fleCompactionOptions"}))
            .clusterParameters[0]
            .maxCompactionSize;
    assert.commandWorked(edb.adminCommand(
        {setClusterParameter: {fleCompactionOptions: {maxCompactionSize: NumberInt(32 * 5)}}}));

    // Compact each distinct value where no anchor is present yet
    assert.commandWorked(coll.compact());
    // 6 anchors inserted, and 5 non-anchors removed = +1 net change
    client.assertEncryptedCollectionCounts(collName, 32, 33, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 43, 10);

    // Compact the latest insertions, but now with anchors present
    assert.commandWorked(coll.compact());
    // 2 more anchors inserted, and 5 non-anchors removed = -3 net change
    client.assertEncryptedCollectionCounts(collName, 42, 40, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    client.assertEncryptedCollectionCounts(collName, 47, 50, 10);

    assert.commandWorked(coll.compact());
    // 6 anchors inserted, and 5 non-anchors removed = +1 net change
    client.assertEncryptedCollectionCounts(collName, 47, 51, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Restore the default memory limit
    assert.commandWorked(edb.adminCommand({
        setClusterParameter: {fleCompactionOptions: {maxCompactionSize: NumberInt(oldMemoryLimit)}}
    }));
});
