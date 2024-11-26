// Verify cleanup collection capability in client side

/**
 * @tags: [
 * assumes_read_concern_unchanged,
 * assumes_read_preference_unchanged,
 * directly_against_shardsvrs_incompatible,
 * assumes_unsharded_collection,
 * requires_fcv_71
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    QEStateCollectionStatsTracker
} from "jstests/fle2/libs/qe_state_collection_stats_tracker.js";

const dbName = 'cleanup_collection_db';
const collName = 'encrypted';
const ecocName = 'enxcol_.' + collName + '.ecoc';
const ecocCompactName = ecocName + ".compact";

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

jsTestLog("Test cleanup on non-existent encrypted collection fails");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    edb[collName].drop();
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: {}}),
        ErrorCodes.NamespaceNotFound);
});

jsTestLog("Test cleanup on view fails");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    edb.createCollection("testview", {viewOn: collName});
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": "testview", cleanupTokens: {}}),
        ErrorCodes.CommandNotSupportedOnView);
});

jsTestLog("Test cleanup on unencrypted collection fails");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    assert.commandWorked(edb.createCollection("unencrypted"));
    assert.commandFailedWithCode(edb.unencrypted.cleanup(), 6346807);
});

jsTestLog("Test cleanup on empty encrypted collection");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const res = edb[collName].cleanup();
    assert.commandWorked(res);
    assert(res.hasOwnProperty("stats"));
    assert(res.stats.hasOwnProperty("esc"));
    assert(res.stats.hasOwnProperty("ecoc"));
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test normal cleanup of inserts (ESC) only");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    insertInitialTestData(client, coll);

    // Cleanup each distinct value where no null anchor is present yet
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 32, 6, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 16, 10);

    // Cleanup the latest insertions, but now with null anchors present
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 42, 6, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    client.assertEncryptedCollectionCounts(collName, 47, 16, 10);

    // Cleanup only squashes the 5 insertions for ssn
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 47, 12, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test cleanup of 1:1 insert and deletes");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 200, 200);

    // squash inserts into 23 null docs for "first" and 11 for "ssn"
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 100, 34, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // deleting all entries and cleaning up does not affect the ESC or ECOC.
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 34, 0);

    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 0, 34, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // insert the same entries again, but don't cleanup before delete
    for (let i = 1; i <= 100; i++) {
        assert.commandWorked(
            coll.insert({"first": "bob_" + (i % 23), "ssn": "222-23-212" + (i % 11), "_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 100, 234, 200);
    for (let i = 100; i > 0; i--) {
        assert.commandWorked(coll.deleteOne({"_id": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 234, 200);
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 0, 34, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test alternating cleanup & compact cycles");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    const values = [];
    const getRandomValue = function(list) {
        let idx = Math.floor(Math.random() * list.length);
        return list[idx];
    };

    let nEdc = 0;

    const tracker = new QEStateCollectionStatsTracker();

    for (let cycle = 0; cycle < 20; cycle++) {
        // add more unique values to the selection
        for (let i = 0; i < 10; i++) {
            values.push("value_" + (i + values.length));
        }

        let firstValue;
        let ssnValue;
        let inserts = [];

        // generate documents to insert, then bulk insert
        for (let nInserts = 0; nInserts < 100; nInserts++) {
            firstValue = getRandomValue(values);
            ssnValue = getRandomValue(values);

            inserts.push({first: firstValue, ssn: ssnValue});

            tracker.updateStatsPostInsert("first", firstValue);
            tracker.updateStatsPostInsert("ssn", ssnValue);
            nEdc++;
        }
        assert.commandWorked(coll.insertMany(inserts, {ordered: false}));

        let totals = tracker.calculateTotalStatsForFields("first", "ssn");

        client.assertEncryptedCollectionCounts(collName, nEdc, totals.esc, totals.ecoc);
        client.assertESCNonAnchorCount(collName, totals.escNonAnchors);

        // compact on even cycles, cleanup on odd cycles
        if ((cycle % 2) == 0) {
            let res = assert.commandWorked(coll.compact());

            print("Compact stats: " + tojson(res));
            // all of ECOC was read
            assert.eq(res.stats.ecoc.read, totals.ecoc);
            // compact never deletes from ECOC
            assert.eq(res.stats.ecoc.deleted, 0);
            // compact never updates ESC
            assert.eq(res.stats.esc.updated, 0);
            // # of ESC inserts is equal to # of unique ecoc entries
            assert.eq(res.stats.esc.inserted, totals.ecocUnique);
            // # of ESC deletes is the total # of non-anchors
            assert.eq(res.stats.esc.deleted, totals.escNonAnchors);
            // # of ESC reads must be greater than # of non-anchors
            assert.gt(res.stats.esc.read, totals.escNonAnchors);

            tracker.updateStatsPostCompactForFields("first", "ssn");
        } else {
            let res = assert.commandWorked(coll.cleanup());

            print("Cleanup stats: " + tojson(res));
            // all of ECOC was read
            assert.eq(res.stats.ecoc.read, totals.ecoc);
            // cleanup never deletes from ECOC
            assert.eq(res.stats.ecoc.deleted, 0);
            // # of ESC deletes is the total # of non-anchors + deletable anchors
            assert.eq(res.stats.esc.deleted, totals.escNonAnchors + totals.escDeletableAnchors);
            // # of ESC inserts is the # of values that got a new null anchor
            assert.eq(res.stats.esc.inserted, totals.escFutureNullAnchors);
            // # of ESC updates is the # of compacted values minus those with new null anchors
            assert.eq(res.stats.esc.updated, totals.ecocUnique - totals.escFutureNullAnchors);
            // # of ESC reads must be greater than # of non-anchors
            assert.gt(res.stats.esc.read, totals.escNonAnchors);

            tracker.updateStatsPostCleanupForFields("first", "ssn");
        }

        client.assertStateCollectionsAfterCompact(collName, true);

        totals = tracker.calculateTotalStatsForFields("first", "ssn");

        client.assertEncryptedCollectionCounts(collName, nEdc, totals.esc, totals.ecoc);
        client.assertESCNonAnchorCount(collName, 0);
    }
});

jsTestLog("Test cleanup where ecoc and ecoc.compact both exist and ecoc.compact is empty");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Pre-create the renamed ecoc collection, so that both enxcol_.encrypted.ecoc and
    // enxcol_.encrypted.ecoc.compact exist before cleanup
    assert.commandWorked(edb.createCollection(ecocCompactName));

    // Insert & delete some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas", "ctr": i}));
        assert.commandWorked(coll.deleteOne({"ctr": i}));
    }
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5);

    // First cleanup should be no-op because the ecoc.compact is empty;
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 0, 5, 5);
    // The current ecoc is never renamed; so must still exist after cleanup
    client.assertStateCollectionsAfterCompact(collName, true);

    // Second cleanup cleans up "first"
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 0, 1, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test cleanup where ecoc and ecoc.compact both exist and ecoc.compact is non-empty");
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

    // First cleanup adds 1 null anchor for "first" and does not delete from ESC
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 10, 16, 5);
    client.assertStateCollectionsAfterCompact(collName, true);

    // Second cleanup adds 1 null anchor for "ssn" and deletes the 15 non-anchors
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 10, 2, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test cleanup where ecoc.compact exists but ecoc does not");
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

    // Cleanup adds a null anchor for "first" and does not delete from ESC
    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 5, 6, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);
});

jsTestLog("Test cleanup where both ecoc & ecoc.compact do not exist");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Insert some non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "silas"}));
    }
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    // Drop the ecoc collection
    edb[ecocName].drop();

    assert.commandWorked(coll.cleanup());
    client.assertEncryptedCollectionCounts(collName, 5, 5, 0);

    // if neither the ecoc or ecoc.compact exist, then cleanup does not create the ecoc
    client.assertStateCollectionsAfterCompact(collName, false);
});

jsTestLog("Test cleanup with missing cleanup tokens");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const tokens = {
        first: HexData(0, "00".repeat(32)),
    };
    // empty compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: {}}), 7294900);

    // incomplete compaction tokens
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: tokens}),
        7294900);
});

jsTestLog("Test cleanup with malformed cleanup tokens");
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
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: badTokens1}),
        6346801);
    // token has wrong bindata type
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: badTokens2}),
        6346801);
    // token has wrong bindata length
    assert.commandFailedWithCode(
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: badTokens3}),
        6373501);
});

jsTestLog("Test cleanup with wrong cleanup tokens");
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
        edb.runCommand({"cleanupStructuredEncryptionData": collName, cleanupTokens: tokens}),
        7618816);

    client.assertStateCollectionsAfterCompact(
        collName, ecocExistsAfterCompact, true /* ecocTempExists */);
});

jsTestLog("Test cleanup with max ESC deletes capped at 5 entries");
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

    // Cleanup each distinct value where no anchor is present yet
    assert.commandWorked(coll.cleanup());
    // 6 null anchors inserted, and 5 non-anchors removed = +1 net change
    client.assertEncryptedCollectionCounts(collName, 32, 33, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more non-unique values for "first"
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "roger"}));
        assert.commandWorked(coll.insert({"first": "roderick"}));
    }
    client.assertEncryptedCollectionCounts(collName, 42, 43, 10);

    // Cleanup the latest insertions, but now with null anchors present
    assert.commandWorked(coll.cleanup());
    // No new anchors inserted, and 5 non-anchors removed = -5 net change
    client.assertEncryptedCollectionCounts(collName, 42, 38, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Insert more unique values for "first", all with similar value for ssn
    for (let i = 1; i <= 5; i++) {
        assert.commandWorked(coll.insert({"first": "rufus_" + i, "ssn": "123-12-1234"}));
    }
    client.assertEncryptedCollectionCounts(collName, 47, 48, 10);

    assert.commandWorked(coll.cleanup());
    // 6 new null anchors inserted, and 5 non-anchors removed = +1 net change
    client.assertEncryptedCollectionCounts(collName, 47, 49, 0);
    client.assertStateCollectionsAfterCompact(collName, ecocExistsAfterCompact);

    // Restore the default memory limit
    assert.commandWorked(edb.adminCommand({
        setClusterParameter: {fleCompactionOptions: {maxCompactionSize: NumberInt(oldMemoryLimit)}}
    }));
});

jsTestLog("Test cleanup with max ESC anchor deletes capped at 5 entries");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];

    // Run 20 insert & compact cycles to fill the ESC with 20 anchors
    for (let i = 0; i < 20; i++) {
        assert.commandWorked(coll.insert({first: "frodo"}));
        assert.commandWorked(coll.compact());
    }
    // insert one more so there's an ECOC entry
    assert.commandWorked(coll.insert({first: "frodo"}));
    client.assertEncryptedCollectionCounts(collName, 21, 21, 1);
    client.assertESCNonAnchorCount(collName, 1);

    const oldPQMemoryLimit =
        assert.commandWorked(edb.adminCommand({getClusterParameter: "fleCompactionOptions"}))
            .clusterParameters[0]
            .maxAnchorCompactionSize;
    assert.commandWorked(edb.adminCommand({
        setClusterParameter: {fleCompactionOptions: {maxAnchorCompactionSize: NumberInt(32 * 5)}}
    }));

    assert.commandWorked(coll.cleanup());
    // 5 anchors removed + 1 non-anchor removed + 1 null anchor inserted
    client.assertEncryptedCollectionCounts(collName, 21, 16, 0);
    client.assertESCNonAnchorCount(collName, 0);

    // Restore the default memory limit
    assert.commandWorked(edb.adminCommand({
        setClusterParameter:
            {fleCompactionOptions: {maxAnchorCompactionSize: NumberInt(oldPQMemoryLimit)}}
    }));
});
