// Verify that compact + cleanup work correctly with anchor padding

/**
 * @tags: [
 * assumes_read_concern_unchanged,
 * assumes_read_preference_unchanged,
 * directly_against_shardsvrs_incompatible,
 * assumes_unsharded_collection,
 * requires_fcv_81
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    QEUint32RangeCollectionStatsTracker
} from "jstests/fle2/libs/qe_state_collection_stats_tracker.js";

const dbName = 'cleanup_collection_db';
const collName = 'encrypted';
const ecocName = 'enxcol_.' + collName + '.ecoc';

const INTMIN = NumberInt("-2147483648");
const INTMAX = NumberInt("2147483647");
const sampleEncryptedFields = {
    fields: [
        {
            path: "n",
            bsonType: "int",
            queries: {"queryType": "range", contention: 0, sparsity: 1, min: INTMIN, max: INTMAX}
        },
    ]
};

jsTestLog("Test alternating cleanup & compact cycles with padding");
runEncryptedTest(db, dbName, collName, sampleEncryptedFields, (edb, client) => {
    const coll = edb[collName];
    let nEdc = 0;

    const tracker = new QEUint32RangeCollectionStatsTracker();
    let paddingNonAnchors = 0;
    let paddingNullAnchors = 0;

    for (let cycle = 0; cycle < 20; cycle++) {
        // insert random documents
        for (let nInserts = 0; nInserts < 100; nInserts++) {
            let val = Math.floor(Math.random() * 100000);

            assert.commandWorked(coll.insert({n: NumberInt(val)}));

            tracker.updateStatsPostInsert("n", val);
            nEdc++;
        }
        let totals = tracker.calculateTotalStatsForFields("n");

        client.assertEncryptedCollectionCounts(
            collName, nEdc, totals.esc + paddingNonAnchors + paddingNullAnchors, totals.ecoc);
        client.assertESCNonAnchorCount(collName, totals.escNonAnchors);

        // compact on even cycles, cleanup on odd cycles
        if ((cycle % 2) == 0) {
            let res = assert.commandWorked(coll.compact({anchorPaddingFactor: 0.5}));

            print("Compact stats: " + tojson(res));
            // all of ECOC was read
            assert.eq(res.stats.ecoc.read, totals.ecoc);
            // compact never deletes from ECOC
            assert.eq(res.stats.ecoc.deleted, 0);
            // compact never updates ESC
            assert.eq(res.stats.esc.updated, 0);
            // # of ESC inserts is = to # of unique ecoc entries + # of padding non-anchors
            assert.gt(res.stats.esc.inserted - totals.ecocUnique, 0);
            // # of ESC deletes is the total # of pre-compact non-anchors
            assert.eq(res.stats.esc.deleted, totals.escNonAnchors + paddingNonAnchors);
            // # of ESC reads must be greater than # of non-anchors
            assert.gt(res.stats.esc.read, totals.escNonAnchors + paddingNonAnchors);
            paddingNonAnchors += res.stats.esc.inserted - totals.ecocUnique;
            tracker.updateStatsPostCompactForFields("n");
        } else {
            let res = assert.commandWorked(coll.cleanup());

            // If there is a padding null anchor already, we don't expect to do any inserts for
            // padding. If there isn't, we expect 1 insert for the new null anchor. Inversely, if
            // there is a padding null anchor, we expect it to be updated, but if there isn't, there
            // are no padding-related updates.
            const nPaddingInserts = 1 - paddingNullAnchors;
            const nPaddingUpdates = paddingNullAnchors;

            print("Cleanup stats: " + tojson(res));
            // all of ECOC was read
            assert.eq(res.stats.ecoc.read, totals.ecoc);
            // cleanup never deletes from ECOC
            assert.eq(res.stats.ecoc.deleted, 0);
            // # of ESC deletes is the total # of non-anchors + deletable anchors
            assert.eq(res.stats.esc.deleted,
                      totals.escNonAnchors + paddingNonAnchors + totals.escDeletableAnchors);
            // # of ESC inserts is the # of values that got a new null anchor
            assert.eq(res.stats.esc.inserted, totals.escFutureNullAnchors + nPaddingInserts);
            // # of ESC updates is the # of compacted values minus those with new null anchors
            assert.eq(res.stats.esc.updated,
                      totals.ecocUnique - totals.escFutureNullAnchors + nPaddingUpdates);
            // # of ESC reads must be greater than # of non-anchors
            assert.gt(res.stats.esc.read, totals.escNonAnchors + paddingNonAnchors);
            // All padding non-anchors should be gone, one anchor created
            paddingNonAnchors = 0;
            paddingNullAnchors = 1;

            tracker.updateStatsPostCleanupForFields("n");
        }

        client.assertStateCollectionsAfterCompact(collName, true);

        totals = tracker.calculateTotalStatsForFields("n");

        client.assertEncryptedCollectionCounts(
            collName, nEdc, totals.esc + paddingNonAnchors + paddingNullAnchors, totals.ecoc);
        client.assertESCNonAnchorCount(collName, 0);
    }
});
