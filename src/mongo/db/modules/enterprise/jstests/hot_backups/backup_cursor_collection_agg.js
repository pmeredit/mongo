/**
 * Running a $backupCursor/$backupCursorExtend aggregation against a collection is prohibited.
 * Aggregations against a collection will lock that collection for reading and open a storage
 * transaction as part of lock-free reads.
 *
 * @tags: [requires_replication, requires_wiredtiger, requires_persistence]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "foo";

const db = rst.getPrimary().getDB(dbName);
assert.commandWorked(db.getCollection(collName).insert({}));

// Calling $backupCursor on a collection-less aggregation is the intended way.
const backupCursor = openBackupCursor(db);
backupCursor.close();

// Calling $backupCursor using an aggregation against a collection is prohibited. An aggregation
// against a collection will lock the collection for reading and open a storage transaction as part
// of lock-free reads.
try {
    db.getCollection(collName).aggregate([{$backupCursor: {}}]);
} catch (e) {
    assert(e.code == ErrorCodes.InvalidNamespace);
}

// The same restriction is held for $backupCursorExtend.
try {
    db.getCollection(collName).aggregate([{$backupCursorExtend: {}}]);
} catch (e) {
    assert(e.code == ErrorCodes.InvalidNamespace);
}

// Similarly referencing a collection in a later stage like $out is not allowed.
const foreignColectionStages = [
    {$out: collName},
    {$unionWith: collName},
    {$lookup: {from: collName, pipeline: [], as: "joined"}},
    {$merge: collName},
    {
        $graphLookup:
            {from: collName, startWith: "$a", connectFromField: "a", connectToField: "a", as: "a"}
    }
];
["$backupCursor", "$backupCursorExtend"].forEach((stage) => {
    foreignColectionStages.forEach((foreignReference) => {
        assert.throwsWithCode(() => db.aggregate([{[stage]: {}}, foreignReference]),
                              ErrorCodes.InvalidNamespace);
    });
});

rst.stopSet();