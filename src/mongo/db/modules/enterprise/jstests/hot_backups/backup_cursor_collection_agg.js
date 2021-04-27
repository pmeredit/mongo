/**
 * Running a $backupCursor/$backupCursorExtend aggregation against a collection is prohibited.
 * Aggregations against a collection will lock that collection for reading and open a storage
 * transaction as part of lock-free reads.
 *
 * @tags: [requires_replication, requires_wiredtiger]
 */
(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "foo";

const db = rst.getPrimary().getDB(dbName);
assert.commandWorked(db.getCollection(collName).insert({}));

// Calling $backupCursor on a collection-less aggregation is the intended way.
let cursor = db.aggregate([{$backupCursor: {}}]);
cursor.close();

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

rst.stopSet();
}());
