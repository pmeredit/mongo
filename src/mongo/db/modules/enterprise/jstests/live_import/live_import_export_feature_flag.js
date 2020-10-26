/**
 * Test effectiveness of turning off the feature flag for live import/export.
 *
 *  @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

// Get a sample collectionProperties for the test.
const collectionProperties = exportCollection("test", "foo");

const standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=false"});
const testDB = standalone.getDB("test");
const adminDB = standalone.getDB("admin");

// exportCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(testDB.runCommand({exportCollection: "foo"}),
                             ErrorCodes.CommandNotSupported);

// importCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(testDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.CommandNotSupported);

const importUUID = UUID();
// voteCommitImportCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(
    adminDB.runCommand(
        {voteCommitImportCollection: importUUID, from: "localhost:27017", dryRunSuccess: true}),
    ErrorCodes.CommandNotSupported);

MongoRunner.stopMongod(standalone);
}());
