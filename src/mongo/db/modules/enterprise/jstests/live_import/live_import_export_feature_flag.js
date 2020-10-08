/**
 * Test effectiveness of turning off the feature flag for live import/export.
 */

(function() {
"use strict";

const standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=false"});
const testDB = standalone.getDB("test");
const adminDB = standalone.getDB("admin");

// exportCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(testDB.runCommand({exportCollection: "foo"}),
                             ErrorCodes.CommandNotSupported);

// importCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(testDB.runCommand({importCollection: "foo", collectionProperties: {}}),
                             ErrorCodes.CommandNotSupported);

const importUUID = UUID();
// voteCommitImportCollection command is not allowed when the feature flag is off.
assert.commandFailedWithCode(
    adminDB.runCommand(
        {voteCommitImportCollection: importUUID, from: "localhost:27017", dryRunSuccess: true}),
    ErrorCodes.CommandNotSupported);

MongoRunner.stopMongod(standalone);
}());
