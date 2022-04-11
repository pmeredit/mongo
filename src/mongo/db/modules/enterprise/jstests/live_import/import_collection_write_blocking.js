/**
 * Ensures that if write blocking mode is enabled, users can import collection only if they can
 * bypass write blocking.
 *
 * @tags: [
 *   creates_and_authenticates_user,
 *   requires_auth,
 *   requires_fcv_60,
 *   requires_non_retryable_commands,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");
load("jstests/noPassthrough/libs/user_write_blocking.js");  // For UserWriteBlockHelpers

const dbName = "test";
const collName = "foo";

// Create some data to be copied and imported for the test.
const collectionProperties = exportCollection(dbName, collName);

function testImportCollectionSuccess(asAuthenticated, setupFixture) {
    // Make a new fixture, copying files to import.
    const fixture = new UserWriteBlockHelpers.ReplicaFixture();
    if (setupFixture !== undefined) {
        setupFixture(fixture);
    }
    fixture.getAllDbPaths().forEach(path => copyFilesForExport(collectionProperties, path));

    asAuthenticated(fixture, ({conn}) => {
        const db = conn.getDB(dbName);
        // Ensure importCollection succeeds and the correct data is imported
        assert.commandWorked(db.runCommand({importCollection: collectionProperties}));
        assert(Array.contains(db.getCollectionNames(), collName));
        validateImportCollection(db[collName], collectionProperties);
    });

    fixture.stop();
}

function testImportCollectionFailure(asAuthenticated, setupFixture) {
    // Make a new fixture, copying files to import.
    const fixture = new UserWriteBlockHelpers.ReplicaFixture();
    if (setupFixture !== undefined) {
        setupFixture(fixture);
    }
    fixture.getAllDbPaths().forEach(path => copyFilesForExport(collectionProperties, path));

    asAuthenticated(fixture, ({conn}) => {
        const db = conn.getDB(dbName);
        // Ensure importCollection fails and no data is imported
        assert.commandFailedWithCode(db.runCommand({importCollection: collectionProperties}),
                                     ErrorCodes.UserWritesBlocked);
        assert(!Array.contains(db.getCollectionNames(), collName));
    });

    fixture.stop();
}

function test() {
    const runAsAdmin = (fixture, fn) => fixture.asAdmin(fn);
    const runAsUser = (fixture, fn) => fixture.asUser(fn);

    // Test that by default, write block mode is disabled and thus importCollection will succeed for
    // both users
    testImportCollectionSuccess(runAsAdmin);
    testImportCollectionSuccess(runAsUser);

    // Test that when write block mode is enabled, importCollection will succeed for the bypass
    // user, but will fail for the non-bypass user
    const setupFixture = fixture => fixture.enableWriteBlockMode();
    testImportCollectionSuccess(runAsAdmin, setupFixture);
    testImportCollectionFailure(runAsUser, setupFixture);
}

test();
}());
