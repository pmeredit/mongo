/**
 * Test sanity of the importCollection command.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_helpers.js");

function testParsing(db) {
    // Missing 'collectionProperties' field.
    assert.commandFailedWithCode(db.runCommand({importCollection: "foo"}), 40414);

    // Invalid collection name.
    assert.commandFailedWithCode(db.runCommand({importCollection: 1, collectionProperties: {}}),
                                 ErrorCodes.BadValue);

    // Invalid 'collectionProperties' field type.
    assert.commandFailedWithCode(
        db.runCommand({importCollection: "foo", collectionProperties: "bar"}),
        ErrorCodes.TypeMismatch);

    // Invalid 'force' field type.
    assert.commandFailedWithCode(
        db.runCommand({importCollection: "foo", collectionProperties: {}, force: "bar"}),
        ErrorCodes.TypeMismatch);

    // Unknown field.
    assert.commandFailedWithCode(
        db.runCommand({importCollection: "foo", collectionProperties: {}, foo: "unknown"}), 40415);
}

// Test standalone.
jsTestLog("Testing standalone");
let standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=true"});
let testDB = standalone.getDB("test");
testParsing(testDB);

// importCollection is not allowed on standalone.
assert.commandFailedWithCode(testDB.runCommand({importCollection: "foo", collectionProperties: {}}),
                             ErrorCodes.NoReplicationEnabled);
MongoRunner.stopMongod(standalone);

// Get a sample output of the exportCollection command for the replica set test.
const collectionProperties = exportEmptyCollectionFromStandalone("test", "foo");

// Test replica set.
jsTestLog("Testing replica set");
const rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {auth: "", setParameter: "featureFlagLiveImportExport=true"},
    keyFile: "jstests/libs/key1"
});
rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryAdmin = primary.getDB('admin');
const secondaryAdmin = secondary.getDB('admin');

// Setup initial users.
primaryAdmin.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles}, {w: 2});
assert(primaryAdmin.auth('admin', 'pass'));
assert(secondaryAdmin.auth('admin', 'pass'));

// Noop the importCollection command because we don't need to actually import the collection for
// this sanity test.
assert.commandWorked(
    primaryAdmin.runCommand({configureFailPoint: "noopImportCollectionCommand", mode: "alwaysOn"}));
assert.commandWorked(secondaryAdmin.runCommand(
    {configureFailPoint: "noopImportCollectionCommand", mode: "alwaysOn"}));

const primaryDB = primary.getDB("test");
const secondaryDB = secondary.getDB("test");
primaryDB.createUser({user: 'rw', pwd: 'pass', roles: jsTest.basicUserRoles}, {w: 2});
primaryDB.createUser({user: 'r', pwd: 'pass', roles: jsTest.readOnlyUserRoles}, {w: 2});

testParsing(primaryDB);
testParsing(secondaryDB);

// importCollection is not allowed on secondary.
jsTestLog("Testing on secondary");
assert.commandFailedWithCode(
    secondaryDB.runCommand({importCollection: "foo", collectionProperties: {}}),
    ErrorCodes.NotWritablePrimary);

assert(primaryAdmin.logout());
assert(secondaryAdmin.logout());

// importCollection should work with readWrite access.
jsTestLog("Testing with readWrite access");
assert(primaryDB.auth('rw', 'pass'));

// Namespace in the command doesn't match namespace in collectionProperties.
assert.commandFailedWithCode(
    primaryDB.runCommand({importCollection: "bar", collectionProperties: collectionProperties}),
    ErrorCodes.BadValue);

// Working cases.
assert.commandWorked(
    primaryDB.runCommand({importCollection: "foo", collectionProperties: collectionProperties}));
assert.commandWorked(primaryDB.runCommand(
    {importCollection: "foo", collectionProperties: collectionProperties, force: false}));
assert.commandWorked(primaryDB.runCommand(
    {importCollection: "foo", collectionProperties: collectionProperties, force: true}));
assert(primaryDB.logout());

// importCollection is not allowed with read-only access.
jsTestLog("Testing with read-only access");
assert(primaryDB.auth('r', 'pass'));
assert.commandFailedWithCode(
    primaryDB.runCommand({importCollection: "foo", collectionProperties: {}}),
    ErrorCodes.Unauthorized);
assert(primaryDB.logout());

rst.stopSet();
}());
