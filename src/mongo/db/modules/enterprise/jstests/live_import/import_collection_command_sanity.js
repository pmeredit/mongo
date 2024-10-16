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

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

// Get sample outputs of the exportCollection command for the tests.
const collectionProperties = exportCollection("test", "foo");
const systemProfileProperties = exportCollection("test", "system.profile");
const localCollectionProperties = exportCollection("local", "foo");

function testParsing(db) {
    // Invalid command parameter.
    assert.commandFailedWithCode(db.runCommand({importCollection: "foo"}), ErrorCodes.TypeMismatch);

    // Invalid 'force' field type.
    assert.commandFailedWithCode(
        db.runCommand({importCollection: collectionProperties, force: "bar"}),
        ErrorCodes.TypeMismatch);

    // Unknown field.
    assert.commandFailedWithCode(
        db.runCommand({importCollection: collectionProperties, foo: "unknown"}), 40415);
}

// Test standalone.
jsTestLog("Testing standalone");
let standalone = MongoRunner.runMongod();
let testDB = standalone.getDB("test");
testParsing(testDB);

// importCollection is not allowed on standalone.
assert.commandFailedWithCode(testDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.NoReplicationEnabled);
MongoRunner.stopMongod(standalone);

// Test replica set.
jsTestLog("Testing replica set");
const rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {auth: ""},
    keyFile: "jstests/libs/key1",
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
primaryAdmin.createUser({user: 'clusterAdmin', pwd: 'pass', roles: ['clusterAdmin']}, {w: 2});
primaryDB.createUser({user: 'dbOwner', pwd: 'pass', roles: ["dbOwner"]}, {w: 2});

testParsing(primaryDB);
testParsing(secondaryDB);

// importCollection is not allowed on secondary.
jsTestLog("Testing on secondary");
assert.commandFailedWithCode(secondaryDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.NotWritablePrimary);

// Importing unreplicated collection is not supported.
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: systemProfileProperties}),
                             ErrorCodes.CommandNotSupported);
assert.commandFailedWithCode(
    primary.getDB("local").runCommand({importCollection: localCollectionProperties}),
    ErrorCodes.CommandNotSupported);

assert(primaryAdmin.logout());
assert(secondaryAdmin.logout());

// importCollection should work with clusterAdmin access.
jsTestLog("Testing with clusterAdmin access");
assert(primaryAdmin.auth('clusterAdmin', 'pass'));

// Working cases.
assert.commandWorked(primaryDB.runCommand({importCollection: collectionProperties}));
assert.commandWorked(primaryDB.runCommand({importCollection: collectionProperties, force: false}));
assert.commandWorked(primaryDB.runCommand({importCollection: collectionProperties, force: true}));
assert(primaryAdmin.logout());

// importCollection is not allowed with dbOwner access.
jsTestLog("Testing with dbOwner access");
assert(primaryDB.auth('dbOwner', 'pass'));
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.Unauthorized);
assert(primaryDB.logout());

rst.stopSet();

jsTestLog("Testing sharded cluster");
const nodeOptions = {};
const st = new ShardingTest({
    shards: 1,
    rs: {nodes: 1},
    config: 1,
    other: {rsOptions: nodeOptions, configOptions: nodeOptions},
});
// There is no importCollection command on mongos.
assert.commandFailedWithCode(st.getDB("test").runCommand({importCollection: collectionProperties}),
                             ErrorCodes.CommandNotFound);
// importCollection is not supported on shard servers.
assert.commandFailedWithCode(
    st.shard0.getDB("test").runCommand({importCollection: collectionProperties}),
    ErrorCodes.CommandNotSupported);
// importCollection is not supported on config servers.
assert.commandFailedWithCode(
    st.config0.getDB("test").runCommand({importCollection: collectionProperties}),
    ErrorCodes.CommandNotSupported);
st.stop();