/**
 * Test sanity of the voteCommitImportCollection command.
 *
 * @tags: [requires_persistence, requires_replication, requires_wiredtiger]
 */

(function() {
"use strict";

function testInvalidUsages(db) {
    // Missing fields.
    assert.commandFailedWithCode(db.runCommand({voteCommitImportCollection: "test.foo"}), 40414);

    // Invalid hostAndPort.
    assert.commandFailedWithCode(db.runCommand({
        voteCommitImportCollection: "test.foo",
        from: "localhost:27017:1",
        dryRunSuccess: true
    }),
                                 ErrorCodes.FailedToParse);

    // Invalid vote.
    assert.commandFailedWithCode(db.runCommand({
        voteCommitImportCollection: "test.foo",
        from: "localhost:27017",
        dryRunSuccess: "yes"
    }),
                                 ErrorCodes.TypeMismatch);

    // Invalid reason.
    assert.commandFailedWithCode(db.runCommand({
        voteCommitImportCollection: "test.foo",
        from: "localhost:27017",
        dryRunSuccess: false,
        reason: 12345
    }),
                                 ErrorCodes.TypeMismatch);

    // Unknown field.
    assert.commandFailedWithCode(db.runCommand({
        voteCommitImportCollection: "test.foo",
        from: "localhost:27017",
        dryRunSuccess: false,
        foo: "foo"
    }),
                                 40415);
}

// Test standalone.
jsTestLog("Testing standalone");
const standalone = MongoRunner.runMongod({setParameter: "featureFlagLiveImportExport=true"});
const adminDB = standalone.getDB("admin");
testInvalidUsages(adminDB);

// voteCommitImportCollection is not allowed on standalone.
assert.commandFailedWithCode(
    adminDB.runCommand(
        {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}),
    ErrorCodes.NoReplicationEnabled);
MongoRunner.stopMongod(standalone);

// Test replica set.
jsTestLog("Testing replica set");
const kKeyFile = "jstests/libs/key1";
const rst = new ReplSetTest({
    nodes: 2,
    nodeOptions: {auth: "", setParameter: "featureFlagLiveImportExport=true"},
    keyFile: kKeyFile
});
rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryAdmin = primary.getDB('admin');
const secondaryAdmin = secondary.getDB('admin');

// Setup initial user.
primaryAdmin.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles}, {w: 2});
assert(primaryAdmin.auth('admin', 'pass'));
assert(secondaryAdmin.auth('admin', 'pass'));

testInvalidUsages(primaryAdmin);
testInvalidUsages(secondaryAdmin);

// voteCommitImportCollection is not allowed even with admin role.
assert.commandFailedWithCode(
    primaryAdmin.runCommand(
        {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}),
    ErrorCodes.Unauthorized);
assert.commandFailedWithCode(
    secondaryAdmin.runCommand(
        {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}),
    ErrorCodes.Unauthorized);

assert(primaryAdmin.logout());
assert(secondaryAdmin.logout());

// Test with internal privilege.
authutil.asCluster(primary, kKeyFile, () => {
    // voteCommitImportCollection is admin-only.
    assert.commandFailedWithCode(
        primary.getDB("test").runCommand(
            {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}),
        ErrorCodes.Unauthorized);
    // voteCommitImportCollection is only allowed against admin database with internal privilege.
    assert.commandWorked(primary.getDB("admin").runCommand(
        {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}));
});

authutil.asCluster(secondary, kKeyFile, () => {
    // voteCommitImportCollection is not allowed on secondary.
    assert.commandFailedWithCode(
        secondary.getDB("admin").runCommand(
            {voteCommitImportCollection: "test.foo", from: "localhost:27017", dryRunSuccess: true}),
        ErrorCodes.NotWritablePrimary);
});

rst.stopSet();
}());
