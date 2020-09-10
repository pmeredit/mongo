/**
 * Test the sanity of the exportCollection command.
 *
 * @tags: [requires_persistence, requires_replication, requires_wiredtiger]
 */

(function() {
"use strict";

function testStandalone() {
    const standalone = MongoRunner.runMongod({auth: "", keyFile: "jstests/libs/key1"});
    const testDB = standalone.getDB("test");
    const adminDB = standalone.getDB("admin");

    // exportCollection is allowed on standalone nodes, with the correct authorization.
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.Unauthorized);

    adminDB.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles});
    assert(adminDB.auth('admin', 'pass'));
    const res = assert.commandWorked(testDB.runCommand({exportCollection: "foo"}));
    assert(res.hasOwnProperty("collectionProperties"));
    adminDB.logout();

    MongoRunner.stopMongod(standalone);
}

jsTestLog("Testing standalone");
testStandalone();

function testReplicaSet() {
    const rst = new ReplSetTest({nodes: 2, nodeOptions: {auth: ""}, keyFile: "jstests/libs/key1"});
    rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();

    const primaryDB = primary.getDB("test");
    const secondaryDB = secondary.getDB("test");
    const primaryAdmin = primary.getDB('admin');
    const secondaryAdmin = secondary.getDB('admin');

    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.Unauthorized);
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.Unauthorized);

    primaryAdmin.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles}, {w: 2});
    assert(primaryAdmin.auth('admin', 'pass'));
    assert(secondaryAdmin.auth('admin', 'pass'));

    // exportCollection is not allowed on a live replica set.
    jsTestLog("Testing on primary");
    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: "foo"}), 5070401);

    jsTestLog("Testing on secondary");
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.NotWritablePrimary);

    assert(primaryAdmin.logout());
    assert(secondaryAdmin.logout());

    rst.stopSet();
}

jsTestLog("Testing replica set");
testReplicaSet();

function testReplicaSetNodesInStandaloneMode() {
    const rst = new ReplSetTest({nodes: 2, nodeOptions: {auth: ""}, keyFile: "jstests/libs/key1"});
    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();

    primary.getDB("admin").createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles},
                                      {w: 2});
    rst.stopSet(/*signal=*/null, /*forRestart=*/true);

    const primaryStandalone = MongoRunner.runMongod({
        dbpath: primary.dbpath,
        noReplSet: true,
        noCleanData: true,
        auth: "",
        keyFile: "jstests/libs/key1"
    });
    const secondaryStandalone = MongoRunner.runMongod({
        dbpath: secondary.dbpath,
        noReplSet: true,
        noCleanData: true,
        auth: "",
        keyFile: "jstests/libs/key1"
    });

    const primaryDB = primaryStandalone.getDB("test");
    const secondaryDB = secondaryStandalone.getDB("test");
    const primaryAdminDB = primaryStandalone.getDB("admin");
    const secondaryAdminDB = secondaryStandalone.getDB("admin");

    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.Unauthorized);
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: "foo"}),
                                 ErrorCodes.Unauthorized);

    jsTestLog("Testing on standalone primary");
    assert(primaryAdminDB.auth('admin', 'pass'));
    assert.commandWorked(primaryDB.runCommand({exportCollection: "foo"}));
    assert(primaryAdminDB.logout());
    MongoRunner.stopMongod(primaryStandalone);

    jsTestLog("Testing on standalone secondary");
    assert(secondaryAdminDB.auth('admin', 'pass'));
    assert.commandWorked(secondaryDB.runCommand({exportCollection: "foo"}));
    assert(secondaryAdminDB.logout());
    MongoRunner.stopMongod(secondaryStandalone);
}

jsTestLog("Testing replica set nodes in standalone mode");
testReplicaSetNodesInStandaloneMode();
}());
