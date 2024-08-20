/**
 * Test the sanity of the exportCollection command.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 *   creates_and_authenticates_user
 * ]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";

const collName = "foo";

function testStandalone() {
    let standalone = MongoRunner.runMongod({auth: "", keyFile: "jstests/libs/key1"});
    let testDB = standalone.getDB("test");
    let adminDB = standalone.getDB("admin");

    // exportCollection is allowed on standalone nodes, with the correct authorization.
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);

    adminDB.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles});

    assert(adminDB.auth('admin', 'pass'));

    adminDB.createUser({user: 'clusterAdmin', pwd: 'pass', roles: ["clusterAdmin"]});
    testDB.createUser({user: 'dbOwner', pwd: 'pass', roles: ["dbOwner"]});

    assert.commandWorked(testDB.createCollection(collName));
    assert.commandWorked(testDB.createView("baz", collName, []));
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: collName}), 5088600);

    adminDB.logout();

    MongoRunner.stopMongod(standalone);

    // Restart in read-only mode.
    standalone = MongoRunner.runMongod({
        auth: "",
        keyFile: "jstests/libs/key1",
        dbpath: standalone.dbpath,
        noCleanData: true,
        queryableBackupMode: ""
    });
    testDB = standalone.getDB("test");
    adminDB = standalone.getDB("admin");

    // 'admin' should include all 'clusterAdmin' privileges.
    assert(adminDB.auth('admin', 'pass'));
    assert.commandWorked(testDB.runCommand({exportCollection: collName}));

    // Verify that the command fails to run on non-existent collections.
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: "bar"}), 5091801);

    // Verify that the command fails to run on views.
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: "baz"}), 5091800);

    adminDB.logout();

    // Export should work with clusterAdmin access.
    assert(adminDB.auth('clusterAdmin', 'pass'));
    assert.commandWorked(testDB.runCommand({exportCollection: collName}));
    adminDB.logout();

    // Export should fail with dbOwner only access.
    assert(testDB.auth('dbOwner', 'pass'));
    assert.commandFailedWithCode(testDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);
    testDB.logout();

    MongoRunner.stopMongod(standalone);
}

jsTestLog("Testing standalone");
testStandalone();

function testReplicaSet() {
    const rst = new ReplSetTest({
        nodes: 2,
        nodeOptions: {auth: ""},
        keyFile: "jstests/libs/key1",
    });
    rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();

    const primaryDB = primary.getDB("test");
    const secondaryDB = secondary.getDB("test");
    const primaryAdmin = primary.getDB('admin');
    const secondaryAdmin = secondary.getDB('admin');

    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);

    primaryAdmin.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles}, {w: 2});
    assert(primaryAdmin.auth('admin', 'pass'));
    assert(secondaryAdmin.auth('admin', 'pass'));

    assert.commandWorked(primaryDB.createCollection(collName));

    // exportCollection is not allowed on a live replica set.
    jsTestLog("Testing on primary");
    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: collName}), 5070401);

    jsTestLog("Testing on secondary");
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: collName}),
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

    assert(primary.getDB("admin").auth('admin', 'pass'));
    assert.commandWorked(primary.getDB("test").createCollection(collName));
    assert(primary.getDB("admin").logout());

    rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

    const primaryStandalone = MongoRunner.runMongod({
        dbpath: primary.dbpath,
        noReplSet: true,
        noCleanData: true,
        auth: "",
        keyFile: "jstests/libs/key1",
        queryableBackupMode: ""
    });
    const secondaryStandalone = MongoRunner.runMongod({
        dbpath: secondary.dbpath,
        noReplSet: true,
        noCleanData: true,
        auth: "",
        keyFile: "jstests/libs/key1",
        queryableBackupMode: ""
    });

    const primaryDB = primaryStandalone.getDB("test");
    const secondaryDB = secondaryStandalone.getDB("test");
    const primaryAdminDB = primaryStandalone.getDB("admin");
    const secondaryAdminDB = secondaryStandalone.getDB("admin");

    assert.commandFailedWithCode(primaryDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);
    assert.commandFailedWithCode(secondaryDB.runCommand({exportCollection: collName}),
                                 ErrorCodes.Unauthorized);

    jsTestLog("Testing on standalone primary");
    assert(primaryAdminDB.auth('admin', 'pass'));
    assert.commandWorked(primaryDB.runCommand({exportCollection: collName}));
    assert(primaryAdminDB.logout());
    MongoRunner.stopMongod(primaryStandalone);

    jsTestLog("Testing on standalone secondary");
    assert(secondaryAdminDB.auth('admin', 'pass'));
    assert.commandWorked(secondaryDB.runCommand({exportCollection: collName}));
    assert(secondaryAdminDB.logout());
    MongoRunner.stopMongod(secondaryStandalone);
}

jsTestLog("Testing replica set nodes in standalone mode");
testReplicaSetNodesInStandaloneMode();
