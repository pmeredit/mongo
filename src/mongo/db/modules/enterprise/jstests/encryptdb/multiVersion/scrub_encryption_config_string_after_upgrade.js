/**
 * This test verifies that the initial config string used for WT gets sanitized during an FCV
 * upgrade. This can occur if the user creates the collection with a node-local option like
 * "encryption". As a result of this test we expect the node that durably stored the invalid option
 * to sanitize it during an upgrade.
 *
 * TODO SERVER-80490: This can be removed once 8.0 is branched since it will fail.
 *
 * @tags: [
 *   requires_wiredtiger,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

import "jstests/multiVersion/libs/multi_rs.js";

const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

var ekfValid1 = assetsPath + "ekf";
run("chmod", "600", ekfValid1);

function createNodeConfigWithEncryption(params) {
    const defaultParams = {
        enableEncryption: "",
        encryptionKeyFile: ekfValid1,
    };

    const opts = Object.merge(defaultParams, params);

    return opts;
}

// Setup the ReplSetTest object. Make sure to create it with an older version that allows node-local
// options.
const rst = new ReplSetTest({
    name: jsTestName(),
    nodes: 2,
    nodeOptions: createNodeConfigWithEncryption({binVersion: 'last-lts'})
});

rst.startSet();
rst.initiate();

const originalPrimary = rst.getPrimary();

let testDB = originalPrimary.getDB('test');
const collName = jsTestName();

// Create a collection that uses node-local options on the primary. Secondaries will get the
// sanitized version.
const originalOpts = {
    storageEngine: {
        wiredTiger: {
            configString:
                "allocation_size=4KB,encryption=(keyid=\"admin\",name=AES256-CBC),internal_page_max=4KB"
        }
    }
};
assert.commandWorked(testDB.createCollection(collName, originalOpts));

const sanitizedStorageEngineOpts = {
    storageEngine: {wiredTiger: {configString: "allocation_size=4KB,internal_page_max=4KB"}}
};

// On secondaries the options should not be there. Verify this behavior.
rst.nodes.forEach(function(node) {
    node.setSecondaryOk();
    const collInfo = node.getDB('test').getCollectionInfos({name: collName});
    jsTestLog(`Node: ${node}: ${JSON.stringify(collInfo)}`);
    if (node.host === originalPrimary.host) {
        assert.eq(collInfo[0].options, originalOpts);
    } else {
        assert.eq(collInfo[0].options, sanitizedStorageEngineOpts);
    }
});

// Upgrade the binaries, the nodes should still have the same metadata in the catalog.
rst.upgradeSet({binVersion: 'latest'});

rst.nodes.forEach(function(node) {
    node.setSecondaryOk();
    const collInfo = node.getDB('test').getCollectionInfos({name: collName});
    jsTestLog(`Node: ${node}: ${JSON.stringify(collInfo)}`);
    if (node.host === originalPrimary.host) {
        assert.eq(collInfo[0].options, originalOpts);
    } else {
        assert.eq(collInfo[0].options, sanitizedStorageEngineOpts);
    }
});

// Upgrade FCV version. This should modify the catalog to scrub the node-local information.
const primaryAdminDB = rst.getPrimary().getDB("admin");
assert.commandWorked(
    primaryAdminDB.runCommand({setFeatureCompatibilityVersion: latestFCV, confirm: true}));
rst.awaitReplication();

rst.nodes.forEach(function(node) {
    node.setSecondaryOk();
    const collInfo = node.getDB('test').getCollectionInfos({name: collName});
    jsTestLog(`Node: ${node}: ${JSON.stringify(collInfo)}`);
    assert.eq(collInfo[0].options, sanitizedStorageEngineOpts);
});

rst.stopSet();
