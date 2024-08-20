/**
 * Tests running backup restore with backup cursor after upgrading
 * a replica set through several major versions.
 *
 * For each version downloaded by the multiversion setup:
 * - Start a replica set of that version, without clearing data files
 *   from the previous iteration.
 * - Create a new collection.
 * - Insert a document into the new collection.
 * - Create an index on the new collection.
 *
 * At the end of the latest version, verify the backup cursor can be
 * successfully opened and backup restore can succeed despite that the
 * oplog entry format might be changed after upgrade. See SERVER-55070.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

import "jstests/multiVersion/libs/multi_rs.js";
import "jstests/multiVersion/libs/verify_versions.js";

import {IndexCatalogHelpers} from "jstests/libs/index_catalog_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// Setup the dbpath for this test.
const dbpath = MongoRunner.dataPath + 'major_version_upgrade';
resetDbpath(dbpath);

// This lists all supported releases and needs to be kept up to date as versions are added and
// dropped.
// TODO SERVER-76166: Programmatically generate list of LTS versions.
const versions = [
    // SERVER-55070 mentions an oplog entry parsing issue because the oplog format is
    // different between 3.4 and later versions. However 3.4 is no longer officially
    // supported, so this test starts with a later version for future protection.
    {binVersion: '4.4', featureCompatibilityVersion: '4.4', testCollection: 'four_four'},
    {binVersion: '5.0', featureCompatibilityVersion: '5.0', testCollection: 'five_zero'},
    {binVersion: '6.0', featureCompatibilityVersion: '6.0', testCollection: 'six_zero'},
    {binVersion: '7.0', featureCompatibilityVersion: '7.0', testCollection: 'seven_zero'},
    {binVersion: 'last-lts', featureCompatibilityVersion: lastLTSFCV, testCollection: 'last_lts'},
    {
        binVersion: 'last-continuous',
        featureCompatibilityVersion: lastContinuousFCV,
        testCollection: 'last_continuous'
    },
    {binVersion: 'latest', featureCompatibilityVersion: latestFCV, testCollection: 'latest'},
];

// Setup the ReplSetTest object.
const nodes = {
    n1: {binVersion: versions[0].binVersion},
    n2: {binVersion: versions[0].binVersion},
    n3: {binVersion: versions[0].binVersion},
};
const rst = new ReplSetTest({
    nodes,
    nodeOptions: {
        // Disable background checkpoints: a zero value disables checkpointing.
        syncdelay: 0,
        setParameter: {logComponentVerbosity: tojson({storage: 2})}
    },
});

// Start up and initiate the replica set.
rst.startSet();
rst.initiateWithHighElectionTimeout();

// Iterate from earliest to latest versions specified in the versions list, and follow the steps
// outlined at the top of this test file.
var version;
for (let i = 0; i < versions.length; i++) {
    version = versions[i];

    // Connect to the primary running the old version to ensure that the test can insert and
    // create indices.
    let primary = rst.getPrimary();

    // Upgrade the secondary nodes first.
    rst.upgradeSecondaries({binVersion: version.binVersion});

    assert.eq(
        primary, rst.getPrimary(), "Primary changed unexpectedly after upgrading secondaries");
    assert.neq(null,
               primary,
               `replica set was unable to start up after upgrading secondaries to version: ${
                   version.binVersion}`);

    // Connect to the 'test' database.
    let testDB = primary.getDB('test');
    assert.commandWorked(testDB.createCollection(version.testCollection));
    assert.commandWorked(testDB[version.testCollection].insert({a: 1}));
    assert.eq(1,
              testDB[version.testCollection].count(),
              `mongo should have inserted 1 document into collection ${version.testCollection}`);

    // Create an index on the new collection.
    assert.commandWorked(testDB[version.testCollection].createIndex({a: 1}));

    // Do the index creation and insertion again after upgrading the primary node.
    primary = rst.upgradePrimary(primary, {binVersion: version.binVersion});
    assert.neq(
        null, primary, `replica set was unable to start up with version: ${version.binVersion}`);
    assert.binVersion(primary, version.binVersion);
    testDB = primary.getDB('test');

    assert.commandWorked(testDB[version.testCollection].insert({b: 1}));
    assert.eq(2,
              testDB[version.testCollection].count(),
              `mongo should have inserted 2 documents into collection ${version.testCollection}`);

    assert.commandWorked(testDB[version.testCollection].createIndex({b: 1}));

    // Verify that all previously inserted data and indices are accessible.
    for (let j = 0; j <= i; j++) {
        const oldVersionCollection = versions[j].testCollection;
        assert.eq(2,
                  testDB[oldVersionCollection].count(),
                  `data from ${oldVersionCollection} should be available; nodes: ${tojson(nodes)}`);
        assert.neq(
            null,
            IndexCatalogHelpers.findByKeyPattern(testDB[oldVersionCollection].getIndexes(), {a: 1}),
            `index a from ${oldVersionCollection} should be available; nodes: ${tojson(nodes)}`);
        assert.neq(
            null,
            IndexCatalogHelpers.findByKeyPattern(testDB[oldVersionCollection].getIndexes(), {b: 1}),
            `index b from ${oldVersionCollection} should be available; nodes: ${tojson(nodes)}`);
    }

    // Set the appropriate featureCompatibilityVersion upon upgrade, if applicable.
    if (version.hasOwnProperty('featureCompatibilityVersion')) {
        const primaryAdminDB = primary.getDB("admin");
        const res = primaryAdminDB.runCommand(
            {"setFeatureCompatibilityVersion": version.featureCompatibilityVersion});
        if (!res.ok && res.code === 7369100) {
            // We failed due to requiring 'confirm: true' on the command. This will only
            // occur on 7.0+ nodes that have 'enableTestCommands' set to false. Retry the
            // setFCV command with 'confirm: true'.
            assert.commandWorked(primaryAdminDB.runCommand({
                "setFeatureCompatibilityVersion": version.featureCompatibilityVersion,
                confirm: true,
            }));
        } else {
            assert.commandWorked(res);
        }
    }
}

// Test backup restore with backup cursor at the latest version.
assert.eq(version.binVersion, 'latest', "Failed to upgrade to latest version");
jsTestLog("Testing backup restore with backup cursor at the latest version");
await import("src/mongo/db/modules/enterprise/jstests/hot_backups/backup_restore_backup_cursor.js");

// Stop the replica set.
rst.stopSet();
