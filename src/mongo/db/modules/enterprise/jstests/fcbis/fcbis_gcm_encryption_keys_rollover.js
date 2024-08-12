/**
 * Tests that while using encrypted storage, the file copy based initial sync destination node will
 * roll over GCM encryption keys synced from the sync source.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

if (!platformSupportsGCM) {
    print("Skipping test: Platform does not support GCM");
    quit();
}
const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
const keyFilePath = assetsPath + "ekf";
run("chmod", "600", keyFilePath);

const testName = "fcbis_gcm";
const encryptedNodeOptions = {
    enableEncryption: "",
    encryptionKeyFile: keyFilePath,
    encryptionCipherMode: "AES256-GCM",
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}})
    }
};

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({name: testName, nodes: [encryptedNodeOptions]});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

jsTestLog("Inserting data to the primary before adding the initial sync node.");
assert.commandWorked(primaryDb.test.insert({foo: "bar"}));
// Makes sure our stable timestamp is current.
rst.awaitLastStableRecoveryTimestamp();
// Forces a checkpoint to be taken.
assert.commandWorked(primary.adminCommand({fsync: 1}));

const primaryEncryptionInfo =
    assert.commandWorked(primary.adminCommand({getParameter: 1, keystoreSchemaVersion: 1}))
        .keystoreSchemaVersion;
jsTestLog("Primary node encryption information: " + tojson(primaryEncryptionInfo));

jsTestLog("Adding the initial sync node to the replica set.");
const initialSyncNodeOptions =
    Object.merge(encryptedNodeOptions, {rsConfig: {priority: 0, votes: 0}});
const initialSyncNode = rst.add(initialSyncNodeOptions);
rst.reInitiate();
// Wait for initial sync to finish.
rst.awaitSecondaryNodes();

const initialSyncNodeEncryptionInfo =
    assert.commandWorked(initialSyncNode.adminCommand({getParameter: 1, keystoreSchemaVersion: 1}))
        .keystoreSchemaVersion;
jsTestLog("Initial sync node encryption information: " + tojson(initialSyncNodeEncryptionInfo));

jsTestLog("Validating that keys got rolled over on the initial sync node.");
assert(initialSyncNodeEncryptionInfo.version === primaryEncryptionInfo.version);
assert(initialSyncNodeEncryptionInfo.systemKeyId > primaryEncryptionInfo.systemKeyId);
assert(initialSyncNodeEncryptionInfo.rolloverId > primaryEncryptionInfo.rolloverId);

jsTestLog(
    "Inserting new data after adding the initial sync node and waiting for it to ger replicated.");
assert.commandWorked(primaryDb.test.insert({bizz: "buzz"}));
rst.awaitReplication();

jsTestLog("Validating that data encrypted can be read on the initial sync node.");
const initialSyncNodeDb = initialSyncNode.getDB("test");
const findResults = initialSyncNodeDb.test.find({}, {_id: 0}).toArray();

jsTestLog("Initial sync node find results: " + tojson(findResults));
assert.eq(findResults.length, 2);
assert.contains({bizz: "buzz"}, findResults);
assert.contains({foo: "bar"}, findResults);

rst.stopSet();
