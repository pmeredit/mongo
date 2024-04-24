/**
 * Tests resharding a non-empty encrypted collection with a new shard key.
 *
 * @tags: [requires_sharding]
 */

import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";
import {ReshardingTest} from "jstests/sharding/libs/resharding_test_fixture.js";

const reshardingTest = new ReshardingTest({
    numDonors: 2,
    numRecipients: 1,
    reshardInPlace: false,
});
reshardingTest.setup();

const donorShardNames = reshardingTest.donorShardNames;
const recipientShardNames = reshardingTest.recipientShardNames;

const dbName = "fle2_reshard_collection";
const collName = "basic";
const nss = dbName + "." + collName;

assert.commandWorked(
    reshardingTest._st.s.adminCommand({enableSharding: dbName, primaryShard: donorShardNames[0]}));

const client = new EncryptedClient(reshardingTest._st.s, dbName);
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields:
        {"fields": [{"path": "name", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

// Shard the encrypted collection on "oldKey", presplit into two chunks:
// lower half in donor-shard0, upper half in donor-shard1
reshardingTest.createShardedCollection({
    ns: nss,
    shardKeyPattern: {oldKey: 1},
    chunks: [
        {min: {oldKey: MinKey}, max: {oldKey: 50}, shard: donorShardNames[0]},
        {min: {oldKey: 50}, max: {oldKey: MaxKey}, shard: donorShardNames[1]},
    ],
});

const testDb = client.getDB();
const testColl = testDb[collName];

assert.commandWorked(testColl.einsert({name: "Al", oldKey: 0, newKey: 100}));
assert.commandWorked(testColl.einsert({name: "Bob", oldKey: 77, newKey: 26}));

// Reshard the collection on "newKey"
// Run writes in the background which will be applied by the oplog applier.
reshardingTest.withReshardingInBackground(
    {
        newShardKeyPattern: {newKey: 1},
        newChunks: [{min: {newKey: MinKey}, max: {newKey: MaxKey}, shard: recipientShardNames[0]}],
    },
    () => {
        // We wait until cloneTimestamp has been chosen to guarantee that any subsequent writes will
        // be applied by the ReshardingOplogApplier.
        reshardingTest.awaitCloneTimestampChosen();
        assert.commandWorked(testColl.einsert({name: "Charlie", oldKey: 1, newKey: -1}));
        assert.commandWorked(testColl.eupdateOne({name: "Al"}, {$set: {name: "Allen"}}));
    });

// Assert encrypted fields are still queryable
for (let value of ["Allen", "Bob", "Charlie"]) {
    let doc = testColl.efindOne({name: value});
    assert(doc);
    assert(doc.hasOwnProperty(kSafeContentField));
}

reshardingTest.teardown();
