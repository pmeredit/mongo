/**
 * Test drop collection warns if the state collection still exist before drop of the main
 * collection.
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2Enabled()) {
    return;
}

function runTest(conn, connLog) {
    let dbName = 'drop_state';
    let db = conn.getDB(dbName);

    let client = new EncryptedClient(db.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    // Fail: drop the user collection before the state collections.
    assert(db.basic.drop());
    assert(checkLog.checkContainsWithCountJson(connLog,
                                               6491401,
                                               {
                                                   name: "drop_state.basic",
                                                   stateCollections: [
                                                       "drop_state.fle2.basic.esc",
                                                       "drop_state.fle2.basic.ecc",
                                                       "drop_state.fle2.basic.ecoc",
                                                   ]
                                               },
                                               /*expectedCount=*/1));

    assert.commandWorked(client.createEncryptionCollection("basic2", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    // Pass: drop the user collection after the state collections.
    assert(db.fle2.basic2.esc.drop());
    assert(db.fle2.basic2.ecc.drop());
    assert(db.fle2.basic2.ecoc.drop());
    assert(db.basic2.drop());
    assert(checkLog.checkContainsWithCountJson(connLog,
                                               6491401,
                                               {
                                                   name: "drop_state.basic2",
                                               },
                                               /*expectedCount=*/0));
}

jsTestLog("ReplicaSet: Testing fle2 drop collection warning");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 drop collection warning");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s, st.shard0);

    st.stop();
}
}());
