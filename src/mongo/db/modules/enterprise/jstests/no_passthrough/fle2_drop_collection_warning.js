/**
 * Test drop collection warns if the state collection still exist before drop of the main
 * collection.
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

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

    let colls = ["drop_state.enxcol_.basic.esc", "drop_state.enxcol_.basic.ecoc"];

    assert(checkLog.checkContainsWithCountJson(connLog,
                                               6491401,
                                               {name: "drop_state.basic", stateCollections: colls},
                                               /*expectedCount=*/ 1));

    assert.commandWorked(client.createEncryptionCollection("basic2", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    // Pass: drop the user collection after the state collections.
    assert(db.enxcol_.basic2.esc.drop());
    assert(db.enxcol_.basic2.ecoc.drop());
    assert(db.basic2.drop());
    assert(checkLog.checkContainsWithCountJson(connLog,
                                               6491401,
                                               {
                                                   name: "drop_state.basic2",
                                               },
                                               /*expectedCount=*/ 0));
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
