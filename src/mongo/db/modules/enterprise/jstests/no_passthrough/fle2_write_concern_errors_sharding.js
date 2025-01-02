/**
 * Test FLE2 handling of write concern errors in multi-shard cluster.
 *
 * @tags: [
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const UNSATISFIABLE_WC = {
    w: 3,
    j: false,
    wtimeout: 1000,
};
const collName = "basic";

function runCommonTestSetup(fixture, plainDbName, fleDbName) {
    const conn = fixture.s;
    conn.getDB(plainDbName).dropDatabase();
    conn.getDB(fleDbName).dropDatabase();

    // Make shard2 the primary for the encrypted db. The ESC and ECOC will live in this shard.
    assert.commandWorked(
        conn.adminCommand({enableSharding: fleDbName, primaryShard: fixture.shard2.shardName}));
    const eclient = new EncryptedClient(conn.getDB(fleDbName).getMongo(), fleDbName);
    assert.commandWorked(eclient.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    const fleNs = fleDbName + "." + collName;
    const plainNs = plainDbName + "." + collName;
    const fleDb = eclient.getDB();
    const plainDb = conn.getDB(plainDbName);

    assert.commandWorked(fleDb.adminCommand({shardCollection: fleNs, key: {_id: 1}}));
    assert.commandWorked(plainDb.adminCommand({shardCollection: plainNs, key: {_id: 1}}));

    assert.commandWorked(fixture.splitAt(fleNs, {_id: 100}));
    assert.commandWorked(fixture.splitAt(fleNs, {_id: 200}));
    assert.commandWorked(fixture.splitAt(plainNs, {_id: 100}));
    assert.commandWorked(fixture.splitAt(plainNs, {_id: 200}));
    fixture.moveChunk(fleNs, {_id: 0}, fixture.shard0.shardName);
    fixture.moveChunk(fleNs, {_id: 200}, fixture.shard1.shardName);
    fixture.moveChunk(fleNs, {_id: 100}, fixture.shard2.shardName);
    fixture.moveChunk(plainNs, {_id: 0}, fixture.shard0.shardName);
    fixture.moveChunk(plainNs, {_id: 200}, fixture.shard1.shardName);
    fixture.moveChunk(plainNs, {_id: 100}, fixture.shard2.shardName);

    const docsToInsert = [
        // shard 0 documents
        {_id: 1, first: "michael"},
        {_id: 2, first: "sonny"},
        {_id: 3, first: "vincent"},
        // shard 1 documents
        {_id: 201, first: "vito"},
        {_id: 202, first: "connie"},
        {_id: 203, first: "tom"},
        // shard 2 documents
        {_id: 101, first: "fredo"}
    ];

    assert.commandWorked(fleDb.erunCommand({insert: collName, documents: docsToInsert}));
    assert.commandWorked(plainDb.runCommand({insert: collName, documents: docsToInsert}));
    assert.eq(7, plainDb[collName].countDocuments({}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 7, 7);
    return eclient;
}

function runCommandOnBothDB(cmd, plainDb, fleDb) {
    let pres = plainDb.runCommand(cmd);
    let eres = fleDb.erunCommand(cmd);
    print("Unencrypted result: " + tojson(pres));
    print("Encrypted result: " + tojson(eres));
    return {pres: pres, eres: eres};
}

function runShardedInsertTests(fixture) {
    const plainDbName = "plaindb_insert";
    const fleDbName = "fledb_insert";
    const eclient = runCommonTestSetup(fixture, plainDbName, fleDbName);
    const fleDb = eclient.getDB();
    const plainDb = fixture.s.getDB(plainDbName);

    // Tests insertion only at the primary shard (shard2) where the ESC and ECOC live.
    print("INSERT: single shard commit");
    let cmd = {
        insert: collName,
        documents: [{_id: 110, first: "clemenza"}],
        writeConcern: UNSATISFIABLE_WC
    };
    let {pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb);
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(pres.n, 1);
    assert.eq(eres.n, 1);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    eclient.assertEncryptedCollectionCounts(collName, 8, 8, 8);

    // Tests insertion at a shard (shard0) different from where the ESC and ECOC live.
    // The shape of the response should be the same as before.
    print("INSERT: multiple write shards (FLE2 only)");
    cmd = {
        insert: collName,
        documents: [{_id: 10, first: "clemenza"}],
        writeConcern: UNSATISFIABLE_WC
    };
    eres = fleDb.erunCommand(cmd);
    print("Encrypted result: " + tojson(eres));
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(eres.n, 1);
    assert.eq(eres.ok, 1);
    eclient.assertEncryptedCollectionCounts(collName, 9, 9, 9);
}

function runShardedDeleteTests(fixture) {
    const plainDbName = "plaindb_delete";
    const fleDbName = "fledb_delete";
    const eclient = runCommonTestSetup(fixture, plainDbName, fleDbName);
    const fleDb = eclient.getDB();
    const plainDb = fixture.s.getDB(plainDbName);

    // Test delete where one or more shards are read, but only one is written to.
    print("DELETE: single write shard commit");
    let cmd = {
        delete: collName,
        deletes: [{q: {first: "connie"}, limit: 0}],
        writeConcern: UNSATISFIABLE_WC
    };
    let {pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb);
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);

    // The delete goes through in the unencrypted case:
    assert.eq(0, plainDb[collName].countDocuments({first: "connie"}));
    assert.eq(pres.n, 1);
    assert.eq(pres.ok, 1);
    assert(!pres.hasOwnProperty("writeErrors"));

    // Encrypted delete also sends the delete to all shards, but it executes them within an
    // internal transaction. In this test case, only one shard is written to, but the single write
    // shard commit aborts the transaction before a commit can be sent to the write shard,
    // so the delete does not go through:
    assert.eq(1, fleDb[collName].ecount({first: "connie"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 7, 7);
    assert.eq(eres.n, 0);
    assert.eq(eres.ok, 1);
    assert(eres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeConcernError"));

    // Test delete targeted to single shard; the delete goes through in both cases
    print("DELETE: single shard commit");
    cmd = {delete: collName, deletes: [{q: {_id: 2}, limit: 0}], writeConcern: UNSATISFIABLE_WC};
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(0, plainDb[collName].countDocuments({_id: 2}));  // delete goes through
    assert.eq(0, fleDb[collName].ecount({_id: 2}));
    assert.eq(pres.n, 1);
    assert.eq(eres.n, 1);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    eclient.assertEncryptedCollectionCounts(collName, 6, 7, 7);

    // Test delete on multiple shards; the delete goes through in both cases
    print("DELETE: multiple write shards");
    cmd = {
        delete: collName,
        deletes: [{q: {first: {$in: ["michael", "tom"]}}, limit: 0}],
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(0, plainDb[collName].countDocuments({first: {$in: ["michael", "tom"]}}));
    assert.eq(0, fleDb[collName].ecount({first: {$in: ["michael", "tom"]}}));
    assert.eq(pres.n, 2);
    assert.eq(eres.n, 2);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    eclient.assertEncryptedCollectionCounts(collName, 4, 7, 7);

    // Test no-op delete
    print("DELETE: read only");
    cmd = {
        delete: collName,
        deletes: [{q: {first: {$in: ["clemenza", "tessio"]}}, limit: 0}],
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(pres.n, 0);
    assert.eq(eres.n, 0);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    eclient.assertEncryptedCollectionCounts(collName, 4, 7, 7);
}

function runShardedUpdateTests(fixture) {
    const plainDbName = "plaindb_update";
    const fleDbName = "fledb_update";
    const eclient = runCommonTestSetup(fixture, plainDbName, fleDbName);
    const fleDb = eclient.getDB();
    const plainDb = fixture.s.getDB(plainDbName);

    // Test update where one or more shards are read, but only one is written to.
    print("UPDATE: single write shard commit");
    let cmd = {
        update: collName,
        updates: [{q: {first: "fredo"}, u: {$set: {first: "clemenza"}}}],
        writeConcern: UNSATISFIABLE_WC
    };
    let {pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb);
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);

    // The update does not go through in both cases, even if there's a matching document.
    assert.eq(0, plainDb[collName].countDocuments({first: "clemenza"}));
    assert.eq(0, fleDb[collName].ecount({first: "clemenza"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 7, 7);
    assert.eq(pres.n, 0);
    assert.eq(eres.n, 0);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    assert.eq(pres.nModified, 0);
    assert.eq(eres.nModified, 0);
    // TODO: SERVER-98557 UnsatisfiableWriteConcern is being reported in writeErrors instead of
    // writeConcernError
    assert(!pres.hasOwnProperty("writeConcernError"));
    assert(!eres.hasOwnProperty("writeConcernError"));

    // Test update targeted to single shard; the update goes through in both cases
    print("UPDATE: single shard commit");
    cmd = {
        update: collName,
        updates: [{q: {_id: 101}, u: {$set: {first: "clemenza"}}}],
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.eq(1, plainDb[collName].countDocuments({first: "clemenza"}));
    assert.eq(1, fleDb[collName].ecount({first: "clemenza"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 8, 8);
    assert.eq(pres.n, 1);
    assert.eq(eres.n, 1);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    assert.eq(pres.nModified, 1);
    assert.eq(eres.nModified, 1);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));

    // Test update on multiple shards; the update goes through in both cases.
    print("UPDATE: multiple write shards");
    pres = plainDb.runCommand({
        update: collName,
        updates: [{
            q: {$or: [{first: "sonny"}, {first: "vito"}]},
            u: {$set: {first: "tessio"}},
            multi: true
        }],
        writeConcern: UNSATISFIABLE_WC
    });
    eres = fleDb.erunCommand({
        update: collName,
        updates: [{q: {first: "sonny"}, u: {$set: {first: "tessio"}}}],
        writeConcern: UNSATISFIABLE_WC
    });
    print("Unencrypted result: " + tojson(pres));
    print("Encrypted result: " + tojson(eres));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.eq(2, plainDb[collName].countDocuments({first: "tessio"}));
    assert.eq(1, fleDb[collName].ecount({first: "tessio"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 9, 9);
    assert.eq(pres.n, 2);
    assert.eq(eres.n, 1);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    assert.eq(pres.nModified, 2);
    assert.eq(eres.nModified, 1);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));

    // Test no-op update
    print("UPDATE: read only");
    cmd = {
        update: collName,
        updates: [{q: {first: "apollonia"}, u: {$set: {second: "barzini"}}}],
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    eclient.assertEncryptedCollectionCounts(collName, 7, 9, 9);
    assert.eq(pres.n, 0);
    assert.eq(eres.n, 0);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    assert.eq(pres.nModified, 0);
    assert.eq(eres.nModified, 0);
    // TODO: SERVER-98557 unencrypted case report UnsatisfiableWriteConcern in writeErrors
    // instead of a writeConcernError
    assert(!pres.hasOwnProperty("writeConcernError"));
    assert(!eres.hasOwnProperty("writeErrors"));
}

function runShardedFindAndModifyTests(fixture) {
    const plainDbName = "plaindb_findandmodify";
    const fleDbName = "fledb_findandmodify";
    const eclient = runCommonTestSetup(fixture, plainDbName, fleDbName);
    const fleDb = eclient.getDB();
    const plainDb = fixture.s.getDB(plainDbName);

    // Test findAndModify where one or more shards are read, but only one is written to.
    print("FINDANDMODIFY: single write shard commit");
    let cmd = {
        findAndModify: collName,
        query: {first: "fredo"},
        update: {$set: {first: "clemenza"}},
        writeConcern: UNSATISFIABLE_WC
    };
    let {pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb);
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);

    // The update does not go through in both cases, even if there's a matching document.
    assert.eq(0, plainDb[collName].countDocuments({first: "clemenza"}));
    assert.eq(0, fleDb[collName].ecount({first: "clemenza"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 7, 7);
    assert.eq(pres.ok, 0);
    assert.eq(eres.ok, 0);
    assert(!pres.hasOwnProperty("lastErrorObject"));
    assert(!eres.hasOwnProperty("lastErrorObject"));
    // TODO: SERVER-98557 UnsatisfiableWriteConcern is top-level error instead of a
    // writeConcernError
    assert(!pres.hasOwnProperty("writeConcernError"));
    assert(!eres.hasOwnProperty("writeConcernError"));

    // Test findAndModify targeted to single shard; the update goes through in both cases
    print("FINDANDMODIFY: single shard commit");
    cmd = {
        findAndModify: collName,
        query: {_id: 101},
        update: {$set: {first: "clemenza"}},
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.eq(1, plainDb[collName].countDocuments({first: "clemenza"}));
    assert.eq(1, fleDb[collName].ecount({first: "clemenza"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 8, 8);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    assert.eq(pres.lastErrorObject.n, 1);
    assert.eq(eres.lastErrorObject.n, 1);
    assert.eq(pres.lastErrorObject.updatedExisting, true);
    assert.eq(eres.lastErrorObject.updatedExisting, true);

    // Test findAndModify on multiple shards - FLE2 only
    // Writes to ESC + ECOC in shard2, and the chunk in shard0
    print("FINDANDMODIFY: multiple write shards");
    cmd = {
        findAndModify: collName,
        query: {_id: 2},
        update: {$set: {first: "tessio"}},
        writeConcern: UNSATISFIABLE_WC
    };
    eres = fleDb.erunCommand(cmd);
    print("Encrypted result: " + tojson(eres));
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.eq(1, fleDb[collName].ecount({first: "tessio"}));
    eclient.assertEncryptedCollectionCounts(collName, 7, 9, 9);
    assert.eq(eres.ok, 1);
    assert.eq(eres.lastErrorObject.n, 1);
    assert.eq(eres.lastErrorObject.updatedExisting, true);

    // Test no-op findAndModify
    print("FINDANDMODIFY: read only");
    cmd = {
        findAndModify: collName,
        query: {first: "apollonia"},
        update: {$set: {second: "tessio"}},
        writeConcern: UNSATISFIABLE_WC
    };
    ({pres, eres} = runCommandOnBothDB(cmd, plainDb, fleDb));
    assert.commandFailedWithCode(pres, ErrorCodes.UnsatisfiableWriteConcern);
    assert.commandFailedWithCode(eres, ErrorCodes.UnsatisfiableWriteConcern);
    eclient.assertEncryptedCollectionCounts(collName, 7, 9, 9);

    // TODO: SERVER-98557 unencrypted case puts UnsatisfiableWriteConcern as top-level error
    // instead of a writeConcernError
    assert.eq(pres.ok, 0);
    assert.eq(eres.ok, 1);
    assert(!pres.hasOwnProperty("writeConcernError"));
    assert(eres.hasOwnProperty("writeConcernError"));
    assert(!pres.hasOwnProperty("lastErrorObject"));
    assert(eres.hasOwnProperty("lastErrorObject"));
    assert.eq(eres.lastErrorObject.n, 0);
    assert.eq(eres.lastErrorObject.updatedExisting, false);
}

jsTestLog("Sharding: Testing FLE2 write concern errors");
{
    const st = new ShardingTest({
        shards: 3,
        mongos: 1,
        config: 1,
    });
    st.s.adminCommand({setParameter: 1, logComponentVerbosity: {sharding: 2, command: 2}});
    runShardedInsertTests(st);
    runShardedDeleteTests(st);
    runShardedUpdateTests(st);
    runShardedFindAndModifyTests(st);
    st.stop();
}
