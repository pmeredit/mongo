/**
 * Test FLE2 handling of write concern errors.
 *
 * @tags: [
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {FixtureHelpers} from "jstests/libs/fixture_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const UNSATISFIABLE_WC = {
    w: 3,
    j: false,
    wtimeout: 1000,
};
const collName = "basic";
const VALUES = ["michael", "sonny", "vito", "connie", "fredo"];

function checkHasWCE(res, expectedCode = ErrorCodes.UnsatisfiableWriteConcern) {
    assert(res.hasOwnProperty("writeConcernError"));
    assert.eq(res.writeConcernError.code, expectedCode);
    return res;
}

function maybeShardTestCollections(sharded, fleDb, plainDb) {
    if (!sharded)
        return;
    assert.commandWorked(fleDb.adminCommand(
        {shardCollection: fleDb.getName() + "." + collName, key: {_id: "hashed"}}));
    assert.commandWorked(plainDb.adminCommand(
        {shardCollection: plainDb.getName() + "." + collName, key: {_id: "hashed"}}));
}

function runInsertTests(conn, sharded) {
    const plainDbName = "plaindb_insert";
    const fleDbName = "fledb_insert";
    const client = new EncryptedClient(conn.getDB(fleDbName).getMongo(), fleDbName);
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    const fleDb = client.getDB();
    const plainDb = conn.getDB(plainDbName);
    const docsToInsert = VALUES.map((name) => ({first: name}));
    maybeShardTestCollections(sharded, fleDb, plainDb);

    assert.commandWorked(fleDb[collName].insert({_id: 256}));
    assert.commandWorked(plainDb[collName].insert({_id: 256}));
    client.assertEncryptedCollectionCounts(collName, 1, 0, 0);

    let cmd = {insert: collName, documents: [{first: "tom"}], writeConcern: UNSATISFIABLE_WC};
    let multiCmd = {insert: collName, documents: docsToInsert, writeConcern: UNSATISFIABLE_WC};
    let dupIdCmd = {
        insert: collName,
        documents: [{_id: 256, first: "tom"}],
        writeConcern: UNSATISFIABLE_WC
    };
    let batchedCmdWithDupId = {
        insert: collName,
        documents: [{first: "michael"}, {_id: 256, first: "tom"}, {first: "vito"}],
        writeConcern: UNSATISFIABLE_WC
    };

    function runCommandsAndCompareResults(
        command, pdb, edb, errorCode, expectedN, expectedOk = 1, fleWCEMasked = false) {
        let pres = pdb.runCommand(command);
        let eres = edb.erunCommand(command);
        print("Unencrypted result: " + tojson(pres));
        print("Encrypted result: " + tojson(eres));
        assert.commandFailedWithCode(pres, errorCode);
        assert.commandFailedWithCode(eres, errorCode);
        checkHasWCE(pres, ErrorCodes.UnsatisfiableWriteConcern);
        if (!fleWCEMasked) {
            checkHasWCE(eres, ErrorCodes.UnsatisfiableWriteConcern);
        } else {
            assert(!eres.hasOwnProperty("writeConcernError"));
        }
        assert.eq(pres.hasOwnProperty("writeErrors"), eres.hasOwnProperty("writeErrors"));
        assert.eq(pres.n, expectedN);
        assert.eq(eres.n, expectedN);
        assert.eq(pres.ok, expectedOk);
        assert.eq(eres.ok, expectedOk);
    }

    print("INSERT: single insert with unsatisfiable WC");
    runCommandsAndCompareResults(cmd, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, 1);
    client.assertEncryptedCollectionCounts(collName, 2, 1, 1);

    print("INSERT: batched insert with unsatisfiable WC");
    // For batched inserts that produce multiple WCEs, only the WCE for the last statement that
    // produced one will be returned in the response.
    runCommandsAndCompareResults(multiCmd, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, 5);
    client.assertEncryptedCollectionCounts(collName, 7, 6, 6);

    print("INSERT: single insert with unsatisfiable WC and duplicate _id");
    // TODO: SERVER-84081 TXN API abort handling does not expose the WCE to FLE2 CRUD,
    // so WCEs are not reported in FLE2 responses alongside write errors.
    runCommandsAndCompareResults(dupIdCmd, plainDb, fleDb, ErrorCodes.DuplicateKey, 0, 1, true);
    client.assertEncryptedCollectionCounts(collName, 7, 6, 6);

    // The following FLE2 tests get a WCE along with the DuplicateKey write error because
    // the write error happens on the second insert statement, at which point the WCE has
    // already been set by the previous insert statement.
    print("INSERT: batched insert with unsatisfiable WC and duplicate _id at index 1");
    runCommandsAndCompareResults(batchedCmdWithDupId, plainDb, fleDb, ErrorCodes.DuplicateKey, 1);
    client.assertEncryptedCollectionCounts(collName, 8, 7, 7);

    print("INSERT: unordered batched insert with unsatisfiable WC and duplicate _id at index 1");
    batchedCmdWithDupId.ordered = false;
    runCommandsAndCompareResults(batchedCmdWithDupId, plainDb, fleDb, ErrorCodes.DuplicateKey, 2);
    client.assertEncryptedCollectionCounts(collName, 10, 9, 9);
    delete batchedCmdWithDupId.ordered;
}

function runDeleteTests(conn, sharded) {
    const plainDbName = "plaindb_delete";
    const fleDbName = "fledb_delete";
    const client = new EncryptedClient(conn.getDB(fleDbName).getMongo(), fleDbName);
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    const fleDb = client.getDB();
    const plainDb = conn.getDB(plainDbName);
    const docsToInsert = VALUES.map((name) => ({first: name}));
    maybeShardTestCollections(sharded, fleDb, plainDb);

    assert.commandWorked(fleDb.erunCommand({insert: collName, documents: docsToInsert}));
    assert.commandWorked(plainDb.runCommand({insert: collName, documents: docsToInsert}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    let cmd = {
        delete: collName,
        deletes: [{q: {first: {$in: ["vito", "sonny"]}}, limit: 0}],
        writeConcern: UNSATISFIABLE_WC
    };

    print("DELETE: simple delete with unsatisfiable WC");
    let pres =
        assert.commandFailedWithCode(plainDb.runCommand(cmd), ErrorCodes.UnsatisfiableWriteConcern);
    let eres =
        assert.commandFailedWithCode(fleDb.erunCommand(cmd), ErrorCodes.UnsatisfiableWriteConcern);
    assert(!pres.hasOwnProperty("writeErrors"));
    assert(!eres.hasOwnProperty("writeErrors"));
    assert.eq(pres.n, 2);
    assert.eq(eres.n, 2);
    assert.eq(pres.ok, 1);
    assert.eq(eres.ok, 1);
    client.assertEncryptedCollectionCounts(collName, 3, 5, 5);
}

function runUpdateTests(conn, sharded) {
    const plainDbName = "plaindb_update";
    const fleDbName = "fledb_update";
    const client = new EncryptedClient(conn.getDB(fleDbName).getMongo(), fleDbName);
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    const fleDb = client.getDB();
    const plainDb = conn.getDB(plainDbName);
    const docsToInsert = VALUES.map((name) => ({first: name}));
    maybeShardTestCollections(sharded, fleDb, plainDb);

    assert.commandWorked(fleDb.erunCommand({insert: collName, documents: docsToInsert}));
    assert.commandWorked(plainDb.runCommand({insert: collName, documents: docsToInsert}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    let cmdSet = {
        update: collName,
        updates: [{q: {first: "fredo"}, u: {$set: {first: "apollonia"}}}],
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdReplace = {
        update: collName,
        updates: [{q: {first: "apollonia"}, u: {first: "clemenza"}}],
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdUpsert = {
        update: collName,
        updates: [{q: {_id: NumberLong(256)}, u: {$set: {first: "tom"}}, upsert: true}],
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdModifyId = {
        update: collName,
        updates: [{q: {first: "vito"}, u: {$set: {_id: 1}}}],
        writeConcern: UNSATISFIABLE_WC
    };

    function runCommandsAndCompareResults(command,
                                          pdb,
                                          edb,
                                          errorCode,
                                          expectedN,
                                          expectedNModified,
                                          expectedOk = 1,
                                          fleWCEMasked = false) {
        let pres = assert.commandFailedWithCode(pdb.runCommand(command), errorCode);
        let eres = assert.commandFailedWithCode(edb.erunCommand(command), errorCode);
        print("Unencrypted result: " + tojson(pres));
        print("Encrypted result: " + tojson(eres));
        checkHasWCE(pres, ErrorCodes.UnsatisfiableWriteConcern);
        if (!fleWCEMasked) {
            checkHasWCE(eres, ErrorCodes.UnsatisfiableWriteConcern);
        } else {
            assert(!eres.hasOwnProperty("writeConcernError"));
        }
        assert.eq(pres.hasOwnProperty("writeErrors"), eres.hasOwnProperty("writeErrors"));
        assert.eq(pres.n, expectedN);
        assert.eq(eres.n, expectedN);
        assert.eq(pres.nModified, expectedNModified);
        assert.eq(eres.nModified, expectedNModified);
        assert.eq(pres.ok, expectedOk);
        assert.eq(eres.ok, expectedOk);
    }

    print("UPDATE: modification with unsatisfiable WC");
    runCommandsAndCompareResults(
        cmdSet, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, 1, 1, 1);
    client.assertEncryptedCollectionCounts(collName, 5, 6, 6);

    print("UPDATE: replacement with unsatisfiable WC");
    runCommandsAndCompareResults(
        cmdReplace, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, 1, 1, 1);
    client.assertEncryptedCollectionCounts(collName, 5, 7, 7);

    print("UPDATE: upsert with unsatisfiable WC");
    runCommandsAndCompareResults(
        cmdUpsert, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, 1, 0, 1);
    client.assertEncryptedCollectionCounts(collName, 6, 8, 8);

    print("UPDATE: modification of immutable field with unsatisfiable WC");
    // TODO: SERVER-84081 TXN API abort handling does not expose the WCE to FLE2 CRUD,
    // so WCEs are not reported in FLE2 responses alongside write errors.
    runCommandsAndCompareResults(
        cmdModifyId, plainDb, fleDb, ErrorCodes.ImmutableField, 0, 0, 1, true);
    client.assertEncryptedCollectionCounts(collName, 6, 8, 8);
}

function runFindAndModifyTests(conn, sharded) {
    const plainDbName = "plaindb_findandmodify";
    const fleDbName = "fledb_findandmodify";
    const client = new EncryptedClient(conn.getDB(fleDbName).getMongo(), fleDbName);
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    const fleDb = client.getDB();
    const plainDb = conn.getDB(plainDbName);
    const docsToInsert = VALUES.map((name) => ({first: name}));
    maybeShardTestCollections(sharded, fleDb, plainDb);

    assert.commandWorked(fleDb.erunCommand({insert: collName, documents: docsToInsert}));
    assert.commandWorked(plainDb.runCommand({insert: collName, documents: docsToInsert}));
    client.assertEncryptedCollectionCounts(collName, 5, 5, 5);

    let cmdSet = {
        findAndModify: collName,
        query: {first: "fredo"},
        update: {$set: {first: "apollonia"}},
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdReplace = {
        findAndModify: collName,
        query: {first: "apollonia"},
        update: {first: "clemenza"},
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdRemove = {
        findAndModify: collName,
        query: {first: "clemenza"},
        remove: true,
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdUpsert = {
        findAndModify: collName,
        query: {_id: NumberLong(256)},
        update: {$set: {first: "clemenza"}},
        upsert: true,
        writeConcern: UNSATISFIABLE_WC
    };
    let cmdModifyId = {
        findAndModify: collName,
        query: {first: "sonny"},
        update: {$set: {_id: 1}},
        writeConcern: UNSATISFIABLE_WC
    };

    function runCommandsAndCompareResults(
        command, pdb, edb, errorCode, lastErrorObject, expectedOk = 1, fleWCEMasked = false) {
        let pres = assert.commandFailedWithCode(pdb.runCommand(command), errorCode);
        let eres = assert.commandFailedWithCode(edb.erunCommand(command), errorCode);
        print("Unencrypted result: " + tojson(pres));
        print("Encrypted result: " + tojson(eres));
        checkHasWCE(pres, ErrorCodes.UnsatisfiableWriteConcern);
        if (!fleWCEMasked) {
            checkHasWCE(eres, ErrorCodes.UnsatisfiableWriteConcern);
        } else {
            assert(!eres.hasOwnProperty("writeConcernError"));
        }
        if (lastErrorObject !== undefined) {
            assert.eq(pres.lastErrorObject.n, lastErrorObject.n);
            assert.eq(eres.lastErrorObject.n, lastErrorObject.n);

            if (lastErrorObject.updatedExisting !== undefined) {
                assert.eq(pres.lastErrorObject.updatedExisting, lastErrorObject.updatedExisting);
                assert.eq(eres.lastErrorObject.updatedExisting, lastErrorObject.updatedExisting);
            } else {
                assert(!pres.lastErrorObject.hasOwnProperty("updatedExisting"));
                assert(!eres.lastErrorObject.hasOwnProperty("updatedExisting"));
            }

            if (lastErrorObject.upserted !== undefined) {
                assert.eq(pres.lastErrorObject.upserted, lastErrorObject.upserted);
                assert.eq(eres.lastErrorObject.upserted, lastErrorObject.upserted);
            } else {
                assert(!pres.lastErrorObject.hasOwnProperty("upserted"));
                assert(!eres.lastErrorObject.hasOwnProperty("upserted"));
            }
        } else {
            assert(!pres.hasOwnProperty("lastErrorObject"));
            assert(!eres.hasOwnProperty("lastErrorObject"));
        }
        assert.eq(pres.ok, expectedOk);
        assert.eq(eres.ok, expectedOk);
    }

    print("FINDANDMODIFY: modification with unsatisfiable WC");
    runCommandsAndCompareResults(cmdSet,
                                 plainDb,
                                 fleDb,
                                 ErrorCodes.UnsatisfiableWriteConcern,
                                 {n: 1, updatedExisting: true});
    client.assertEncryptedCollectionCounts(collName, 5, 6, 6);

    print("FINDANDMODIFY: replacement with unsatisfiable WC");
    runCommandsAndCompareResults(cmdReplace,
                                 plainDb,
                                 fleDb,
                                 ErrorCodes.UnsatisfiableWriteConcern,
                                 {n: 1, updatedExisting: true});
    client.assertEncryptedCollectionCounts(collName, 5, 7, 7);

    print("FINDANDMODIFY: remove with unsatisfiable WC");
    runCommandsAndCompareResults(
        cmdRemove, plainDb, fleDb, ErrorCodes.UnsatisfiableWriteConcern, {n: 1});
    client.assertEncryptedCollectionCounts(collName, 4, 7, 7);

    print("FINDANDMODIFY: upsert with unsatisfiable WC");
    runCommandsAndCompareResults(cmdUpsert,
                                 plainDb,
                                 fleDb,
                                 ErrorCodes.UnsatisfiableWriteConcern,
                                 {n: 1, updatedExisting: false, upserted: 256});
    client.assertEncryptedCollectionCounts(collName, 5, 8, 8);

    print("FINDANDMODIFY: modification of immutable field with unsatisfiable WC");
    // TODO: SERVER-84081 encrypted findAndModify masks the WCE in both mongos and mongod
    runCommandsAndCompareResults(
        cmdModifyId, plainDb, fleDb, ErrorCodes.ImmutableField, undefined, 0, true);
    client.assertEncryptedCollectionCounts(collName, 5, 8, 8);
}

function runTests(conn, sharded = false) {
    runInsertTests(conn, sharded);
    runDeleteTests(conn, sharded);
    runUpdateTests(conn, sharded);
    runFindAndModifyTests(conn, sharded);
}

jsTestLog("ReplicaSet: Testing FLE2 write concern errors");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTests(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing FLE2 write concern errors");
{
    const st = new ShardingTest({
        shards: 1,
        mongos: 1,
        config: 1,
    });
    runTests(st.s, true);
    st.stop();
}
