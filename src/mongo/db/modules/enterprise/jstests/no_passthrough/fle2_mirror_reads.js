
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const dbName = "testdb";
const collName = "basic";

function verifyEncryptedMirrorReads(rst, edb, cmd) {
    const slowQueryId = 51803;
    const secDb = rst.getSecondary().getDB("admin");

    assert.commandWorked(secDb.adminCommand({clearLog: "global"}));

    if (cmd.hasOwnProperty("bulkWrite")) {
        assert.commandWorked(edb.eadminCommand(cmd));
    } else {
        assert.commandWorked(edb.erunCommand(cmd));
    }

    // Verify the secondary does not log a slow query log on the encrypted namespace.
    // The absence of a slow query log implies FLE2 redaction happened due to the presence of
    // encryptionInformation in the mirrored request.
    const slowQueryLogs =
        checkLog.getFilteredLogMessages(secDb, slowQueryId, {"ns": `${dbName}.${collName}`});
    assert.eq(0, slowQueryLogs.length);
}

function runTest(conn, rst) {
    const sharded = conn.isMongos();
    const client = new EncryptedClient(conn, dbName);
    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields": [{
                "path": "first",
                "bsonType": "string",
                "queries": {"queryType": "equality", "contention": 0}
            }]
        }
    }));

    // Enable mirroring of all reads
    rst.getPrimary().adminCommand({setParameter: 1, mirrorReads: {samplingRate: 1.0}});

    // Set slow query threshold to -1 so every query gets logged
    rst.getSecondary().getDB('admin').setProfilingLevel(0, -1);

    const edb = client.getDB();
    const coll = edb.getCollection(collName);

    // insert test data
    for (let i = 1; i <= 10; i++) {
        assert.commandWorked(coll.einsert({_id: i, "first": "bob", ctr: i}));
    }

    for (let testCollScan of [false, true]) {
        if (testCollScan) {
            jsTestLog("Testing QE mirrored reads with very small tag limit");
            conn.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 128});
        }

        // For QE find and count, the secondary receives just one mirrored read
        // containing the original encrypted query.
        jsTestLog("Test encrypted 'find' commands are mirrored");
        verifyEncryptedMirrorReads(rst, edb, {find: collName, filter: {first: "bob"}});

        jsTestLog("Test encrypted 'count' commands are mirrored");
        verifyEncryptedMirrorReads(rst, edb, {count: collName, query: {first: "bob"}});

        // For QE findAndModify/update, the secondary receives two mirrored reads
        // (1 for rewritten command, 1 for find by _id used by garbage collect).
        // On replsets, there's one extra mirrored read for the outer command, which
        // contains the original encrypted query.
        jsTestLog("Test encrypted 'findAndModify' command queries are mirrored");
        verifyEncryptedMirrorReads(
            rst,
            edb,
            {findAndModify: collName, query: {first: "bob", _id: 1}, update: {"$inc": {ctr: 1}}});

        jsTestLog("Test encrypted 'update' command queries are mirrored");
        verifyEncryptedMirrorReads(
            rst,
            edb,
            {update: collName, updates: [{q: {first: "bob", _id: 2}, u: {"$inc": {ctr: 1}}}]});

        if (FeatureFlagUtil.isEnabled(conn, "BulkWriteCommand")) {
            jsTestLog("Test encrypted 'bulkWrite' update command queries are mirrored");
            verifyEncryptedMirrorReads(rst, edb, {
                bulkWrite: 1,
                ops: [{update: 0, filter: {first: "bob", _id: 3}, updateMods: {'$inc': {ctr: 1}}}],
                nsInfo: [{ns: dbName + "." + collName}]
            });
        }
    }
}

jsTestLog("ReplicaSet: Testing QE mirrored reads");
{
    const rst = new ReplSetTest({nodes: 2});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst);
    rst.stopSet();
}

jsTestLog("Sharding: Testing QE mirrored reads");
{
    const st = new ShardingTest({shards: {rs0: {nodes: 2}}, mongos: 1, config: 1});
    runTest(st.s, st.rs0);
    st.stop();
}
