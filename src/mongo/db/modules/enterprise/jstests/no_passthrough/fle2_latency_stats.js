/**
 * Check that serverStatus.opLatencies.*.queryableEncryptionLatencyMicros records time spent in QE
 * operations.
 *
 * @tags: [
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const testDBName = "test";

let readsQE = 0;
let writesQE = 0;
let commandsQE = 0;
let readsNormal = 0;
let writesNormal = 0;
let commandsNormal = 0;

function initCounters(db) {
    const ss = db.serverStatus();

    readsQE = ss.opLatencies.reads.queryableEncryptionLatencyMicros;
    writesQE = ss.opLatencies.writes.queryableEncryptionLatencyMicros;
    commandsQE = ss.opLatencies.commands.queryableEncryptionLatencyMicros;

    readsNormal = ss.opLatencies.reads.latency;
    writesNormal = ss.opLatencies.writes.latency;
    commandsNormal = ss.opLatencies.commands.latency;

    print(`Initial QE Counters: Reads ${readsQE}, Writes ${writesQE}, Commands: ${commandsQE}`);
    print(`Initial Normal Counters: Reads ${readsNormal}, Writes ${writesNormal}, Commands: ${
        commandsNormal}`);
}

function assertCounter(name, originalValue, newValue, changed) {
    if (changed) {
        assert.gt(newValue,
                  originalValue,
                  `Expected ${name} to increase original: ${originalValue}, new: ${newValue}`);
    } else {
        assert.eq(newValue,
                  originalValue,
                  `Expected ${name} not to change, original: ${originalValue}, new: ${newValue}`);
    }
}

function assertCounterChanged(name, originalValue, newValue, changed) {
    if (changed) {
        assert.gt(newValue,
                  originalValue,
                  `Expected ${name} to increase original: ${originalValue}, new: ${newValue}`);
    }
}

function assertLatencyChanges(db, readsChanged, writesChanged, commandsChanged) {
    const ss = db.serverStatus();

    let readsQENew = ss.opLatencies.reads.queryableEncryptionLatencyMicros;
    let writesQENew = ss.opLatencies.writes.queryableEncryptionLatencyMicros;
    let commandsQENew = ss.opLatencies.commands.queryableEncryptionLatencyMicros;

    print(`SS: QE Reads ${readsQENew}, Writes ${writesQENew}, Commands: ${commandsQENew}`);

    assertCounter("QE reads", readsQE, readsQENew, readsChanged);
    assertCounter("QE writes", writesQE, writesQENew, writesChanged);
    assertCounter("QE commands", commandsQE, commandsQENew, commandsChanged);

    readsQE = readsQENew;
    writesQE = writesQENew;
    commandsQE = commandsQENew;

    // Check normal Counters
    // Normal counters should be bumped by QE work but are also bumped by all other activity in the
    // system (like background threads) so we cannot assert they do not change.
    let readsNormalNew = ss.opLatencies.reads.latency;
    let writesNormalNew = ss.opLatencies.writes.latency;
    let commandsNormalNew = ss.opLatencies.commands.latency;

    print(`SS: Normal Reads ${readsNormalNew}, Writes ${writesNormalNew}, Commands: ${
        commandsNormalNew}`);

    assertCounterChanged("normal reads", readsNormal, readsNormalNew, readsChanged);
    assertCounterChanged("normal writes", writesNormal, writesNormalNew, writesChanged);
    assertCounterChanged("normal commands", commandsNormal, commandsNormalNew, commandsChanged);

    readsNormal = readsNormalNew;
    writesNormal = writesNormalNew;
    commandsNormal = commandsNormalNew;
}

function assertOnlyReadsChanged(db) {
    assertLatencyChanges(db, true, false, false);
}

function assertOnlyWritesChanged(db) {
    assertLatencyChanges(db, false, true, false);
}

function assertOnlyCommandsChanged(db) {
    assertLatencyChanges(db, false, false, true);
}

function assertNoCountersChanged(db) {
    assertLatencyChanges(db, false, false, false);
}

function runTest(conn) {
    let db = conn.getDB(testDBName);
    db.dropDatabase();

    let client = new EncryptedClient(db.getMongo(), testDBName);

    assert.commandWorked(client.createEncryptionCollection("badlog", {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "middle", "bsonType": "string"},
            ]
        }
    }));

    let edb = client.getDB();

    initCounters(db);

    // Insert
    //
    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assertOnlyWritesChanged(db);
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));
    assertNoCountersChanged(db);

    // Compact
    assert.commandWorked(edb.badlog.compact());
    assertOnlyCommandsChanged(db);

    // Cleanup
    assert.commandWorked(edb.badlog.cleanup());
    assertOnlyCommandsChanged(db);

    // Find
    //
    assert.eq(edb.badlog.find({first: "mark"}).itcount(), 1);
    assertOnlyReadsChanged(db);

    assert.eq(db.goodlog.find({first: "luke"}).itcount(), 1);
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.find({first: "mark"}).explain());
    assertOnlyCommandsChanged(db);
    assert.commandWorked(db.goodlog.find({first: "luke"}).explain());
    assertNoCountersChanged(db);

    // Count
    //
    assert.eq(edb.badlog.count({first: "mark"}), 1);
    assertOnlyReadsChanged(db);
    assert.eq(db.goodlog.count({first: "luke"}), 1);
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.explain().count({first: "mark"}));
    assertOnlyCommandsChanged(db);
    assert.commandWorked(db.goodlog.explain().count({first: "luke"}));
    assertNoCountersChanged(db);

    // Aggregate
    //
    assert.eq(edb.badlog.aggregate([{$match: {"first": "mark"}}]).itcount(), 1);
    assertOnlyReadsChanged(db);
    assert.eq(db.goodlog.aggregate([{$match: {"first": "luke"}}]).itcount(), 1);
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.explain().aggregate([{$match: {"first": "mark"}}]));
    assertOnlyReadsChanged(db);
    assert.commandWorked(db.goodlog.explain().aggregate([{$match: {"first": "luke"}}]));
    assertNoCountersChanged(db);

    // aggregate explain looks like normal aggregate with a flag explain: true

    // FindAndModify
    //
    assert.commandWorked(edb.badlog.runCommand({
        findAndModify: edb.badlog.getName(),
        query: {"first": "mark"},
        update: {$set: {"last": "marco"}}
    }));
    assertOnlyWritesChanged(db);
    assert.commandWorked(db.goodlog.runCommand({
        findAndModify: db.goodlog.getName(),
        query: {"first": "luke"},
        update: {$set: {"last": "marco"}}
    }));
    assertNoCountersChanged(db);

    assert.eq(edb.badlog.find({last: "marco"}).itcount(), 1);
    assertOnlyReadsChanged(db);
    assert.eq(db.goodlog.find({last: "marco"}).itcount(), 1);
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.runCommand({
        explain: {
            findAndModify: edb.badlog.getName(),
            query: {"first": "mark"},
            update: {$set: {"last": "marco"}}
        }
    }));
    assertOnlyCommandsChanged(db);
    assert.commandWorked(db.goodlog.runCommand({
        explain: {
            findAndModify: db.goodlog.getName(),
            query: {"first": "luke"},
            update: {$set: {"last": "marco"}}
        }
    }));

    assertNoCountersChanged(db);

    // Update
    //
    assert.commandWorked(edb.badlog.updateOne({"first": "mark"}, {$set: {"last": "markus"}}));
    assertOnlyWritesChanged(db);
    assert.commandWorked(db.goodlog.updateOne({"first": "luke"}, {$set: {"last": "markus"}}));
    assertNoCountersChanged(db);

    assert.eq(edb.badlog.find({last: "markus"}).itcount(), 1);
    assertOnlyReadsChanged(db);
    assert.eq(db.goodlog.find({last: "markus"}).itcount(), 1);
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.runCommand({
        explain: {
            update: edb.badlog.getName(),
            updates: [{q: {"first": "mark"}, u: {$set: {"last": "marco"}}}]
        }
    }));
    assertOnlyCommandsChanged(db);
    assert.commandWorked(db.goodlog.runCommand({
        explain: {
            update: db.goodlog.getName(),
            updates: [{q: {"first": "luke"}, u: {$set: {"last": "marco"}}}]
        }
    }));
    assertNoCountersChanged(db);

    // Delete
    //
    assert.commandWorked(edb.badlog.runCommand(
        {explain: {delete: edb.badlog.getName(), deletes: [{q: {"first": "mark"}, limit: 1}]}}));
    assertOnlyCommandsChanged(db);
    assert.commandWorked(db.goodlog.runCommand(
        {explain: {delete: db.goodlog.getName(), deletes: [{q: {"first": "luke"}, limit: 1}]}}));
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.deleteOne({"first": "mark"}));
    assertOnlyWritesChanged(db);

    assert.commandWorked(db.goodlog.deleteOne({"first": "luke"}));
    assertNoCountersChanged(db);

    assert.eq(edb.badlog.find({}).itcount(), 0);
    assertOnlyReadsChanged(db);
    assert.eq(db.goodlog.find({}).itcount(), 0);
    assertNoCountersChanged(db);

    // Batch Size & getMore
    //
    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assertOnlyWritesChanged(db);
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));
    assertNoCountersChanged(db);

    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assertOnlyWritesChanged(db);
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));
    assertNoCountersChanged(db);

    let cursorEncrypted = edb.badlog.find({first: "mark"}).batchSize(1);
    assert.eq(cursorEncrypted.objsLeftInBatch(), 1);
    assert.eq(cursorEncrypted.hasNext(), true);
    assertOnlyReadsChanged(db);

    let cursorUnencrypted = db.goodlog.find({first: "luke"}).batchSize(1);
    assert.eq(cursorUnencrypted.objsLeftInBatch(), 1);
    assert.eq(cursorUnencrypted.hasNext(), true);
    assertNoCountersChanged(db);

    // Force a call to getMore
    let batchE = cursorEncrypted.next();
    assert.eq(cursorEncrypted.hasNext(), true);
    assertOnlyReadsChanged(db);

    let batchU = cursorUnencrypted.next();
    assert.eq(cursorUnencrypted.hasNext(), true);
    assertNoCountersChanged(db);
}

jsTestLog("ReplicaSet: Testing fle2 latency stats");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 latency stats");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});
    runTest(st.s);

    st.stop();
}
