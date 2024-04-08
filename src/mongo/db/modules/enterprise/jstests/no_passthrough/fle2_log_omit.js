/**
 * Test log omission works correctly for various queryable encryption operations on mongos, shards
 * and replica sets.
 *
 * Runs QE and non-QE operations. Ensure the QE operations are not logged while the normal
 * operations are logged.
 *
 * @tags: [
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const testDBName = "test";
let logCountCheck = 1;
let explainLogCountCheck = 1;

const good_ns = "test.goodlog";
const bad_coll_ns = "badlog";
const slowQueryId = 51803;

// Assert a log count of zero with certain things allowed
function assertLogCountAllowList(db, filter, count) {
    const messagesOrig = checkLog.getFilteredLogMessages(db, slowQueryId, filter);

    const allowList = ["renameCollection", "create", "killCursors"];

    let messages = [];
    messagesOrig.forEach((m) => {
        let notAllowed = true;
        for (let item of allowList) {
            if (m.attr.command.hasOwnProperty(item)) {
                notAllowed = false;
            }
        }

        if (notAllowed) {
            messages.push(m);
        }
    });

    const actual = messages.length;

    if (count != actual) {
        print(
            "\n==========================================================================================================EXTRA MESSAGES:\n " +
            tojson(messages) +
            "\n==========================================================================================================");
        assert(false,
               `Wrong log message count for: Expected: ${count}, Actual: ${actual}, ${
                   tojson(filter)}`);
    }
}

function assertNoBadLogs(db) {
    const dbName = db.getName();

    assertLogCountAllowList(db, {"ns": `${dbName}.${bad_coll_ns}`}, 0);
    assertLogCountAllowList(db, {"ns": `${dbName}.enxcol_.${bad_coll_ns}.esc`}, 0);
    assertLogCountAllowList(db, {"ns": `${dbName}.enxcol_.${bad_coll_ns}.ecoc`}, 0);
}

function checkLogCounts(db) {
    print(`Checking logs: ${logCountCheck}`);

    assertNoBadLogs(db);
    assertLogCountAllowList(db, {"ns": good_ns}, logCountCheck);

    logCountCheck++;
}

// Assert a log count but only for explain
function assertLogCountsExplain(db, filter, count) {
    const messagesOrig = checkLog.getFilteredLogMessages(db, slowQueryId, filter);

    let messages = [];
    messagesOrig.forEach((m) => {
        if (m.attr.command.hasOwnProperty("explain")) {
            messages.push(m);
        }
    });

    const actual = messages.length;

    if (count != actual) {
        print(
            "\n==========================================================================================================EXTRA MESSAGES:\n " +
            tojson(messages) +
            "\n==========================================================================================================");
        assert(false,
               `Wrong log message count for: Expected: ${count}, Actual: ${actual}, ${
                   tojson(filter)}`);
    }
}

// Some explain commands show up as "db.$cmd" as the namespace because they are run through
// runCommand
function checkExplainDollarLogCounts(db) {
    const dbName = db.getName();
    print(`Checking explain dollar logs: ${explainLogCountCheck}`);
    assertLogCountsExplain(db, {"ns": `${dbName}.$cmd`}, explainLogCountCheck);
    explainLogCountCheck++;
}

// Some explain commands show up as "db.$cmd" as the namespace in mongos but as "db.coll" in mongod
function checkExplainLogCounts(db) {
    print(`Checking explain logs: ${explainLogCountCheck}`);
    if (db.getMongo().isMongos()) {
        checkExplainDollarLogCounts(db);
    } else {
        checkLogCounts(db);
    }
}

function runTest(conn, alt_conn) {
    logCountCheck = 1;
    explainLogCountCheck = 1;

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

    assert.commandWorked(db.adminCommand({clearLog: 'global'}));

    if (alt_conn !== undefined) {
        let altDB = alt_conn.getDB(testDBName);
        assert.commandWorked(altDB.adminCommand({clearLog: 'global'}));
    }

    // Insert
    //
    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));

    checkLogCounts(db);

    // Compact
    //
    assert.commandWorked(edb.badlog.compact());
    // Do a find to add log line for the assertions to work correctly
    assert.eq(db.goodlog.find({first: "luke"}).itcount(), 1);

    checkLogCounts(db);

    // Cleanup
    //
    assert.commandWorked(edb.badlog.cleanup());
    // Do a find to add log line for the assertions to work correctly
    assert.eq(db.goodlog.find({first: "luke"}).itcount(), 1);

    checkLogCounts(db);

    // Find
    //
    assert.eq(edb.badlog.find({first: "mark"}).itcount(), 1);
    assert.eq(db.goodlog.find({first: "luke"}).itcount(), 1);

    checkLogCounts(db);

    assert.commandWorked(edb.badlog.find({first: "mark"}).explain());
    assert.commandWorked(db.goodlog.find({first: "luke"}).explain());

    checkExplainLogCounts(db);

    // Count
    //
    assert.eq(edb.badlog.count({first: "mark"}), 1);
    assert.eq(db.goodlog.count({first: "luke"}), 1);

    checkLogCounts(db);

    assert.commandWorked(edb.badlog.explain().count({first: "mark"}));
    assert.commandWorked(db.goodlog.explain().count({first: "luke"}));

    checkExplainLogCounts(db);

    // Aggregate
    //
    assert.eq(edb.badlog.aggregate([{$match: {"first": "mark"}}]).itcount(), 1);
    assert.eq(db.goodlog.aggregate([{$match: {"first": "luke"}}]).itcount(), 1);

    checkLogCounts(db);

    assert.commandWorked(edb.badlog.explain().aggregate([{$match: {"first": "mark"}}]));
    assert.commandWorked(db.goodlog.explain().aggregate([{$match: {"first": "luke"}}]));

    // aggregate explain looks like normal aggregate with a flag explain: true
    checkLogCounts(db);

    // FindAndModify
    //
    assert.commandWorked(edb.badlog.runCommand({
        findAndModify: edb.badlog.getName(),
        query: {"first": "mark"},
        update: {$set: {"last": "marco"}}
    }));
    assert.commandWorked(db.goodlog.runCommand({
        findAndModify: db.goodlog.getName(),
        query: {"first": "luke"},
        update: {$set: {"last": "marco"}}
    }));

    checkLogCounts(db);

    assert.eq(edb.badlog.find({last: "marco"}).itcount(), 1);
    assert.eq(db.goodlog.find({last: "marco"}).itcount(), 1);

    checkLogCounts(db);

    assert.commandWorked(edb.badlog.runCommand({
        explain: {
            findAndModify: edb.badlog.getName(),
            query: {"first": "mark"},
            update: {$set: {"last": "marco"}}
        }
    }));
    assert.commandWorked(db.goodlog.runCommand({
        explain: {
            findAndModify: db.goodlog.getName(),
            query: {"first": "luke"},
            update: {$set: {"last": "marco"}}
        }
    }));

    checkExplainDollarLogCounts(db);

    // Update
    //
    assert.commandWorked(edb.badlog.updateOne({"first": "mark"}, {$set: {"last": "markus"}}));
    assert.commandWorked(db.goodlog.updateOne({"first": "luke"}, {$set: {"last": "markus"}}));

    checkLogCounts(db);

    assert.eq(edb.badlog.find({last: "markus"}).itcount(), 1);
    assert.eq(db.goodlog.find({last: "markus"}).itcount(), 1);

    checkLogCounts(db);

    assert.commandWorked(edb.badlog.runCommand({
        explain: {
            update: edb.badlog.getName(),
            updates: [{q: {"first": "mark"}, u: {$set: {"last": "marco"}}}]
        }
    }));
    assert.commandWorked(db.goodlog.runCommand({
        explain: {
            update: db.goodlog.getName(),
            updates: [{q: {"first": "luke"}, u: {$set: {"last": "marco"}}}]
        }
    }));

    checkExplainDollarLogCounts(db);

    // Delete
    //
    assert.commandWorked(edb.badlog.runCommand(
        {explain: {delete: edb.badlog.getName(), deletes: [{q: {"first": "mark"}, limit: 1}]}}));
    assert.commandWorked(db.goodlog.runCommand(
        {explain: {delete: db.goodlog.getName(), deletes: [{q: {"first": "luke"}, limit: 1}]}}));

    checkExplainDollarLogCounts(db);

    assert.commandWorked(edb.badlog.deleteOne({"first": "mark"}));

    assert.commandWorked(db.goodlog.deleteOne({"first": "luke"}));

    checkLogCounts(db);

    assert.eq(edb.badlog.find({}).itcount(), 0);
    assert.eq(db.goodlog.find({}).itcount(), 0);

    checkLogCounts(db);

    // Batch Size & getMore
    //
    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));
    checkLogCounts(db);

    assert.commandWorked(edb.badlog.insert({first: "mark", middle: "john"}));
    assert.commandWorked(db.goodlog.insert({first: "luke", middle: "john"}));
    checkLogCounts(db);

    let cursorEncrypted = edb.badlog.find({first: "mark"}).batchSize(1);
    assert.eq(cursorEncrypted.objsLeftInBatch(), 1);
    assert.eq(cursorEncrypted.hasNext(), true);

    let cursorUnencrypted = db.goodlog.find({first: "luke"}).batchSize(1);
    assert.eq(cursorUnencrypted.objsLeftInBatch(), 1);
    assert.eq(cursorUnencrypted.hasNext(), true);

    checkLogCounts(db);

    // Force a call to getMore
    let batchE = cursorEncrypted.next();
    assert.eq(cursorEncrypted.hasNext(), true);

    let batchU = cursorUnencrypted.next();
    assert.eq(cursorUnencrypted.hasNext(), true);
    checkLogCounts(edb);
}

jsTestLog("ReplicaSet: Testing fle2 log omission");
{
    const rst = new ReplSetTest({nodes: 1, nodeOptions: {verbose: 1}});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), undefined);
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 log omission");
{
    const st =
        new ShardingTest({shards: [{verbose: 1}], mongos: [{verbose: 1}], config: [{verbose: 1}]});
    runTest(st.s, st.shard0);

    // Check the logs at the end on the shard to make sure mongos commands are not leaked on the
    // shard
    let shardDB = st.shard0.getDB(testDBName);
    assertNoBadLogs(shardDB);

    // Check also only the good log explains show up in the shards logs
    assertLogCountsExplain(shardDB, {"ns": `${testDBName}.$cmd`}, 3);

    st.stop();
}
