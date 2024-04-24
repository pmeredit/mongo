/**
 * Test log omission works correctly for various queryable encryption operations on mongos, shards
 * and replica sets.
 *
 * Runs QE and non-QE operations. Ensure the QE operations are not logged while the normal
 * operations are logged.
 *
 * @tags: [
 *   requires_fcv_80
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const testDBName = "test";
let logCountCheck = 1;
let bulkWriteMongosLogCountCheck = 1;

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

    checkLogCountsBulkWrite(db, `${dbName}.${bad_coll_ns}`, 0);
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

// BulkWrite logs 2 types of Slow query messages on Mongod, and only one on Mongos.
// Also, bulkWrite stores the namespaces below its nsInfo field:
//
// Mongod only:
//   "c":"WRITE", "id":51803, "msg":"Slow query","attr":{"type":"bulkWrite","ns":"test.goodlog" ...
// Mongod and Mongos:
//   "c":"COMMAND", "id":51803, "msg":"Slow query", "attr":{"type":"command","ns":"admin.$cmd",
//   "command":{"bulkWrite":1,"ops":[...],"nsInfo":[{"ns":"test.goodlog" ...
//
function checkLogCountsBulkWrite(db, expectedNs, count) {
    const filter = {};
    const messagesOrig = checkLog.getFilteredLogMessages(db, slowQueryId, filter);

    let messages = [];
    messagesOrig.forEach((m) => {
        if ((m.attr.command.hasOwnProperty("bulkWrite") &&
             m.attr.command.nsInfo.some((entry) => entry.ns == expectedNs)) ||
            (m.attr.type == "bulkWrite" && m.attr.ns == expectedNs)) {
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

function checkBulkWriteLogCounts(db) {
    if (db.getMongo().isMongos()) {
        assertNoBadLogs(db);
        checkLogCountsBulkWrite(db, `${good_ns}`, bulkWriteMongosLogCountCheck);
        bulkWriteMongosLogCountCheck++;
    } else {
        checkLogCounts(db);
    }
}

function runTest(conn, alt_conn) {
    logCountCheck = 1;

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

    assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{insert: 0, document: {first: "mark", middle: "john"}}],
        nsInfo: [{ns: "test.badlog"}]
    }));
    assert.commandWorked(db.adminCommand({
        bulkWrite: 1,
        ops: [{insert: 0, document: {first: "luke", middle: "john"}}],
        nsInfo: [{ns: "test.goodlog"}]
    }));

    checkBulkWriteLogCounts(db);

    assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {first: "mark"}, updateMods: {$set: {middle: "paul"}}}],
        nsInfo: [{ns: "test.badlog"}]
    }));
    assert.commandWorked(db.adminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {first: "luke"}, updateMods: {$set: {middle: "paul"}}}],
        nsInfo: [{ns: "test.goodlog"}]
    }));

    checkBulkWriteLogCounts(db);

    assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{delete: 0, filter: {first: "mark"}}],
        nsInfo: [{ns: "test.badlog"}]
    }));
    assert.commandWorked(db.adminCommand({
        bulkWrite: 1,
        ops: [{delete: 0, filter: {first: "luke"}}],
        nsInfo: [{ns: "test.goodlog"}]
    }));

    checkBulkWriteLogCounts(db);
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

    st.stop();
}
