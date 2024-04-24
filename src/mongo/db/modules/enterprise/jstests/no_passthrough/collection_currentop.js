/**
 * Verify sensitive information is omitted for QE currentOp operations.
 *
 * @tags: [
 * requires_fcv_70,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";

const dbName = 'collection_currentop';

function setPostCommandFailpoint(db, {mode, options}) {
    assert.commandWorked(db.adminCommand(
        {configureFailPoint: "waitAfterCommandFinishesExecution", mode: mode, data: options}));
}

async function bgRunCmdFunc(command) {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "collection_currentop");
    let edb = client.getDB();

    if (command.hasOwnProperty('find')) {
        client.getDB().erunCommand(command);
    } else {
        client.getDB().erunCommand({find: edb.basic.getName(), filter: {}});
        let resultFind =
            client.getDB().erunCommand({find: edb.basic.getName(), filter: {}, batchSize: 1});

        assert.eq(resultFind.ok, 1, " RESULT: " + tojson(resultFind));
        let cid = resultFind.cursor.id;

        let cr = undefined;
        do {
            cr = client.getDB().erunCommand({
                "getMore": NumberLong(cid),
                collection: edb.basic.getName(),
                batchSize: 1,
                comment: {testName: jsTestName(), commentField: "comment_getMore"}
            });

            assert.eq(cr.ok, 1, "RESULT2: " + tojson(cr));

        } while (cr.cursor.nextBatch.length > 0);
    }
}

function assertNoEntries(res, isGetMore) {
    assert(res.hasOwnProperty("redacted"));
    assert.eq(res.redacted, true);

    assert(res.hasOwnProperty("command"));
    assert(!res.command.hasOwnProperty("filter"));
    assert(!res.command.hasOwnProperty("$clusterTime"));
    assert(!res.command.hasOwnProperty("$encryptionInformation"));
    assert(!res.command.hasOwnProperty("$readPreference"));
    assert(!res.command.hasOwnProperty("cursor"));
    assert(!res.command.hasOwnProperty("lsid"));
    assert(!res.command.hasOwnProperty("$clusterTime"));

    assert(!res.hasOwnProperty("queryFramework"));
    assert(!res.hasOwnProperty("numYields"));

    assert(!res.hasOwnProperty("locks"));
    assert(!res.hasOwnProperty("waitingForLock"));

    assert(!res.hasOwnProperty("waitingForlockStatsLock"));
    assert(!res.hasOwnProperty("waitingForFlowControl"));
    assert(!res.hasOwnProperty("flowControlStats"));

    if (isGetMore) {
        assert(!res.command.hasOwnProperty("find"));
        assert(res.command.hasOwnProperty("getMore"));
        assert(res.command.hasOwnProperty("collection"));
    } else {
        assert(res.command.hasOwnProperty("find"));
        assert(!res.command.hasOwnProperty("getMore"));
        assert(!res.command.hasOwnProperty("collection"));
    }
}

function runCommentParamTest(conn, {coll, command}) {
    const db = conn.getDB(dbName);
    const adminDB = conn.getDB("admin");

    const isGetMore = !command.hasOwnProperty('find');
    let cmdName = Object.keys(command)[0];

    if (isGetMore) {
        cmdName = "getMore";
    }

    let commentObj = {testName: jsTestName(), commentField: "comment_" + cmdName};
    command["comment"] = commentObj;

    let parallelShell;
    try {
        setPostCommandFailpoint(
            db, {mode: "alwaysOn", options: {ns: coll.getFullName(), commands: [cmdName]}});

        let shellArgs = [];

        // Run the 'command' in a parallel shell.
        parallelShell =
            startParallelShell(funWithArgs(bgRunCmdFunc, command), conn.port, false, ...shellArgs);

        // Wait for the parallel shell to hit the failpoint and verify that the 'comment' field is
        // present in $currentOp.
        const filter = {
            "ns": coll.getFullName(),
            "redacted": true,
            "command.comment": commentObj,
            "command.$db": dbName
        };

        let res;
        assert.soon(() => {
            res = adminDB.aggregate([{$currentOp: {}}, {$match: filter}]).toArray();
            return res.length == 1;
        }, () => tojson(adminDB.aggregate([{$currentOp: {}}]).toArray()));

        assertNoEntries(res[0], isGetMore);
    } finally {
        // Ensure that we unset the failpoint, regardless of the outcome of the test.
        setPostCommandFailpoint(db, {mode: "off", options: {}});
    }
    // Wait for the parallel shell to complete.
    parallelShell();
}

function runTest(conn) {
    // This test runs manual getMores using different connections, which will not inherit the
    // implicit session of the cursor establishing command.
    // TestData.disableImplicitSessions = true;

    const client = new EncryptedClient(conn, dbName);

    const edb = client.getDB();

    jsTest.log("createEncryptionCollection");
    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "middle", "bsonType": "string"},
            ]
        }
    }));

    assert.commandWorked(edb.basic.einsert({first: "mark", count: 1}));
    assert.commandWorked(edb.basic.einsert({first: "mark", count: 2}));
    assert.commandWorked(edb.basic.einsert({first: "mark", count: 3}));
    assert.commandWorked(edb.basic.einsert({first: "mark", count: 4}));
    assert.commandWorked(edb.basic.einsert({first: "mark", count: 5}));
    assert.commandWorked(edb.basic.einsert({first: "mark", count: 6}));

    // Verify currentOp on encrypted collection returns redacted information for find command.
    runCommentParamTest(conn, {coll: edb.basic, command: {find: edb.basic.getName(), filter: {}}});

    // Verify currentOp on encrypted collection returns redacted information for getMore command.
    runCommentParamTest(conn, {coll: edb.basic, command: {"first": "mark"}});
}

const rst = new ReplSetTest({nodes: 1});
rst.startSet();

rst.initiate();
rst.awaitReplication();

runTest(rst.getPrimary());

rst.stopSet();
