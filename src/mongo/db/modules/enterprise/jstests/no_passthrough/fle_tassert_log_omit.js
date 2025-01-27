/**
 * Test log omission on tassert works correctly for various queryable encryption operations. Runs QE
 * operations that hit tasserts and ensures that the resulting ScopedDebugInfo log line does not
 * contain operation details.
 *
 * This test expects collections to persist across a restart.
 * @tags: [requires_persistence]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {FixtureHelpers} from "jstests/libs/fixture_helpers.js";
import {
    failAllInserts,
    getDiagnosticLogs,
    planExecutorAlwaysFails,
    queryPlannerAlwaysFails,
    runWithFailpoint
} from "jstests/libs/query/command_diagnostic_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

// All commands below will reference the encrypted field "a" (see 'query'); when we tassert during
// command processing, we should not see the command in the ScopeDebugInfo log line.
const dbName = "test";
const collName = jsTestName();
function getEncryptedFields() {
    return {
        encryptedFields: {
            "fields": [
                {"path": "a", "bsonType": "int", "queries": {"queryType": "equality"}},
            ]
        }
    };
}
const query = {
    a: NumberInt(1),
    b: 1
};

function runTest(conn, restartFn) {
    const db = conn.getDB(dbName);
    const client = new EncryptedClient(db.getMongo(), dbName);

    // Runs 'command' with 'failpoint' enabled, asserts that a ScopedDebugInfo log line is
    // logged, and that it does not contain the command description.
    function runEncryptedCommandAndCheckLog({description, command, failpoint}) {
        jsTestLog("Running test case: " + tojson(description));

        let edb = client.getDB();
        runWithFailpoint(edb, failpoint.failpointName, failpoint.failpointOpts, () => {
            if (!command.bulkWrite) {
                assert.commandFailedWithCode(edb.erunCommand(command), failpoint.errorCode);
            } else {
                assert.commandWorked(edb.eadminCommand(command));
            }
        });

        // Ignore any diagnostic log lines for queries that may be failing in the background.
        const diagnostics = getDiagnosticLogs({
                                description: description,
                                logFile: conn.fullOptions.logFile
                            }).filter(line => line.includes(collName) || !line.includes("ns: "));
        assert(diagnostics.length > 0, "Failed to find relevant ScopedDebugInfo log line");

        // Log lines should not contain the command contents. There may be multiple ScopedDebugInfo
        // log lines, and each one can contain multiple entries depending on the available printers.
        assert(diagnostics.every(line => line.includes("curOpDiagnostics: omitted") &&
                                     !line.includes("opDescription")),
               "Found a log line containing command diagnostics, when none were expected " +
                   tojson(diagnostics));

        // Each test case restarts the required node to clear the previous logs.
        restartFn();
    }

    // Avoid running these test cases on mongos, since the required failpoints do not exist there.
    if (!FixtureHelpers.isMongos(db)) {
        // GetMore
        const {cursor} =
            assert.commandWorked(client.getDB().erunCommand({find: collName, batchSize: 0}));
        runEncryptedCommandAndCheckLog({
            failpoint: planExecutorAlwaysFails,
            description: "getMore",
            command: {getMore: cursor.id, collection: collName}
        });

        // Insert
        runEncryptedCommandAndCheckLog({
            failpoint: failAllInserts,
            description: "insert",
            command: {insert: collName, documents: [query], ordered: false},
        });
    }

    // Find
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: "simple find",
        command: {find: collName, filter: query, limit: 1},
    });

    // Aggregate
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: "agg with a simple $match",
        command: {aggregate: collName, pipeline: [{$match: query}], cursor: {}},
    });

    // Count
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: "count",
        command: {count: collName, query: query},
    });

    // Delete
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'delete',
        command: {
            delete: collName,
            deletes: [{q: query, limit: 1}],
        },
    });

    // Update
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'update with simple filter',
        command: {
            update: collName,
            updates: [{q: query, u: {b: 2}}],
        },
    });

    // FindAndModify
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'findAndModify remove',
        command: {
            findAndModify: collName,
            query: query,
            remove: true,
        },
    });
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'findAndModify update',
        command: {
            findAndModify: collName,
            query: query,
            update: {b: 2},
        },
    });

    // BulkWrite
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'bulkWrite update',
        command: {
            bulkWrite: 1,
            ops: [
                {update: 0, filter: query, updateMods: {b: 1}},
            ],
            nsInfo: [{ns: `${dbName}.${collName}`}],
        },
        // The top-level command doesn't fail when an update fails.
        errorCode: 0,
    });

    // Explain
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'explain find',
        command: {
            explain: {find: collName, filter: query, limit: 1},
        },
    });
    runEncryptedCommandAndCheckLog({
        failpoint: queryPlannerAlwaysFails,
        description: 'explain aggregate',
        command: {aggregate: collName, pipeline: [{$match: query}, {$unwind: "$arr"}], cursor: {}},
    });
}

jsTestLog("ReplicaSet: Testing fle2 tassert log omission");
{
    const testFixture = new ReplSetTest({nodes: 1, nodeOptions: {useLogFiles: true}});
    testFixture.startSet();
    testFixture.initiate();
    testFixture.awaitReplication();

    const db = testFixture.getPrimary().getDB(dbName);
    db.dropDatabase();
    const client = new EncryptedClient(db.getMongo(), dbName);
    assert.commandWorked(client.createEncryptionCollection(collName, getEncryptedFields()));

    runTest(testFixture.getPrimary(), () => {
        testFixture.restart(testFixture.getPrimary(), {allowedExitCode: MongoRunner.EXIT_ABRUPT});
        testFixture.waitForPrimary();
    });
    testFixture.stopSet();
}

jsTestLog("ShardingTest: Testing fle2 tassert log omission on shards");
{
    const fixture = new ShardingTest({shards: [{useLogFiles: true}], mongos: 1});

    const db = fixture.s.getDB(dbName);
    db.dropDatabase();
    const client = new EncryptedClient(db.getMongo(), dbName);
    assert.commandWorked(client.createEncryptionCollection(collName, getEncryptedFields()));

    runTest(fixture.rs0.getPrimary(), () => {
        fixture.rs0.restart(fixture.rs0.getPrimary(), {allowedExitCode: MongoRunner.EXIT_ABRUPT});
        fixture.rs0.waitForPrimary();
    });
    fixture.stop();
}

jsTestLog("ShardingTest: Testing fle2 tassert log omission on mongos");

{
    const fixture = new ShardingTest({shards: 1, mongos: [{useLogFiles: true}]});

    const db = fixture.s.getDB(dbName);
    db.dropDatabase();
    const client = new EncryptedClient(db.getMongo(), dbName);
    assert.commandWorked(client.createEncryptionCollection(collName, getEncryptedFields()));

    // Shard the collection on 'b' and 'c', and run queries on fields 'a' and 'b'. This requires
    // most of the test cases above to use shard targeting, which uses query planning as a
    // sub-routine. Thus, mongos will hit the queryPlannerAlwaysFails failpoint.
    const coll = db[collName];
    assert.commandWorked(coll.createIndex({b: 1, c: 1}));
    assert.commandWorked(
        fixture.s.adminCommand({shardCollection: coll.getFullName(), key: {b: 1, c: 1}}));

    runTest(
        fixture.s,
        () => fixture.restartMongos(0, fixture.s.opts, {allowedExitCode: MongoRunner.EXIT_ABRUPT}));
    fixture.stop();
}
