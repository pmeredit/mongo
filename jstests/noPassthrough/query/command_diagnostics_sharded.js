/**
 * Test that tassert during CRUD command execution on mongos will log diagnostics about the query.
 *
 * This test expects collections to persist across a restart.
 * @tags: [requires_persistence]
 */
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {
    assertOnDiagnosticLogContents,
    queryPlannerAlwaysFails,
    runWithFailpoint
} from "jstests/libs/query/command_diagnostic_utils.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

const hasEnterpriseModule = getBuildInfo().modules.includes("enterprise");
const dbName = "test";
const collName = jsTestName();
const ns = dbName + "." + collName;

/**
 * Runs 'command' with the queryPlannerAlwaysFails failpoint enabled, finds the resulting
 * ScopedDebugInfo diagnostic log line, and asserts that it contains 'expectedDiagnosticInfo'.
 */
function runTest({
    description,
    command,
    expectedDiagnosticInfo,
    redact = false,
}) {
    // Can't run cases that depend on log redaction without the enterprise module.
    if (!hasEnterpriseModule && redact) {
        return;
    }
    const db = st.s.getDB(dbName);
    if (hasEnterpriseModule) {
        assert.commandWorked(db.adminCommand({setParameter: 1, redactClientLogData: redact}));
    }

    // In addition to the particular diagnostic info expected per test case, all commands should
    // include the following.
    expectedDiagnosticInfo = expectedDiagnosticInfo.concat([
        "millis:",
        "locks: {}",
        "flowControl: {}",
    ]);

    print("Running test case:", tojson({description, command, expectedDiagnosticInfo, redact}));

    const {failpointName, failpointOpts, errorCode} = queryPlannerAlwaysFails;
    runWithFailpoint(db, failpointName, failpointOpts, () => {
        // BulkWrites don't fail if sub-operations fail, but they still generate the diagnostic log.
        if (!command.bulkWrite) {
            assert.commandFailedWithCode(db.runCommand(command), errorCode, description);
        } else {
            assert.commandWorked(db.adminCommand(command), description);
        }
    });

    assertOnDiagnosticLogContents({
        description: description,
        logFile: st.s.fullOptions.logFile,
        expectedDiagnosticInfo: expectedDiagnosticInfo
    });

    // We expect a non-zero exit code due to tassert triggered. Restarting will also clear the
    // log files that we grep in assertOnDiagnosticLogContents for the next test case.
    st.restartMongos(0, st.s0.opts, {allowedExitCode: MongoRunner.EXIT_ABRUPT});
}

// Constructs a collection sharded on fields a, b, and c. The queries below require shard targeting
// (see 'query'), which uses query planning as a sub-routine. Thus, mongos will hit the
// queryPlannerAlwaysFails failpoint.
const st = new ShardingTest({mongos: 1, shards: 1, other: {mongosOptions: {useLogFiles: true}}});
const db = st.s.getDB(dbName);
const coll = db[collName];
coll.drop();
assert.commandWorked(coll.createIndex({a: 1, b: 1, c: 1}));
assert.commandWorked(
    st.s.adminCommand({shardCollection: coll.getFullName(), key: {a: 1, b: 1, c: 1}}));
const query = {
    a: 1,
    b: 1
};

// Find
runTest({
    description: "find",
    command: {find: collName, filter: query, limit: 1},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { find: \\"${collName}\\", filter: { a: 1.0, b: 1.0 }, limit: 1.0`,
    ],
});
runTest({
    description: "find with log redaction",
    command: {find: collName, filter: query, limit: 1},
    redact: true,
    expectedDiagnosticInfo: [
        '{\'currentOp\': { op: \\"###\\", ns: \\"###\\"',
        '\'opDescription\': { find: \\"###\\", filter: { a: \\"###\\", b: \\"###\\" }, limit: \\"###\\"',
    ],
});

// Aggregate
runTest({
    description: "aggregate",
    command: {aggregate: collName, pipeline: [{$match: query}], cursor: {}},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { aggregate: \\"${
            collName}\\", pipeline: [ { $match: { a: 1.0, b: 1.0 } } ]`,
    ]
});

// Count
runTest({
    description: "count",
    command: {count: collName, query: query},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { count: \\"${collName}\\", query: { a: 1.0, b: 1.0 }`,
    ]
});

// Distinct
runTest({
    description: "distinct",
    command: {distinct: collName, key: "a", query: query},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { distinct: \\"${collName}\\", key: \\"a\\", query: { a: 1.0, b: 1.0 }`,
    ]
});

// MapReduce
runTest({
    description: "mapReduce",
    command: {mapReduce: collName, map: () => emit(0, 0), reduce: () => 1, out: {inline: 1}},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { mapReduce: \\"${
            collName}\\", map: () => emit(0, 0), reduce: () => 1, out: { inline: 1.0 }`,
    ]
});

// Delete
// TODO SERVER-98444: We should be able to see the delete sub-operations under "opDescription".
runTest({
    description: 'delete',
    command: {
        delete: collName,
        deletes: [{q: query, limit: 1}],
    },
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"remove\\", ns: \\"${ns}\\"`,
        '\'opDescription\': ',
    ]
});

// Update
// TODO SERVER-98444: We should be able to see the update sub-operations under "opDescription".
runTest({
    description: 'update with simple filter',
    command: {
        update: collName,
        updates: [{q: query, u: {a: 2, b: 2}}],
    },
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"update\\", ns: \\"${ns}\\"`,
        '\'opDescription\': ',
    ]
});

// FindAndModify
runTest({
    description: 'findAndModify remove',
    command: {
        findAndModify: collName,
        query: query,
        remove: true,
    },
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"${ns}\\"`,
        `\'opDescription\': { findAndModify: \\"${
            collName}\\", query: { a: 1.0, b: 1.0 }, remove: true`,
    ]
});

// BulkWrite
runTest({
    command: {
        bulkWrite: 1,
        ops: [{update: 0, filter: query, updateMods: {a: 1}}],
        nsInfo: [{ns: `test.${jsTestName()}`}],
    },
    description: 'bulkWrite fails',
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"bulkWrite\\", ns: \\"admin.$cmd\\"`,
        // Ensure sub-operations are included in the diagnostic log.
        '\'opDescription\': { bulkWrite: 1.0, ops: [ { update: 0.0, filter: { a: 1.0, b: 1.0 }, updateMods: { a: 1.0 } } ]',
    ]
});

// Explain
runTest({
    description: "explain find",
    command: {explain: {find: collName, filter: query, limit: 1}},
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"test.$cmd\\"`,
        `\'opDescription\': { explain: { find: \\"${
            collName}\\", filter: { a: 1.0, b: 1.0 }, limit: 1.0`,
    ],
});
runTest({
    description: "explain aggregate",
    command: {
        explain: {aggregate: collName, pipeline: [{$match: query}, {$unwind: "$arr"}], cursor: {}}
    },
    expectedDiagnosticInfo: [
        `{\'currentOp\': { op: \\"command\\", ns: \\"test.$cmd\\"`,
        `\'opDescription\': { explain: { aggregate: \\"${
            collName}\\", pipeline: [ { $match: { a: 1.0, b: 1.0 } }, { $unwind: \\"$arr\\" } ]`,
    ]
});

st.stop();
