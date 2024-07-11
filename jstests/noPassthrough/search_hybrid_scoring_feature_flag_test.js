/**
 * Test that checks a call to $rankFusion fails when search hybrid scoring feature flag is turned
 * off.
 */

// TODO SERVER-85426 Remove this test when 'featureFlagSearchHybridScoring' is removed.
const conn = MongoRunner.runMongod({
    setParameter: {featureFlagSearchHybridScoring: false},
});
assert.neq(null, conn, 'failed to start mongod');
const testDB = conn.getDB('test');

// Pipeline to run $rankFusion should fail without feature flag turned on.
assert.commandFailedWithCode(
    testDB.runCommand({aggregate: 1, pipeline: [{$rankFusion: {}}], cursor: {}}),
    ErrorCodes.QueryFeatureNotAllowed);

MongoRunner.stopMongod(conn);
