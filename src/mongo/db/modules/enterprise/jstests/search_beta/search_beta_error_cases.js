/**
 * Test error conditions for the `$searchBeta` and `$_internalSearchBetaMongotRemote` aggregation
 * pipeline stages.
 */

(function() {
    "use strict";

    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();

    const dbName = "test";
    const collName = "search_beta_error_cases";
    const testDB = rst.getPrimary().getDB(dbName);
    testDB.dropDatabase();

    assert.commandWorked(testDB[collName].insert({_id: 0}));
    assert.commandWorked(testDB[collName].insert({_id: 1}));
    assert.commandWorked(testDB[collName].insert({_id: 2}));

    // $_internalSearchBetaMongotRemote cannot be used inside a transaction.
    let session = testDB.getMongo().startSession({readConcern: {level: "local"}});
    let sessionDb = session.getDatabase(dbName);

    session.startTransaction();
    assert.commandFailedWithCode(
        sessionDb.runCommand(
            {aggregate: collName, pipeline: [{$_internalSearchBetaMongotRemote: {}}], cursor: {}}),
        ErrorCodes.OperationNotSupportedInTransaction);
    session.endSession();

    // $_internalSearchBetaMongotRemote cannot be used inside a $facet subpipeline.
    let pipeline = [{$facet: {originalPipeline: [{$_internalSearchBetaMongotRemote: {}}]}}];
    let cmdObj = {aggregate: collName, pipeline: pipeline, cursor: {}};

    assert.commandFailedWithCode(testDB.runCommand(cmdObj), 40600);

    // $_internalSearchBetaMongotRemote is only valid as the first stage in a pipeline.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: collName,
        pipeline: [{$match: {}}, {$_internalSearchBetaMongotRemote: {}}],
        cursor: {},
    }),
                                 40602);

    // $searchBeta cannot be used inside a transaction.
    session = testDB.getMongo().startSession({readConcern: {level: "local"}});
    sessionDb = session.getDatabase(dbName);
    session.startTransaction();
    assert.commandFailedWithCode(
        sessionDb.runCommand({aggregate: collName, pipeline: [{$searchBeta: {}}], cursor: {}}),
        ErrorCodes.OperationNotSupportedInTransaction);
    session.endSession();

    // $searchBeta cannot be used inside a $facet subpipeline.
    pipeline = [{$facet: {originalPipeline: [{$searchBeta: {}}]}}];
    cmdObj = {aggregate: collName, pipeline: pipeline, cursor: {}};

    assert.commandFailedWithCode(testDB.runCommand(cmdObj), 40600);

    // $searchBeta is only valid as the first stage in a pipeline.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: collName,
        pipeline: [{$match: {}}, {$searchBeta: {}}],
        cursor: {},
    }),
                                 40602);

    // Make sure the server is still up.
    assert.commandWorked(testDB.runCommand("ping"));

    rst.stopSet();
})();
