/**
 * Test error conditions for the `$search` and `$_internalSearchMongotRemote` aggregation
 * pipeline stages.
 */

(function() {
"use strict";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "search_error_cases";
const testDB = rst.getPrimary().getDB(dbName);
testDB.dropDatabase();

assert.commandWorked(testDB[collName].insert({_id: 0}));
assert.commandWorked(testDB[collName].insert({_id: 1}));
assert.commandWorked(testDB[collName].insert({_id: 2}));

// $_internalSearchMongotRemote cannot be used inside a transaction.
let session = testDB.getMongo().startSession({readConcern: {level: "local"}});
let sessionDb = session.getDatabase(dbName);

session.startTransaction();
assert.commandFailedWithCode(
    sessionDb.runCommand(
        {aggregate: collName, pipeline: [{$_internalSearchMongotRemote: {}}], cursor: {}}),
    ErrorCodes.OperationNotSupportedInTransaction);
session.endSession();

// $_internalSearchMongotRemote cannot be used inside a $facet subpipeline.
let pipeline = [{$facet: {originalPipeline: [{$_internalSearchMongotRemote: {}}]}}];
let cmdObj = {aggregate: collName, pipeline: pipeline, cursor: {}};

assert.commandFailedWithCode(testDB.runCommand(cmdObj), 40600);

// $_internalSearchMongotRemote is only valid as the first stage in a pipeline.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: collName,
    pipeline: [{$match: {}}, {$_internalSearchMongotRemote: {}}],
    cursor: {},
}),
                             40602);

// $search cannot be used inside a transaction.
session = testDB.getMongo().startSession({readConcern: {level: "local"}});
sessionDb = session.getDatabase(dbName);
session.startTransaction();
assert.commandFailedWithCode(
    sessionDb.runCommand({aggregate: collName, pipeline: [{$search: {}}], cursor: {}}),
    ErrorCodes.OperationNotSupportedInTransaction);
session.endSession();

// $search cannot be used inside a $facet subpipeline.
pipeline = [{$facet: {originalPipeline: [{$search: {}}]}}];
cmdObj = {
    aggregate: collName,
    pipeline: pipeline,
    cursor: {}
};

assert.commandFailedWithCode(testDB.runCommand(cmdObj), 40600);

// $search is only valid as the first stage in a pipeline.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: collName,
    pipeline: [{$match: {}}, {$search: {}}],
    cursor: {},
}),
                             40602);

// $search is not allowed in an update pipeline. Error code matters on version.
assert.commandFailedWithCode(
    testDB.runCommand({"findandmodify": collName, "update": [{"$search": {}}]}),
    [6600901, ErrorCodes.InvalidOptions]);

// Make sure the server is still up.
assert.commandWorked(testDB.runCommand("ping"));

rst.stopSet();
})();
