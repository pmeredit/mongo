/**
 * Test the use of "explain" with the "$search" aggregation stage, but does not check the value of
 * executionStats.
 */
import {getAggPlanStage} from "jstests/libs/analyze_plan.js";
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {checkSbeRestrictedOrFullyEnabled} from "jstests/libs/sbe_util.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {MongotMock} from "jstests/with_mongot/mongotmock/lib/mongotmock.js";
import {
    getDefaultLastExplainContents,
    setUpMongotReturnExplain,
    setUpMongotReturnExplainAndCursor,
} from "jstests/with_mongot/mongotmock/lib/utils.js";
// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
const dbName = jsTestName();
const db = conn.getDB(dbName);
const coll = db.search;
coll.drop();

if (checkSbeRestrictedOrFullyEnabled(db) &&
    FeatureFlagUtil.isPresentAndEnabled(db.getMongo(), 'SearchInSbe')) {
    jsTestLog("Skipping the test because it only applies to $search in classic engine.");
    MongoRunner.stopMongod(conn);
    mongotmock.stop();
    quit();
}

assert.commandWorked(coll.insert({_id: 1, name: "Chekhov"}));
assert.commandWorked(coll.insert({_id: 2, name: "Dostoevsky"}));
assert.commandWorked(coll.insert({_id: 5, name: "Pushkin"}));
assert.commandWorked(coll.insert({_id: 6, name: "Tolstoy"}));
assert.commandWorked(coll.insert({_id: 8, name: "Zamatyin"}));

const collUUID = getUUIDFromListCollections(db, coll.getName());

const searchQuery = {
    query: "Chekhov",
    path: "name"
};
const explainContents = getDefaultLastExplainContents();

function checkExplain(result) {
    const searchStage = getAggPlanStage(result, "$_internalSearchMongotRemote");
    assert.neq(searchStage, null, result);
    const stage = searchStage["$_internalSearchMongotRemote"];
    assert(stage.hasOwnProperty("explain"), stage);
    assert.eq(explainContents, stage["explain"]);
}

function runExplainTest(currentVerbosity) {
    const searchCmd = {
        search: coll.getName(),
        collectionUUID: collUUID,
        query: searchQuery,
        explain: {verbosity: currentVerbosity},
        $db: dbName
    };
    // Give mongotmock some stuff to return.
    setUpMongotReturnExplain({
        searchCmd,
        mongotMock: mongotmock,
    });
    const result = coll.explain(currentVerbosity).aggregate([{$search: searchQuery}]);
    checkExplain(result);
}

function runExplainAndCursorTest(currentVerbosity) {
    const searchCmd = {
        search: coll.getName(),
        collectionUUID: collUUID,
        query: searchQuery,
        explain: {verbosity: currentVerbosity},
        $db: dbName
    };
    setUpMongotReturnExplainAndCursor({
        mongotMock: mongotmock,
        coll,
        searchCmd,
        nextBatch: [
            {_id: 1, $searchScore: 0.321},
        ],
    });
    const result = coll.explain(currentVerbosity).aggregate([{$search: searchQuery}]);
    checkExplain(result);
}

runExplainTest("queryPlanner");
// TODO SERVER-91594: Testing "executionStats" and "allPlansExecution" for runExplainTest() is not
// necessary when mongot will not return that.
runExplainTest("executionStats");
runExplainTest("allPlansExecution");

runExplainAndCursorTest("executionStats");
runExplainAndCursorTest("allPlansExecution");
MongoRunner.stopMongod(conn);
mongotmock.stop();
