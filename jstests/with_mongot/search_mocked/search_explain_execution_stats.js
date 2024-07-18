/**
 * Test the use of "explain" with the "$search" aggregation stage. This tests  "executionStats" and
 * "allPlansExecution" verbosities and checks that they function as expected.
 * @tags: [featureFlagSearchExplainExecutionStats]
 *
 */
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {checkSbeRestrictedOrFullyEnabled} from "jstests/libs/sbe_util.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {
    mongotCommandForQuery,
    MongotMock,
} from "jstests/with_mongot/mongotmock/lib/mongotmock.js";
import {
    getDefaultLastExplainContents,
    getSearchStagesAndVerifyExplainOutput,
    setUpMongotReturnExplain,
    setUpMongotReturnExplainAndCursor,
    setUpMongotReturnExplainAndCursorGetMore,
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
const collName = coll.getName();

const explainObject = getDefaultLastExplainContents();

if (checkSbeRestrictedOrFullyEnabled(db) &&
    FeatureFlagUtil.isPresentAndEnabled(db.getMongo(), 'SearchInSbe')) {
    jsTestLog("Skipping the test because it only applies to $search in classic engine.");
    MongoRunner.stopMongod(conn);
    mongotmock.stop();
    quit();
}

assert.commandWorked(coll.insert({_id: 1, name: "Sozin", element: "fire"}));
assert.commandWorked(coll.insert({_id: 2, name: "Zuko", element: "fire"}));
assert.commandWorked(coll.insert({_id: 3, name: "Rangi", element: "fire"}));
assert.commandWorked(coll.insert({_id: 4, name: "Azulon", element: "fire"}));

const collUUID = getUUIDFromListCollections(db, coll.getName());

const searchQuery = {
    query: "fire",
    path: "element"
};

function runExplainTest(verbosity) {
    const searchCmd = mongotCommandForQuery({
        query: searchQuery,
        collName: collName,
        db: dbName,
        collectionUUID: collUUID,
        explainVerbosity: {verbosity}
    });
    const pipeline = [{$search: searchQuery}];

    // TODO SERVER-91594: Test for setUpMongotReturnExplain() can be removed when mongot always
    // returns a cursor.
    {
        setUpMongotReturnExplain({
            searchCmd,
            mongotMock: mongotmock,
        });
        const result = coll.explain(verbosity).aggregate(pipeline);
        getSearchStagesAndVerifyExplainOutput({
            result,
            stageType: "$_internalSearchMongotRemote",
            verbosity,
            nReturned: NumberLong(0),
            nReturnedIdLookup: NumberLong(0),
            explainObject
        });
    }
    {
        setUpMongotReturnExplainAndCursor({
            mongotMock: mongotmock,
            coll,
            searchCmd,
            nextBatch: [
                {_id: 3, $searchScore: 100},
                {_id: 2, $searchScore: 10},
                {_id: 4, $searchScore: 1},
                {_id: 8, $searchScore: 0.2},
            ],
        });
        const result = coll.explain(verbosity).aggregate(pipeline);
        getSearchStagesAndVerifyExplainOutput({
            result,
            stageType: "$_internalSearchMongotRemote",
            verbosity,
            nReturned: NumberLong(4),
            nReturnedIdLookup: NumberLong(3),
            explainObject
        });
    }
    {
        setUpMongotReturnExplainAndCursorGetMore({
            mongotMock: mongotmock,
            coll,
            searchCmd,
            batchList: [
                [{_id: 3, $searchScore: 100}, {_id: 2, $searchScore: 10}],
                [{_id: 4, $searchScore: 1}, {_id: 1, $searchScore: 0.99}],
                [{_id: 8, $searchScore: 0.2}]
            ],
        });
        const result = coll.explain(verbosity).aggregate(pipeline, {cursor: {batchSize: 2}});
        getSearchStagesAndVerifyExplainOutput({
            result,
            stageType: "$_internalSearchMongotRemote",
            verbosity,
            nReturned: NumberLong(5),
            nReturnedIdLookup: NumberLong(4),
            explainObject
        });
    }
}

runExplainTest("executionStats");
runExplainTest("allPlansExecution");
MongoRunner.stopMongod(conn);
mongotmock.stop();
