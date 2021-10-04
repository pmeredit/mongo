/**
 * Verify that `$searchMeta` extracts SEARCH_META variable returned by mongot.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load("jstests/libs/analyze_plan.js");  // For getAggPlanStages().
load('jstests/libs/uuid_util.js');     // For getUUIDFromListCollections.

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});

const dbName = jsTestName();
const testDB = conn.getDB(dbName);

const coll = testDB.searchCollector;
coll.drop();
assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
assert.commandWorked(coll.insert({"_id": 2, "title": "cookies and cakes"}));
assert.commandWorked(coll.insert({"_id": 3, "title": "vegetables"}));

const collUUID = getUUIDFromListCollections(testDB, coll.getName());
const searchQuery = {
    query: "cakes",
    path: "title"
};

const searchCmd = {
    search: coll.getName(),
    collectionUUID: collUUID,
    query: searchQuery,
    $db: dbName
};
const explainContents = {
    content: "test"
};
const cursorId = NumberLong(17);

// Verify that $searchMeta evaluates into SEARCH_META variable returned by mongot.
{
    const history = [{
        expectedCommand: searchCmd,
        response: {
            ok: 1,
            cursor: {
                id: NumberLong(0),
                ns: coll.getFullName(),
                nextBatch: [{_id: 2, score: 0.654}, {_id: 1, score: 0.321}, {_id: 3, score: 0.123}]
            },
            vars: {SEARCH_META: {value: 42}}
        }
    }];
    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId, history: history}));

    let cursorMeta = coll.aggregate([{$searchMeta: searchQuery}], {cursor: {}});
    const expectedMeta = [{value: 42}];
    assert.eq(expectedMeta, cursorMeta.toArray());
}

{
    const history = [{expectedCommand: searchCmd, response: {explain: explainContents, ok: 1}}];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId, history: history}));

    const explain = coll.explain("queryPlanner").aggregate([{$searchMeta: searchQuery}]);
    const searchStage = getAggPlanStage(explain, "$_internalSearchMongotRemote");
    assert.neq(searchStage, null);

    const replaceRootStage = getAggPlanStage(explain, "$replaceRoot");
    assert.eq(replaceRootStage, {"$replaceRoot": {"newRoot": "$$SEARCH_META"}});

    const unionWithStage = getAggPlanStage(explain, "$unionWith");
    const unionWith = unionWithStage["$unionWith"];
    assert(unionWith.hasOwnProperty("pipeline"));
    assert(!unionWith.hasOwnProperty("coll"));

    const limitStage = getAggPlanStage(explain, "$limit");
    assert.eq(limitStage, {$limit: NumberLong(1)});
}

MongoRunner.stopMongod(conn);
mongotmock.stop();
})();
