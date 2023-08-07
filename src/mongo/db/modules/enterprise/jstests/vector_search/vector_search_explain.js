/**
 * Test the use of "explain" with the "$vectorSearch" aggregation stage.
 * @tags: [
 *  featureFlagVectorSearchPublicPreview,
 * ]
 */
import {getAggPlanStage} from "jstests/libs/analyze_plan.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {
    mongotCommandForVectorSearchQuery,
    MongotMock
} from "src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js";

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
const dbName = jsTestName();
const collName = jsTestName();
const testDB = conn.getDB(dbName);
const coll = testDB.getCollection(collName);
coll.drop();
coll.insert({_id: 0});

const collectionUUID = getUUIDFromListCollections(testDB, collName);

const queryVector = [1.0, 2.0, 3.0];
const path = "x";
const numCandidates = 10;
const limit = 5;
const index = "index";
const filter = {
    x: {$gt: 0}
};

const explainContents = {
    profession: "writer"
};
const cursorId = NumberLong(123);

for (const currentVerbosity of ["queryPlanner", "executionStats", "allPlansExecution"]) {
    const pipeline = [{$vectorSearch: {queryVector, path, numCandidates, limit, index, filter}}];
    // Give mongotmock some stuff to return.
    {
        const history = [{
            expectedCommand: mongotCommandForVectorSearchQuery({
                queryVector,
                path,
                numCandidates,
                index,
                limit,
                collName,
                filter,
                dbName,
                collectionUUID,
                explain: {verbosity: currentVerbosity},
            }),
            response: {explain: explainContents, ok: 1}
        }];
        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }
    const result = coll.explain(currentVerbosity).aggregate(pipeline);
    const searchStage = getAggPlanStage(result, "$vectorSearch");
    assert.neq(searchStage, null, searchStage);
    const stage = searchStage["$vectorSearch"];
    assert(stage.hasOwnProperty("explain"), stage);
    assert.eq(explainContents, stage["explain"]);
}

MongoRunner.stopMongod(conn);
mongotmock.stop();
