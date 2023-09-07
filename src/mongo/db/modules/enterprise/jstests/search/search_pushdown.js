/**
 * Tests basic functionality of pushing $search into SBE.
 */
import {getAggPlanStages} from "jstests/libs/analyze_plan.js";
import {assertEngine} from "jstests/libs/analyze_plan.js";
import {checkSBEEnabled} from "jstests/libs/sbe_util.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {MongotMock} from "src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js";

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod(
    {setParameter: {mongotHost: mongotConn.host, featureFlagSearchInSbe: true}});
const db = conn.getDB("test");
const coll = db.search;
coll.drop();

if (!checkSBEEnabled(db)) {
    jsTestLog("Skipping test because SBE is disabled");
    mongotmock.stop();
    MongoRunner.stopMongod(conn);
    quit();
}

assert.commandWorked(coll.insert({"_id": 1, a: "Twinkle twinkle little star"}));
assert.commandWorked(coll.insert({"_id": 2, a: "How I wonder what you are"}));
assert.commandWorked(coll.insert({"_id": 3, a: "You're a star!"}));
assert.commandWorked(coll.insert({"_id": 4, a: "A star is born."}));
assert.commandWorked(coll.insert({"_id": 5, a: "Up above the world so high"}));
assert.commandWorked(coll.insert({"_id": 6, a: "Sun, moon and stars"}));

const collUUID = getUUIDFromListCollections(db, coll.getName());
const searchQuery = {
    query: "star",
    path: "a"
};
const searchCmd = {
    search: coll.getName(),
    collectionUUID: collUUID,
    query: searchQuery,
    $db: "test"
};
const cursorId = NumberLong(123);
const history = [
    {
        expectedCommand: searchCmd,
        response: {
            cursor: {
                id: NumberLong(0),
                ns: coll.getFullName(),
                nextBatch: [
                    {_id: 1, $searchScore: 0.321},
                    {_id: 3, $searchScore: 0.654},
                    {_id: 4, $searchScore: 0.789},
                    {_id: 6, $searchScore: 0.891}
                ]
            },
            ok: 1
        }
    },
];

const searchStage = {
    $search: searchQuery
};
const pipeline = [searchStage];

assert.commandWorked(
    mongotConn.adminCommand({setMockResponses: 1, cursorId: NumberLong(123), history: history}));
const explain = coll.explain().aggregate(pipeline);
// We should have a $search stage.
assert.eq(1, getAggPlanStages(explain, "SEARCH").length, explain);

assert.commandWorked(
    mongotConn.adminCommand({setMockResponses: 1, cursorId: NumberLong(123), history: history}));
// $search uses SBE engine.
assertEngine(pipeline, "sbe" /* engine */, coll);

mongotmock.stop();
MongoRunner.stopMongod(conn);
