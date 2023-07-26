/**
 * Tests basic functionality of pushing $search into SBE.
 */
import {getAggPlanStages} from "jstests/libs/analyze_plan.js";
import {assertEngine} from "jstests/libs/analyze_plan.js";
import {checkSBEEnabled} from "jstests/libs/sbe_util.js";

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

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

const searchStage = {
    $search: {query: "star", path: "a"}
};

const pipeline = [searchStage];
const explain = coll.explain().aggregate(pipeline);

// We should have a $search stage.
assert.eq(1, getAggPlanStages(explain, "SEARCH").length, explain);

// $search uses SBE engine.
assertEngine(pipeline, "sbe" /* engine */, coll);

mongotmock.stop();
MongoRunner.stopMongod(conn);
