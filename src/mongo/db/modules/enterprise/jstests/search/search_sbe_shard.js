/**
 * Tests basic functionality of pushing $search into SBE.
 */
import {getAggPlanStages} from "jstests/libs/analyze_plan.js";
import {assertEngine} from "jstests/libs/analyze_plan.js";
import {checkSBEEnabled} from "jstests/libs/sbe_util.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {MongotMock} from "src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js";
import {
    mockPlanShardedSearchResponse,
    mongotCommandForQuery,
    mongotMultiCursorResponseForBatch,
} from "src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js";
import {
    ShardingTestWithMongotMock
} from "src/mongo/db/modules/enterprise/jstests/mongot/lib/shardingtest_with_mongotmock.js";

const dbName = "test";
const collName = "internal_search_mongot_remote";

const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_search",
    shards: {
        rs0: {nodes: 2},
        rs1: {nodes: 2},
    },
    mongos: 1,
    other: {
        rsOptions: {setParameter: {enableTestCommands: 1, featureFlagSearchInSbe: true}},
    }
});
stWithMock.start();
const st = stWithMock.st;

const mongos = st.s;
const testDB = mongos.getDB(dbName);
const testColl = testDB.getCollection(collName);
const collNS = testColl.getFullName();

if (!checkSBEEnabled(testDB)) {
    jsTestLog("Skipping test because SBE is disabled");
    stWithMock.stop();
    quit();
}

assert.commandWorked(testColl.insert({_id: 1, x: "ow"}));
assert.commandWorked(testColl.insert({_id: 2, x: "now", y: "lorem"}));
assert.commandWorked(testColl.insert({_id: 3, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 4, x: "cow", y: "lorem ipsum"}));
assert.commandWorked(testColl.insert({_id: 11, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 12, x: "cow", y: "lorem ipsum"}));
assert.commandWorked(testColl.insert({_id: 13, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 14, x: "cow", y: "lorem ipsum"}));

// Shard the test collection, split it at {_id: 10}, and move the higher chunk to shard1.
assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
assert.commandWorked(st.s.adminCommand({movePrimary: dbName, to: st.shard0.shardName}));
st.shardColl(testColl, {_id: 1}, {_id: 10}, {_id: 10 + 1});

const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);
const collUUID1 = getUUIDFromListCollections(st.rs1.getPrimary().getDB(dbName), collName);

const mongotQuery = {};
const protocolVersion = NumberInt(1);
const expectedMongotCommand =
    mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0, protocolVersion);

const cursorId = NumberLong(123);
const secondCursorId = NumberLong(cursorId + 1001);
const pipeline = [
    {$search: mongotQuery},
];

function setup(shard0Conn, shard1Conn) {
    const responseOk = 1;

    const mongot0ResponseBatch = [
        {_id: 3, $searchScore: 100},
        {_id: 2, $searchScore: 10},
        {_id: 4, $searchScore: 1},
        {_id: 1, $searchScore: 0.99},
    ];
    const history0 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotMultiCursorResponseForBatch(
            mongot0ResponseBatch, NumberLong(0), [{val: 1}], NumberLong(0), collNS, responseOk),
    }];

    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId, secondCursorId);

    const mongot1ResponseBatch = [
        {_id: 11, $searchScore: 111},
        {_id: 13, $searchScore: 30},
        {_id: 12, $searchScore: 29},
        {_id: 14, $searchScore: 28},
    ];
    const history1 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotMultiCursorResponseForBatch(
            mongot1ResponseBatch, NumberLong(0), [{val: 1}], NumberLong(0), collNS, responseOk),
    }];
    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponses(history1, cursorId, secondCursorId);

    const expectedDocs = [
        {_id: 11, x: "brown", y: "ipsum"},
        {_id: 3, x: "brown", y: "ipsum"},
        {_id: 13, x: "brown", y: "ipsum"},
        {_id: 12, x: "cow", y: "lorem ipsum"},
        {_id: 14, x: "cow", y: "lorem ipsum"},
        {_id: 2, x: "now", y: "lorem"},
        {_id: 4, x: "cow", y: "lorem ipsum"},
        {_id: 1, x: "ow"},
    ];

    mockPlanShardedSearchResponse(
        testColl.getName(), mongotQuery, dbName, undefined /*sortSpec*/, stWithMock);

    return expectedDocs;
}

// Test SBE shard explain.
{
    setup(st.rs0.getPrimary(), st.rs1.getPrimary());

    const explain = testColl.explain().aggregate(pipeline);
    // We should have a $search stage each shard.
    assert.eq(2, getAggPlanStages(explain, "SEARCH").length, explain);
    // We should have shardFilter in SBE plan.
    const plan = explain.shards[st.rs0.name].queryPlanner.winningPlan;
    assert.eq(plan.hasOwnProperty("slotBasedPlan"), true);
    assert.includes(plan.slotBasedPlan.stages, 'shardFilter');
}

stWithMock.stop();
