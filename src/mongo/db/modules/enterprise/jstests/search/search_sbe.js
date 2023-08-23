/**
 * Tests basic functionality of pushing down $search into SBE.
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
const searchQuery1 = {
    query: "star",
    path: "a"
};
const searchCmd1 = {
    search: coll.getName(),
    collectionUUID: collUUID,
    query: searchQuery1,
    $db: "test"
};
const searchQuery2 = {
    query: "re",
    path: "a"
};
const searchCmd2 = {
    search: coll.getName(),
    collectionUUID: collUUID,
    query: searchQuery2,
    $db: "test"
};

const cursorId = NumberLong(123);
const history1 = [
    {
        expectedCommand: searchCmd1,
        response: {
            cursor: {
                id: NumberLong(0),
                ns: coll.getFullName(),
                nextBatch: [
                    {_id: 1, $searchScore: 0.321},
                    {_id: 3, $searchScore: 0.654},
                    {_id: 4, $searchScore: 0.789},
                    // '_id' doesn't exist in db, it should be ignored.
                    {_id: 8, $searchScore: 0.891},
                    {_id: 6, $searchScore: 0.891}
                ]
            },
            ok: 1
        }
    },
];
const expected1 = [
    {"_id": 1, a: "Twinkle twinkle little star"},
    {"_id": 3, a: "You're a star!"},
    {"_id": 4, a: "A star is born."},
    {"_id": 6, a: "Sun, moon and stars"},
];

const history2 = [
    {
        expectedCommand: searchCmd2,
        response: {
            cursor: {
                id: cursorId,
                ns: coll.getFullName(),
                nextBatch: [
                    {_id: 2, $searchScore: 0.321},
                ]
            },
            ok: 1
        }
    },
    {
        expectedCommand: {getMore: cursorId, collection: coll.getName()},
        response: {
            ok: 1,
            cursor: {
                id: NumberLong(0),
                ns: coll.getFullName(),
                nextBatch: [{_id: 3, $searchScore: 0.345}]
            },
        }
    },
];
const expected2 = [
    {"_id": 2, a: "How I wonder what you are"},
    {"_id": 3, a: "You're a star!"},
];

const pipeline1 = [{$search: searchQuery1}];
const pipeline2 = [{$search: searchQuery2}];

// Test SBE pushdown.
{
    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history1}));

    const explain = coll.explain().aggregate(pipeline1);
    // We should have a $search stage.
    assert.eq(1, getAggPlanStages(explain, "SEARCH").length, explain);

    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history1}));
    // $search uses SBE engine.
    assertEngine(pipeline1, "sbe" /* engine */, coll);
}

// Test SBE plan cache.
{
    const getCacheHit = function() {
        return db.serverStatus().metrics.query.planCache.sbe.hits;
    };

    coll.getPlanCache().clear();
    assert.eq(0, coll.getPlanCache().list().length);
    const oldHits = getCacheHit();

    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history1}));
    var res = coll.aggregate(pipeline1);
    assert.eq(expected1, res.toArray());
    // Verify that the cache has 1 entry
    assert.eq(1, coll.getPlanCache().list().length);

    // Re-run the same query.
    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history1}));
    res = coll.aggregate(pipeline1);
    assert.eq(expected1, res.toArray());
    // Verify that the cache has 1 entry, and has been hit for one time.
    assert.eq(1, coll.getPlanCache().list().length);
    assert.eq(getCacheHit(), oldHits + 1);

    // Run a different search query.
    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history2}));
    res = coll.aggregate(pipeline2);
    assert.eq(expected2, res.toArray());
    // Cache not get updated.
    assert.eq(1, coll.getPlanCache().list().length);
    // Hits stats is incremented.
    assert.eq(getCacheHit(), oldHits + 2);
}

// Test how do we handle the case that _id is missing.
{
    const history = [
        {
            expectedCommand: searchCmd1,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: coll.getFullName(),
                    nextBatch: [
                        {haha: 1, $searchScore: 0.321},
                    ]
                },
                ok: 1
            }
        },
    ];
    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history}));
    assert.throwsWithCode(() => coll.aggregate(pipeline1), 4822802);
}

mongotmock.stop();
MongoRunner.stopMongod(conn);
