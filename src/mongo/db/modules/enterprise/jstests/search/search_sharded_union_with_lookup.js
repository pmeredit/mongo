/**
 * Sharding test of `$search` aggregation stage within $unionWith and $lookup stages.
 */
(function() {
"use strict";

load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");

const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_search",
    shards: {
        rs0: {nodes: 1},
        rs1: {nodes: 1},
    },
    mongos: 1
});
stWithMock.start();
const st = stWithMock.st;

const dbName = jsTestName();

const mongos = st.s;
const testDB = mongos.getDB(dbName);

const coll = testDB.getCollection("search");
coll.drop();

const unshardedColl = testDB.getCollection("search_unsharded");
unshardedColl.drop();

const collBase = testDB.getCollection("base");
collBase.drop();

const shardedCollBase = testDB.getCollection("baseSharded");
shardedCollBase.drop();

assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
assert.commandWorked(coll.insert({"_id": 2, "title": "oranges"}));
assert.commandWorked(coll.insert({"_id": 11, "title": "cakes and oranges"}));
assert.commandWorked(coll.insert({"_id": 12, "title": "cakes and kale"}));

assert.commandWorked(unshardedColl.insert({"_id": 1, "title": "cakes"}));
assert.commandWorked(unshardedColl.insert({"_id": 2, "title": "cookies and cakes"}));
assert.commandWorked(unshardedColl.insert({"_id": 3, "title": "vegetables"}));
assert.commandWorked(unshardedColl.insert({"_id": 4, "title": "oranges"}));
assert.commandWorked(unshardedColl.insert({"_id": 5, "title": "cakes and oranges"}));
assert.commandWorked(unshardedColl.insert({"_id": 6, "title": "cakes and apples"}));
assert.commandWorked(unshardedColl.insert({"_id": 7, "title": "apples"}));
assert.commandWorked(unshardedColl.insert({"_id": 8, "title": "cakes and kale"}));

assert.commandWorked(collBase.insert({"_id": 100, "localField": "cakes", "weird": false}));
assert.commandWorked(collBase.insert({"_id": 101, "localField": "cakes and kale", "weird": true}));

assert.commandWorked(shardedCollBase.insert({"_id": 100, "localField": "cakes", "weird": false}));
assert.commandWorked(
    shardedCollBase.insert({"_id": 110, "localField": "cakes and kale", "weird": true}));

// Shard search collection
assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
st.shardColl(coll, {_id: 1}, {_id: 10}, {_id: 10 + 1});
// Ensure primary shard so we only set the correct mongot to have history.
st.ensurePrimaryShard(dbName, st.shard1.shardName);
st.shardColl(shardedCollBase, {_id: 1}, {_id: 105}, {_id: 106});

const shard0Conn = st.rs0.getPrimary();
const shard1Conn = st.rs1.getPrimary();
const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
const collUUIDSharded = getUUIDFromListCollections(testDB, coll.getName());
const collUUID = getUUIDFromListCollections(testDB, unshardedColl.getName());

const mongotQuery = {
    query: "cakes",
    path: "title"
};
const searchCmdSharded =
    mongotCommandForQuery(mongotQuery, coll.getName(), testDB.getName(), collUUIDSharded);
const searchCmd =
    mongotCommandForQuery(mongotQuery, unshardedColl.getName(), testDB.getName(), collUUID);

const shard1History = [
    {
        expectedCommand: searchCmd,
        response: mongotResponseForBatch(
            [
                {_id: 2, $searchScore: 0.91},
                {_id: 1, $searchScore: 0.83},
                {_id: 5, $searchScore: 0.73},
                {_id: 8, $searchScore: 0.67},
                {_id: 6, $searchScore: 0.53}
            ],
            NumberLong(0),
            unshardedColl.getFullName(),
            true)
    },
];

const shard0HistoryShardedColl = [
    {
        expectedCommand: searchCmdSharded,
        response: mongotResponseForBatch(
            [
                {_id: 12, $searchScore: 0.91},
                {_id: 11, $searchScore: 0.83},
            ],
            NumberLong(0),
            coll.getFullName(),
            true)
    },
];

const shard1HistoryShardedColl = [
    {
        expectedCommand: searchCmdSharded,
        response:
            mongotResponseForBatch([{_id: 1, $searchScore: 0.43}, {_id: 2, $searchScore: 0.32}],
                                   NumberLong(0),
                                   coll.getFullName(),
                                   true)
    },
];

const makeLookupPipeline = (collection) => [
        {$project: {"_id": 0}},
        {
            $lookup: {
                from: collection.getName(),
                let: { local_title: "$localField" },
                pipeline: [
                    {
                        $search: mongotQuery
                    },
                    {
                        $match: {
                            $expr: {
                                $eq: ["$title", "$$local_title"]
                            }
                        }
                    },
                    {
                        $project: {
                            "_id": 0,
                            "ref_id": "$_id"
                        }
                    }
                ],
                as: "cake_data"
            }
        }
    ];

// Unsharded $lookup with sharded $search.
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(121));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(121));
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(122));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(122));

assert.sameMembers(
    [
        {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
        {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 12}]}
    ],
    collBase.aggregate(makeLookupPipeline(coll)).toArray());

// Sharded $lookup with sharded $search.
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(121));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(121));
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(122));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(122));

assert.sameMembers(
    [
        {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
        {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 12}]}
    ],
    shardedCollBase.aggregate(makeLookupPipeline(coll)).toArray());

// Unsharded $unionWith with sharded $search.
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(121));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(121));
assert.sameMembers(collBase
                       .aggregate([
                           {$unionWith: {coll: coll.getName(), pipeline: [{$search: mongotQuery}]}},
                       ])
                       .toArray(),
                   [
                       {_id: 100, "localField": "cakes", "weird": false},
                       {_id: 101, "localField": "cakes and kale", "weird": true},
                       {_id: 12, "title": "cakes and kale"},
                       {_id: 11, "title": "cakes and oranges"},
                       {_id: 1, "title": "cakes"},
                       {_id: 2, "title": "oranges"}
                   ]);

// Sharded $unionWith with sharded $search.
s0Mongot.setMockResponses(shard0HistoryShardedColl, NumberLong(121));
s1Mongot.setMockResponses(shard1HistoryShardedColl, NumberLong(121));
assert.sameMembers(shardedCollBase
                       .aggregate([
                           {$unionWith: {coll: coll.getName(), pipeline: [{$search: mongotQuery}]}},
                       ])
                       .toArray(),
                   [
                       {_id: 100, "localField": "cakes", "weird": false},
                       {_id: 110, "localField": "cakes and kale", "weird": true},
                       {_id: 12, "title": "cakes and kale"},
                       {_id: 11, "title": "cakes and oranges"},
                       {_id: 1, "title": "cakes"},
                       {_id: 2, "title": "oranges"}
                   ]);

// Test non-sharded $search in $lookup is allowed (base is unsharded).
s1Mongot.setMockResponses(shard1History, NumberLong(121));
s1Mongot.setMockResponses(shard1History, NumberLong(122));
const unshardedLookupResults = collBase.aggregate(makeLookupPipeline(unshardedColl)).toArray();
const lookupSearchExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupSearchExpected, unshardedLookupResults);

// Test that non-sharded $search in $lookup works (base is sharded).
s1Mongot.setMockResponses(shard1History, NumberLong(121));
s1Mongot.setMockResponses(shard1History, NumberLong(122));
const unshardedLookupShardedBaseResults =
    shardedCollBase.aggregate(makeLookupPipeline(unshardedColl)).toArray();
const lookupSearchShardedBaseExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupSearchShardedBaseExpected, unshardedLookupShardedBaseResults);

// Test that non-sharded $search in $unionWith works (base is unsharded).
s1Mongot.setMockResponses(shard1History, NumberLong(128));
assert.sameMembers(
    collBase
        .aggregate([
            {$unionWith: {coll: unshardedColl.getName(), pipeline: [{$search: mongotQuery}]}},
            {$project: {"_id": 0, "weird": 0}}
        ])
        .toArray(),
    [
        {"localField": "cakes"},
        {"localField": "cakes and kale"},
        {"title": "cakes"},
        {"title": "cookies and cakes"},
        {"title": "cakes and oranges"},
        {"title": "cakes and apples"},
        {"title": "cakes and kale"}
    ]);

// Test non-sharded $search in $unionWith is allowed (base is sharded).
s1Mongot.setMockResponses(shard1History, NumberLong(129));

assert.sameMembers(
    shardedCollBase
        .aggregate([
            {$unionWith: {coll: unshardedColl.getName(), pipeline: [{$search: mongotQuery}]}},
            {$project: {"_id": 0, "weird": 0}}
        ])
        .toArray(),
    [
        {"localField": "cakes"},
        {"localField": "cakes and kale"},
        {"title": "cakes"},
        {"title": "cookies and cakes"},
        {"title": "cakes and oranges"},
        {"title": "cakes and apples"},
        {"title": "cakes and kale"}
    ]);

stWithMock.stop();
})();
