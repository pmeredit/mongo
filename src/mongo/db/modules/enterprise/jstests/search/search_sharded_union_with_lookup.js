/**
 * Sharding test of `$search` aggregation stage within $unionWith and $lookup stages.
 */
(function() {
"use strict";

load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");
load("jstests/libs/feature_flag_util.js");

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
const useShardedFacets = FeatureFlagUtil.isEnabled(testDB, "SearchShardedFacets");

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

let searchCmdSharded = useShardedFacets
    ? mongotCommandForQuery(
          mongotQuery, coll.getName(), testDB.getName(), collUUIDSharded, NumberInt(42))
    : mongotCommandForQuery(mongotQuery, coll.getName(), testDB.getName(), collUUIDSharded);

const searchCmd =
    mongotCommandForQuery(mongotQuery, unshardedColl.getName(), testDB.getName(), collUUID);

const shard0HistoryShardedColl = [
    {
        expectedCommand: searchCmdSharded,
        response: mongotResponseMetadataAgnostic(
            [
                {_id: 12, $searchScore: 0.91},
                {_id: 11, $searchScore: 0.83},
            ],
            NumberLong(0),
            coll.getFullName(),
            true,
            useShardedFacets)
    },
];

const shard1HistoryShardedColl = [
    {
        expectedCommand: searchCmdSharded,
        response: mongotResponseMetadataAgnostic(
            [{_id: 1, $searchScore: 0.43}, {_id: 2, $searchScore: 0.32}],
            NumberLong(0),
            coll.getFullName(),
            true,
            useShardedFacets)
    },
];

const shard1HistoryUnsharded = [
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
                    }, {
                        $project: collection === unshardedColl ? ({
                            "_id": 0, "ref_id": "$_id",
                        }) : ({
                            "_id": 0,
                            "ref_id": "$_id",
                            "searchMeta": useShardedFacets ? "$$SEARCH_META" : {"val": {$literal: 1}},
                        })
                    }],
                as: "cake_data"
            }
        }
    ];
// Unsharded $lookup with sharded $search.
if (useShardedFacets === true) {
    setGenericMergePipeline(coll.getName(), mongotQuery, dbName, stWithMock);
    const historyObj = {
        expectedCommand: {planShardedSearch: coll.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    // Gets called a number of times. The same cursor isn't used again for planShardedSearch, so
    // use two different states.
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);
    s1Mongot.setMockResponses(mergingPipelineHistory, 1424);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponses(mergingPipelineHistory, 1425);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(122), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(122), useShardedFacets);
} else {
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(122), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(122), useShardedFacets);
}

assert.sameMembers(
    [
        {
            "localField": "cakes",
            "weird": false,
            "cake_data": [{"ref_id": 1, "searchMeta": {"val": 1}}]
        },
        {
            "localField": "cakes and kale",
            "weird": true,
            "cake_data": [{"ref_id": 12, "searchMeta": {"val": 1}}]
        }
    ],
    collBase.aggregate(makeLookupPipeline(coll)).toArray());

// Sharded $lookup with sharded $search.
if (useShardedFacets === true) {
    setGenericMergePipeline(coll.getName(), mongotQuery, dbName, stWithMock, stWithMock);
    const historyObj = {
        expectedCommand: {planShardedSearch: coll.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);
    s0Mongot.setMockResponses(mergingPipelineHistory, 1423);
    s1Mongot.setMockResponses(mergingPipelineHistory, 1424);
    s0Mongot.setMockResponses(mergingPipelineHistory, 1424);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(122), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(122), useShardedFacets);
} else {
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
    s0Mongot.setMockResponsesMetadataAgnostic(
        shard0HistoryShardedColl, NumberLong(122), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryShardedColl, NumberLong(122), useShardedFacets);
}

assert.sameMembers(
    [
        {
            "localField": "cakes",
            "weird": false,
            "cake_data": [{"ref_id": 1, "searchMeta": {"val": 1}}]
        },
        {
            "localField": "cakes and kale",
            "weird": true,
            "cake_data": [{"ref_id": 12, "searchMeta": {"val": 1}}]
        }
    ],
    shardedCollBase.aggregate(makeLookupPipeline(coll)).toArray());

// Unsharded $unionWith with sharded $search.
s0Mongot.setMockResponsesMetadataAgnostic(
    shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
s1Mongot.setMockResponsesMetadataAgnostic(
    shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
if (useShardedFacets === true) {
    setGenericMergePipeline(coll.getName(), mongotQuery, dbName, stWithMock);
}
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
if (useShardedFacets === true) {
    setGenericMergePipeline(coll.getName(), mongotQuery, dbName, stWithMock);
}
s0Mongot.setMockResponsesMetadataAgnostic(
    shard0HistoryShardedColl, NumberLong(121), useShardedFacets);
s1Mongot.setMockResponsesMetadataAgnostic(
    shard1HistoryShardedColl, NumberLong(121), useShardedFacets);
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
if (useShardedFacets === true) {
    const historyObj = {
        expectedCommand:
            {planShardedSearch: unshardedColl.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);
    s1Mongot.setMockResponses(mergingPipelineHistory, 1424);
    s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(121));
    s1Mongot.setMockResponses(mergingPipelineHistory, 1425);
    s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(122));
} else {
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryUnsharded, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryUnsharded, NumberLong(122), useShardedFacets);
}
const unshardedLookupResults = collBase.aggregate(makeLookupPipeline(unshardedColl)).toArray();
const lookupSearchExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupSearchExpected, unshardedLookupResults);

// Test that non-sharded $search in $lookup works (base is sharded).
if (useShardedFacets === true) {
    setGenericMergePipeline(unshardedColl.getName(), mongotQuery, dbName, stWithMock);
    const historyObj = {
        expectedCommand:
            {planShardedSearch: unshardedColl.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);
    s1Mongot.setMockResponses(mergingPipelineHistory, 1424);
    s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(121));
    s1Mongot.setMockResponses(mergingPipelineHistory, 1425);
    s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(122));
} else {
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryUnsharded, NumberLong(121), useShardedFacets);
    s1Mongot.setMockResponsesMetadataAgnostic(
        shard1HistoryUnsharded, NumberLong(122), useShardedFacets);
}
const unshardedLookupShardedBaseResults =
    shardedCollBase.aggregate(makeLookupPipeline(unshardedColl)).toArray();
const lookupSearchShardedBaseExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupSearchShardedBaseExpected, unshardedLookupShardedBaseResults);

// Test that non-sharded $search in $unionWith works (base is unsharded).
if (useShardedFacets == true) {
    setGenericMergePipeline(coll.getName(), mongotQuery, dbName, stWithMock);
    const historyObj = {
        expectedCommand:
            {planShardedSearch: unshardedColl.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);
}
s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(128));
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

if (useShardedFacets == true) {
    setGenericMergePipeline(unshardedColl.getName(), mongotQuery, dbName, stWithMock);
}
s1Mongot.setMockResponses(shard1HistoryUnsharded, NumberLong(129));

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
