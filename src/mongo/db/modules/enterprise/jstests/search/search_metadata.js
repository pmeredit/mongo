/**
 * Tests that "searchScore", "searchHighlights", and "searchScoreDetails" metadata is properly
 * plumbed through the $search agg stage.
 */
(function() {
"use strict";

load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

const dbName = "test";

// Start mock mongot.
const mongotMock = new MongotMock();
mongotMock.start();
const mockConn = mongotMock.getConnection();

// Start mongod.
const conn = MongoRunner.runMongod({
    setParameter: {mongotHost: mockConn.host},
});
const testDB = conn.getDB(dbName);
const coll = testDB.search_metadata;

assert.commandWorked(coll.insert({_id: 0, foo: 1, bar: "projected out"}));
assert.commandWorked(coll.insert({_id: 1, foo: 2, bar: "projected out"}));
assert.commandWorked(coll.insert({_id: 10, foo: 3, bar: "projected out"}));
assert.commandWorked(coll.insert({_id: 11, foo: 4, bar: "projected out"}));
assert.commandWorked(coll.insert({_id: 20, foo: 5, bar: "projected out"}));
const collUUID = getUUIDFromListCollections(testDB, coll.getName());

// $search populates {$meta: "searchScore"}, {$meta: "searchHighlights"}, and {$meta:
// "searchScoreDetails"}.
{
    const mongotQuery = {scoreDetails: true};
    const cursorId = NumberLong(123);
    const pipeline = [
        {$search: mongotQuery},
        {
            $project: {
                _id: 1,
                foo: 1,
                score: {$meta: "searchScore"},
                highlights: {$meta: "searchHighlights"},
                scoreInfo: {$meta: "searchScoreDetails"}
            }
        }
    ];
    const highlights = ["a", "b", "c"];
    const searchScoreDetails = {scoreDetails: "the score is great"};
    const mongotResponseBatch = [{
        _id: 0,
        $searchScore: 1.234,
        $searchHighlights: highlights,
        $searchScoreDetails: searchScoreDetails
    }];
    const responseOk = 1;
    const expectedDocs =
        [{_id: 0, foo: 1, score: 1.234, highlights: highlights, scoreInfo: searchScoreDetails}];

    const history = [{
        expectedCommand: mongotCommandForQuery(mongotQuery, coll.getName(), dbName, collUUID),
        response: mongotResponseForBatch(
            mongotResponseBatch, NumberLong(0), coll.getFullName(), responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(coll.aggregate(pipeline).toArray(), expectedDocs);
}

// Check that metadata is passed along correctly when there are multiple batches, both between
// the shell and mongod, and between mongod and mongot.
{
    const mongotQuery = {scoreDetails: true};
    const cursorId = NumberLong(123);
    const pipeline = [
        {$search: mongotQuery},
        {
            $project: {
                _id: 1,
                foo: 1,
                score: {$meta: "searchScore"},
                highlights: {$meta: "searchHighlights"},
                scoreInfo: {$meta: "searchScoreDetails"}
            }
        }
    ];

    const searchScoreDetailsFirstBatch = {scoreDetails: "the score is very good"};
    const batchOne = [
        {
            _id: 0,
            $searchScore: 1.234,
            $searchHighlights: ["a"],
            $searchScoreDetails: searchScoreDetailsFirstBatch
        },
        {
            _id: 1,
            $searchScore: 1.21,
            $searchHighlights: ["a", "b", "c"],
            $searchScoreDetails: searchScoreDetailsFirstBatch
        }
    ];

    const searchScoreDetailsSecondBatch = {scoreDetails: "the score is fantastic"};
    const batchTwo = [
        // $searchHighlights should be able to be any type.
        {
            _id: 10,
            $searchScore: 0.0,
            $searchHighlights: null,
            $searchScoreDetails: searchScoreDetailsSecondBatch
        },
        {
            _id: 11,
            $searchScore: 0.1,
            $searchHighlights: [],
            $searchScoreDetails: searchScoreDetailsSecondBatch
        },
    ];

    // searchHighlights should be able to be an array of objects.
    const searchScoreDetailsThirdBatch = {scoreDetails: "the score could not be better"};
    const highlightsWithSubobjs = [{a: 1, b: 1}, {a: 1, b: 2}];
    const batchThree = [{
        _id: 20,
        $searchScore: 0.2,
        $searchHighlights: highlightsWithSubobjs,
        $searchScoreDetails: searchScoreDetailsThirdBatch
    }];

    const expectedDocs = [
        {_id: 0, foo: 1, score: 1.234, highlights: ["a"], scoreInfo: searchScoreDetailsFirstBatch},
        {
            _id: 1,
            foo: 2,
            score: 1.21,
            highlights: ["a", "b", "c"],
            scoreInfo: searchScoreDetailsFirstBatch
        },
        {_id: 10, foo: 3, score: 0.0, highlights: null, scoreInfo: searchScoreDetailsSecondBatch},
        {_id: 11, foo: 4, score: 0.1, highlights: [], scoreInfo: searchScoreDetailsSecondBatch},
        {
            _id: 20,
            foo: 5,
            score: 0.2,
            highlights: highlightsWithSubobjs,
            scoreInfo: searchScoreDetailsThirdBatch
        },
    ];

    const history = [
        {
            expectedCommand: mongotCommandForQuery(mongotQuery, coll.getName(), dbName, collUUID),
            response: mongotResponseForBatch(batchOne, cursorId, coll.getFullName(), 1),
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: mongotResponseForBatch(batchTwo, cursorId, coll.getFullName(), 1),
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: mongotResponseForBatch(batchThree, NumberLong(0), coll.getFullName(), 1),
        }
    ];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(coll.aggregate(pipeline, {cursor: {batchSize: 2}}).toArray(), expectedDocs);
}

mongotMock.stop();
MongoRunner.stopMongod(conn);
})();
