/**
 * Tests for the `$vectorSearch` aggregation pipeline stage.
 */
(function() {
"use strict";

load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
load("src/mongo/db/modules/enterprise/jstests/vector_search/lib/mongotmock.js");

const dbName = jsTestName();
const collName = jsTestName();
const collNS = dbName + "." + collName;

// Start mock mongot.
const mongotMock = new MongotMock();
mongotMock.start();
const mockConn = mongotMock.getConnection();

// Start mongod.
const conn = MongoRunner.runMongod({
    setParameter: {mongotHost: mockConn.host},
    verbose: 1,
});
const testDB = conn.getDB(dbName);
assertCreateCollection(testDB, collName);
const collectionUUID = getUUIDFromListCollections(testDB, collName);

const coll = testDB.getCollection(collName);
coll.insert({_id: 0});

const queryVector = [1.0, 2.0, 3.0];
const path = "x";
const candidates = NumberInt(10);
const indexName = "index";

const cursorId = NumberLong(123);
const responseOk = 1;

// $vectorSearch can query mongot and correctly pass along results.
(function testVectorSearchQueriesMongotAndReturnsResults() {
    const filter = {x: {$gt: 0}};
    const pipeline = [{$vectorSearch: {queryVector, path, candidates, indexName, filter}}];

    const mongotResponseBatch = [{_id: 0}];
    const expectedDocs = [{_id: 0}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates, indexName, collName, filter, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

// $vectorSearch only returns # candidates documents.
(function testVectorSearchReturnsNumCandidatesDocuments() {
    const pipeline = [{$vectorSearch: {queryVector, path, candidates: NumberInt(1), indexName}}];

    const mongotResponseBatch = [{_id: 0}, {_id: 1}];
    const expectedDocs = [{_id: 0}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates: 1, indexName, collName, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

// TODO SERVER-78284 Enable this test.
/*
// $vectorSearch populates {$meta: distance}.
(function testVectorSearchPopulatesDistanceMetaField() {
    const pipeline = [{$vectorSearch: {queryVector, path, candidates, indexName}}, {$project: {_id:
1, distance: {$meta: "distance"}}}]; const mongotResponseBatch = [{_id: 0, distance: 1.234}]; const
expectedDocs = [{_id: 0, distance: 1.234}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery({queryVector, path, candidates, indexName,
collName, dbName, collectionUUID}), response: mongotResponseForBatch(mongotResponseBatch,
NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();
*/

coll.insert({_id: 1});
coll.insert({_id: 10});
coll.insert({_id: 11});
coll.insert({_id: 20});

// $vectorSearch handles multiple documents and batches correctly.
(function testVectorSearchMultipleBatches() {
    // TODO SERVER-78284 Include and project distance in the results.
    // const pipeline = [{$vectorSearch: {queryVector, path, candidates,
    // indexName}}, {$project: {_id: 1, distance: {$meta: "distance"}}}];

    // const batchOne = [{_id: 0, distance: 1.234}, {_id: 1, distance: 1.21}];
    // const batchTwo = [{_id: 10, distance: 1.1}, {_id: 11, distance: 0.8}];
    // const batchThree = [{_id: 20, distance: 0.2}];
    // const expectedDocs = [
    //     {_id: 0, distance: 1.234},
    //     {_id: 1, distance: 1.21},
    //     {_id: 10, distance: 1.1},
    //     {_id: 11, distance: 0.8},
    //     {_id: 20, distance: 0.2},
    // ];
    const pipeline = [{$vectorSearch: {queryVector, path, candidates, indexName}}];

    const batchOne = [{_id: 0}, {_id: 1}];
    const batchTwo = [{_id: 10}, {_id: 11}];
    const batchThree = [{_id: 20}];
    const expectedDocs = [
        {_id: 0},
        {_id: 1},
        {_id: 10},
        {_id: 11},
        {_id: 20},
    ];

    const history = [
        {
            expectedCommand: mongotCommandForKnnQuery(
                {queryVector, path, candidates, indexName, collName, dbName, collectionUUID}),
            response: mongotResponseForBatch(batchOne, cursorId, collNS, 1),
        },
        {
            expectedCommand: {getMore: cursorId, collection: collName},
            response: mongotResponseForBatch(batchTwo, cursorId, collNS, 1),
        },
        {
            expectedCommand: {getMore: cursorId, collection: collName},
            response: mongotResponseForBatch(batchThree, NumberLong(0), collNS, 1),
        }
    ];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline, {cursor: {batchSize: 2}}).toArray(),
              expectedDocs);
})();

mongotMock.stop();
MongoRunner.stopMongod(conn);
})();
