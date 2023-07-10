/**
 * Tests for the `$vectorSearch` aggregation pipeline stage.
 * @tags: [
 *  featureFlagVectorSearchPublicPreview,
 * ]
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
const candidates = 10;
const limit = 5;
const index = "index";

const cursorId = NumberLong(123);
const responseOk = 1;

// $vectorSearch can query mongot and correctly pass along results.
(function testVectorSearchQueriesMongotAndReturnsResults() {
    const filter = {x: {$gt: 0}};
    const pipeline = [{$vectorSearch: {queryVector, path, candidates, limit, index, filter}}];

    const mongotResponseBatch = [{_id: 0}];
    const expectedDocs = [{_id: 0}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates, index, collName, filter, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

// $vectorSearch only returns # limit documents.
(function testVectorSearchRespectsLimit() {
    const pipeline = [{$vectorSearch: {queryVector, path, candidates, limit: 1, index}}];

    const mongotResponseBatch = [{_id: 0}, {_id: 1}];
    const expectedDocs = [{_id: 0}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates, index, collName, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

// $vectorSearch populates {$meta: distance}.
(function testVectorSearchPopulatesDistanceMetaField() {
    const pipeline = [
        {$vectorSearch: {queryVector, path, candidates, limit, index}},
        {$project: {_id: 1, dist: {$meta: "vectorSearchDistance"}}}
    ];
    const mongotResponseBatch = [{_id: 0, $vectorSearchDistance: 1.234}];
    const expectedDocs = [{_id: 0, dist: 1.234}];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates, index, collName, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

coll.insert({_id: 1});
coll.insert({_id: 10});
coll.insert({_id: 11});
coll.insert({_id: 20});

// $vectorSearch handles multiple documents and batches correctly.
(function testVectorSearchMultipleBatches() {
    const pipeline = [
        {$vectorSearch: {queryVector, path, candidates, limit, index}},
        {$project: {_id: 1, dist: {$meta: "vectorSearchDistance"}}}
    ];

    const batchOne =
        [{_id: 0, $vectorSearchDistance: 1.234}, {_id: 1, $vectorSearchDistance: 1.21}];
    const batchTwo = [{_id: 10, $vectorSearchDistance: 1.1}, {_id: 11, $vectorSearchDistance: 0.8}];
    const batchThree = [{_id: 20, $vectorSearchDistance: 0.2}];

    const expectedDocs = [
        {_id: 0, dist: 1.234},
        {_id: 1, dist: 1.21},
        {_id: 10, dist: 1.1},
        {_id: 11, dist: 0.8},
        {_id: 20, dist: 0.2},
    ];

    const history = [
        {
            expectedCommand: mongotCommandForKnnQuery(
                {queryVector, path, candidates, index, collName, dbName, collectionUUID}),
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

coll.insert({_id: 2, x: "now", y: "lorem"});
coll.insert({_id: 3, x: "brown", y: "ipsum"});
coll.insert({_id: 4, x: "cow", y: "lorem ipsum"});

// $vectorSearch uses the idLookup stage as expected.
(function testVectorSearchPerformsIdLookup() {
    const pipeline = [
        {$vectorSearch: {queryVector, path, candidates, limit, index}},
    ];

    const mongotResponseBatch = [{_id: 2}, {_id: 3}, {_id: 4}];

    const expectedDocs = [
        {_id: 2, x: "now", y: "lorem"},
        {_id: 3, x: "brown", y: "ipsum"},
        {_id: 4, x: "cow", y: "lorem ipsum"}
    ];

    const history = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, candidates, index, collName, dbName, collectionUUID}),
        response: mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
    }];

    mongotMock.setMockResponses(history, cursorId);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

mongotMock.stop();
MongoRunner.stopMongod(conn);
})();
