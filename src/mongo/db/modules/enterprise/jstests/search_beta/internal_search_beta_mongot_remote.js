/**
 * Tests for the `$_internalSearchBetaMongotRemote` aggregation pipeline stage.
 */
(function() {
    "use strict";

    load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
    load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/mongotmock.js");

    const dbName = "test";
    const collName = "internal_search_beta_mongot_remote";
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
    const collUUID = getUUIDFromListCollections(testDB, collName);

    // $_internalSearchBetaMongotRemote can query mongot and correctly pass along results.
    {
        const mongotQuery = {};
        const cursorId = NumberLong(123);
        const pipeline = [{$_internalSearchBetaMongotRemote: mongotQuery}];
        const mongotResponseBatch = [{_id: 0}];
        const responseOk = 1;
        const expectedDocs = [{_id: 0}];

        const history = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID),
            response:
                mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        mongotMock.setMockResponses(history, cursorId);
        assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
    }

    // $_internalSearchBetaMongotRemote populates {$meta: searchScore}.
    {
        const mongotQuery = {};
        const cursorId = NumberLong(123);
        const pipeline = [
            {$_internalSearchBetaMongotRemote: mongotQuery},
            {$project: {_id: 1, score: {$meta: "searchScore"}}}
        ];
        const mongotResponseBatch = [{_id: 0, $searchScore: 1.234}];
        const responseOk = 1;
        const expectedDocs = [{_id: 0, score: 1.234}];

        const history = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID),
            response:
                mongotResponseForBatch(mongotResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        mongotMock.setMockResponses(history, cursorId);
        assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
    }

    // $_internalSearchBetaMongotRemote handles multiple documents and batches correctly.
    {
        const mongotQuery = {};
        const cursorId = NumberLong(123);
        const pipeline = [
            {$_internalSearchBetaMongotRemote: mongotQuery},
            {$project: {_id: 1, score: {$meta: "searchScore"}}}
        ];

        const batchOne = [{_id: 0, $searchScore: 1.234}, {_id: 1, $searchScore: 1.21}];
        const batchTwo = [{_id: 10, $searchScore: 1.1}, {_id: 11, $searchScore: 0.8}];
        const batchThree = [{_id: 20, $searchScore: 0.2}];
        const expectedDocs = [
            {_id: 0, score: 1.234},
            {_id: 1, score: 1.21},
            {_id: 10, score: 1.1},
            {_id: 11, score: 0.8},
            {_id: 20, score: 0.2},
        ];

        const history = [
            {
              expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID),
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
    }

    mongotMock.stop();
    MongoRunner.stopMongod(conn);
})();
