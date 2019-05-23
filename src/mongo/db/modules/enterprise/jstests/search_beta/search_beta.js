/**
 * Test the basic operation of a `$searchBeta` aggregation stage.
 */
(function() {
    "use strict";
    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/mongotmock.js");
    load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

    // Set up mongotmock and point the mongod to it.
    const mongotmock = new MongotMock();
    mongotmock.start();
    const mongotConn = mongotmock.getConnection();

    const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
    const db = conn.getDB("test");
    const coll = db.search_beta;
    coll.drop();

    assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
    assert.commandWorked(coll.insert({"_id": 2, "title": "cookies and cakes"}));
    assert.commandWorked(coll.insert({"_id": 3, "title": "vegetables"}));
    assert.commandWorked(coll.insert({"_id": 4, "title": "oranges"}));
    assert.commandWorked(coll.insert({"_id": 5, "title": "cakes and oranges"}));
    assert.commandWorked(coll.insert({"_id": 6, "title": "cakes and apples"}));
    assert.commandWorked(coll.insert({"_id": 7, "title": "apples"}));
    assert.commandWorked(coll.insert({"_id": 8, "title": "cakes and kale"}));

    const collUUID = getUUIDFromListCollections(db, coll.getName());
    const searchBetaQuery = {query: "cakes", path: "title"};
    const searchBetaCmd =
        {searchBeta: coll.getName(), collectionUUID: collUUID, query: searchBetaQuery, $db: "test"};

    // Give mongotmock some stuff to return.
    {
        const cursorId = NumberLong(123);
        const history = [
            {
              expectedCommand: searchBetaCmd,
              response: {
                  cursor: {
                      id: cursorId,
                      ns: coll.getFullName(),
                      nextBatch:
                          [{_id: 1, score: 0.321}, {_id: 2, score: 0.654}, {_id: 5, score: 0.789}]
                  },
                  ok: 1
              }
            },
            {
              expectedCommand: {getMore: cursorId, collection: coll.getName()},
              response: {
                  cursor:
                      {id: cursorId, ns: coll.getFullName(), nextBatch: [{_id: 6, score: 0.123}]},
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
                      nextBatch: [{_id: 8, score: 0.345}]
                  },
              }
            },
        ];

        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }

    // Perform a $searchBeta query.
    // Note that the 'batchSize' provided here only applies to the cursor between the driver and
    // mongod, and has no effect on the cursor between mongod and mongotmock.
    let cursor = coll.aggregate([{$searchBeta: searchBetaQuery}], {cursor: {batchSize: 2}});

    const expected = [
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 5, "title": "cakes and oranges"},
        {"_id": 6, "title": "cakes and apples"},
        {"_id": 8, "title": "cakes and kale"}
    ];
    assert.eq(expected, cursor.toArray());

    // Simulate a case where mongot produces as an error after getting a searchBeta command.
    {
        const history = [
            {
              expectedCommand: searchBetaCmd,
              response: {
                  ok: 0,
                  errmsg: "mongot error",
                  code: ErrorCodes.InternalError,
                  codeName: "InternalError"
              }
            },
        ];

        assert.commandWorked(mongotConn.adminCommand(
            {setMockResponses: 1, cursorId: NumberLong(123), history: history}));
        const err = assert.throws(() => coll.aggregate([{$searchBeta: searchBetaQuery}]));
        assert.commandFailedWithCode(err, ErrorCodes.InternalError);
    }

    // Simulate a case where mongot produces an error during a getMore.
    {
        const cursorId = NumberLong(123);
        const history = [
            {
              expectedCommand: searchBetaCmd,
              response: {
                  cursor: {
                      id: cursorId,
                      ns: coll.getFullName(),
                      nextBatch: [
                          {_id: 1, score: 0.321},
                          {_id: 2, score: 0.654},
                          {_id: 3, score: 0.654},
                          {_id: 4, score: 0.789}
                      ]
                  },
                  ok: 1
              }
            },
            {
              expectedCommand: {getMore: cursorId, collection: coll.getName()},
              response: {
                  ok: 0,
                  errmsg: "mongot error",
                  code: ErrorCodes.InternalError,
                  codeName: "InternalError"
              }
            },
        ];

        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));

        // The aggregate() (and searchBeta command) should succeed.
        // Note that 'batchSize' here only tells mongod how many docs to return per batch and has
        // no effect on the batches between mongod and mongotmock.
        const kBatchSize = 4;
        const cursor = coll.aggregate([{$searchBeta: searchBetaQuery}], {batchSize: kBatchSize});

        // Iterate the first batch until it is exhausted.
        for (let i = 0; i < kBatchSize; i++) {
            cursor.next();
        }

        // The next call to next() will result in a 'getMore' being sent to mongod. $searchBeta's
        // internal cursor to mongot will have no results left, and thus, a 'getMore' will be sent
        // to mongot. The error should propagate back to the client.
        const err = assert.throws(() => cursor.next());
        assert.commandFailedWithCode(err, ErrorCodes.InternalError);
    }

    // Run $searchBeta on an empty collection.
    {
        const cursor = db.doesNotExit.aggregate([{$searchBeta: searchBetaQuery}]);
        assert.eq(cursor.toArray(), []);
    }

    // Fail on non-local read concern.
    const err = assert.throws(
        () => coll.aggregate([{$searchBeta: {}}], {readConcern: {level: "majority"}}));
    assert.commandFailedWithCode(err, ErrorCodes.InvalidOptions);

    MongoRunner.stopMongod(conn);
    mongotmock.stop();
})();
