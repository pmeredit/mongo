/**
 * Test that mongotmock gets a kill cursor command when the cursor is killed on mongod.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/mongotmock.js");
    load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

    const mongotMock = new MongotMock();
    mongotMock.start();

    const mongotConn = mongotMock.getConnection();
    const mongotTestDB = mongotConn.getDB("mongotTest");

    const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
    const db = conn.getDB("mongotTest");
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

    const cursorId = NumberLong(123);
    const collUUID = getUUIDFromListCollections(db, coll.getName());
    const searchBetaQuery = {query: "cakes", path: "title"};
    const searchBetaCmd =
        mongotCommandForQuery(searchBetaQuery, coll.getName(), "mongotTest", collUUID);

    const cursorHistory = [
        {
          expectedCommand: searchBetaCmd,
          response: {
              ok: 1,
              cursor: {firstBatch: [{_id: 0}, {_id: 1}], id: cursorId, ns: coll.getFullName()}
          }
        },
        {
          expectedCommand: {getMore: cursorId, collection: coll.getName()},
          response: {cursor: {id: cursorId, ns: coll.getFullName(), nextBatch: [{_id: 6}]}, ok: 1}
        },
        {
          expectedCommand: {killCursors: coll.getName(), cursors: [cursorId]},
          response: {
              cursorsKilled: [cursorId],
              cursorsNotFound: [],
              cursorsAlive: [],
              cursorsUnknown: [],
              ok: 1,
          }
        }
    ];

    assert.commandWorked(
        mongotTestDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: cursorHistory}));

    // Perform a $searchBeta query.
    // Note that the 'batchSize' provided here only applies to the cursor between the driver and
    // mongod, and has no effect on the cursor between mongod and mongotmock.
    let cursor = coll.aggregate([{$searchBeta: searchBetaQuery}], {cursor: {batchSize: 2}});

    // Call killCursors on the mongod cursor.
    cursor.close();

    // Make sure killCursors was called on mongot.
    let resp = assert.commandWorked(mongotTestDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);

    mongotMock.stop();
    MongoRunner.stopMongod(conn);
}());
