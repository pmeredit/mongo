/**
* Test the debug information of the internal search beta document source.
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
    db.setLogLevel(1);
    db.setProfilingLevel(2);
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
                          [{_id: 1, score: 0.789}, {_id: 2, score: 0.654}, {_id: 5, score: 0.321}]
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
    const log = assert.commandWorked(db.adminCommand({getLog: "global"})).log;
    function containsMongotCursor(logLine) {
        return logLine.includes("mongot: {");
    }
    const mongotCursorLog = log.filter(containsMongotCursor);
    assert.eq(mongotCursorLog.length, 3);
    const expectedRegex = RegExp('mongot: \{ cursorid: 123, timeWaitingMillis: [0-9]* \}');
    mongotCursorLog.forEach(function(element) {
        assert(expectedRegex.test(element), element);
    });

    const profilerLog = db.system.profile.find({"mongot": {$exists: true}}).toArray();
    assert.eq(profilerLog.length, 3);
    profilerLog.forEach(function(element) {
        assert.eq(element.mongot.cursorid, 123, element);
    });

    MongoRunner.stopMongod(conn);
    mongotmock.stop();
})();
