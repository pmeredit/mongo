/**
 * Test that a mongod running with SSL can connect to a mongotmock (which does not use SSL).
 */
(function() {
    "use strict";
    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/mongotmock.js");
    load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

    // Set up mongotmock and point the mongod to it.
    const mongotmock = new MongotMock();
    mongotmock.start();
    const mongotConn = mongotmock.getConnection();

    const conn = MongoRunner.runMongod({
        sslMode: "requireSSL",
        sslPEMKeyFile: "jstests/libs/password_protected.pem",
        sslPEMKeyPassword: "qwerty",
        setParameter: {mongotHost: mongotConn.host},
    });

    const db = conn.getDB("test");
    const collName = "search_beta";
    db[collName].drop();
    assert.commandWorked(db[collName].insert({"_id": 1, "title": "cakes"}));

    const collUUID = getUUIDFromListCollections(db, collName);
    const searchBetaQuery = {query: "cakes", path: "title"};

    // Give mongotmock some stuff to return.
    {
        const cursorId = NumberLong(123);
        const searchBetaCmd =
            {searchBeta: collName, collectionUUID: collUUID, query: searchBetaQuery, $db: "test"};
        const history = [
            {
              expectedCommand: searchBetaCmd,
              response: {
                  cursor: {
                      id: NumberLong(0),
                      ns: "test." + collName,
                      nextBatch: [{_id: 1, score: 0.321}]
                  },
                  ok: 1
              }
            },
        ];

        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }

    // Perform a $searchBeta query.
    let cursor = db[collName].aggregate([{$searchBeta: searchBetaQuery}]);

    const expected = [
        {"_id": 1, "title": "cakes"},
    ];
    assert.eq(expected, cursor.toArray());

    MongoRunner.stopMongod(conn);
    mongotmock.stop();
})();
