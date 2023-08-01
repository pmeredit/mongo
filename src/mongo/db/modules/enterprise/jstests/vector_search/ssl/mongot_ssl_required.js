/**
 * Test that a mongod running with SSL can connect to a mongotmock (which does not use SSL).
 * @tags: [
 *   featureFlagVectorSearchPublicPreview,
 * ]
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");
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
const collName = "vector_search";
db[collName].drop();
assert.commandWorked(db[collName].insert({"_id": 1, "title": "cakes"}));

const collUUID = getUUIDFromListCollections(db, collName);
const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    limit: 5
};

// Give mongotmock some stuff to return.
{
    const cursorId = NumberLong(123);
    const vectorSearchCmd =
        {knn: collName, collectionUUID: collUUID, ...vectorSearchQuery, $db: "test"};
    const history = [
        {
            expectedCommand: vectorSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: "test." + collName,
                    nextBatch: [{_id: 1, $vectorSearchScore: 0.321}]
                },
                ok: 1
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}

// Perform a $vectorSearch query.
let cursor = db[collName].aggregate([{$vectorSearch: vectorSearchQuery}]);

const expected = [
    {"_id": 1, "title": "cakes"},
];
assert.eq(expected, cursor.toArray());

MongoRunner.stopMongod(conn);
mongotmock.stop();
})();
