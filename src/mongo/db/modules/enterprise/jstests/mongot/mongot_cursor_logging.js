/**
 * Test the debug information of the internal $search document source.
 */

import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";

load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const dbName = jsTestName();
const collName = jsTestName();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
const db = conn.getDB(dbName);
db.setLogLevel(1);
db.setProfilingLevel(2);
const coll = db.getCollection(collName);
coll.drop();

assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
assert.commandWorked(coll.insert({"_id": 2, "title": "cookies and cakes"}));
assert.commandWorked(coll.insert({"_id": 3, "title": "vegetables"}));
assert.commandWorked(coll.insert({"_id": 4, "title": "oranges"}));
assert.commandWorked(coll.insert({"_id": 5, "title": "cakes and oranges"}));
assert.commandWorked(coll.insert({"_id": 6, "title": "cakes and apples"}));
assert.commandWorked(coll.insert({"_id": 7, "title": "apples"}));
assert.commandWorked(coll.insert({"_id": 8, "title": "cakes and kale"}));

const collectionUUID = getUUIDFromListCollections(db, coll.getName());
let cursorId = NumberLong(123);

function runTest(pipeline, expectedCommand) {
    // Give mongotmock some stuff to return.
    {
        const history = [
            {
                expectedCommand,
                response: {
                    cursor: {
                        id: cursorId,
                        ns: coll.getFullName(),
                        nextBatch: [{_id: 1}, {_id: 2}, {_id: 5}]
                    },
                    ok: 1
                }
            },
            {
                expectedCommand: {getMore: cursorId, collection: coll.getName()},
                response:
                    {cursor: {id: cursorId, ns: coll.getFullName(), nextBatch: [{_id: 6}]}, ok: 1}
            },
            {
                expectedCommand: {getMore: cursorId, collection: coll.getName()},
                response: {
                    ok: 1,
                    cursor: {id: NumberLong(0), ns: coll.getFullName(), nextBatch: [{_id: 8}]},
                }
            },
        ];

        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }

    // Perform a $search/$vectorSearch query.
    // Note that the 'batchSize' provided here only applies to the cursor between the driver and
    // mongod, and has no effect on the cursor between mongod and mongotmock.
    let cursor = coll.aggregate(pipeline, {cursor: {batchSize: 2}});

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
        return logLine.includes(`\"cursorid\":${cursorId.valueOf()},`);
    }
    const mongotCursorLog = log.filter(containsMongotCursor);
    assert.eq(mongotCursorLog.length, 3, tojson(log));
    const expectedRegex =
        /"mongot":{"cursorid":[0-9]+,"timeWaitingMillis":[0-9]+,"batchNum":([1-3])}/;
    let expectedBatchNum = 1;
    mongotCursorLog.forEach(function(element) {
        let regexMatch = expectedRegex.exec(element);
        assert.eq(regexMatch.length, 2, element + " - regex - " + expectedRegex);
        // Take advantage of being able to compare strings to ints.
        assert.eq(regexMatch[1],
                  expectedBatchNum,
                  "Expected batch number " + expectedBatchNum + " but found " + regexMatch[1]);
        expectedBatchNum++;
    });

    const profilerLog = db.system.profile.find({"mongot.cursorid": cursorId}).toArray();
    assert.eq(profilerLog.length, 3, tojson(profilerLog));

    // Increment the cursor so that the next test will have a unique value.
    cursorId = NumberLong(cursorId + 1);
}

// TODO SERVER-75690 Enable this test.
if (FeatureFlagUtil.isEnabled(db, "VectorSearchPublicPreview")) {
    const vectorSearchQuery =
        {queryVector: [1.0, 2.0, 3.0], path: "x", numCandidates: 10, limit: 5};
    runTest([{$vectorSearch: vectorSearchQuery}],
            mongotCommandForKnnQuery({...vectorSearchQuery, collName, dbName, collectionUUID}));
}

const searchQuery = {
    query: "cakes",
    path: "title"
};
runTest([{$search: searchQuery}],
        {search: collName, collectionUUID, query: searchQuery, $db: dbName});

MongoRunner.stopMongod(conn);
mongotmock.stop();