/**
 * Tests that pipelines containing `$search` fail if the resolved collation is non-simple.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

const simpleCollation = {
    locale: "simple"
};
const nonSimpleCollation = {
    locale: "en"
};
const searchCollationNonSimpleErrCode = 7130700;
const searchMetaCollationNonSimpleErrCode = 7130701;
const simpleCollection = "simpleCollection";
const nonSimpleCollection = "nonSimpleCollection";

function populateColl(coll) {
    assert.commandWorked(coll.insert({"_id": 1, "shape": "circle"}));
    assert.commandWorked(coll.insert({"_id": 2, "shape": "square"}));
}

const searchQuery = {
    query: "circle",
    path: "shape"
};

function runCmd(collection, stage, collation = null) {
    let cmd = {aggregate: collection, pipeline: [stage], cursor: {}};
    if (collation != null) {
        cmd.collation = collation;
    }
    return db.runCommand(cmd);
}

const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
const db = conn.getDB("test");

// Create a collection where the default collation is explicitly simple.
assert.commandWorked(db.createCollection(simpleCollection, {collation: simpleCollation}));
populateColl(db.getCollection(simpleCollection));
const simpleCollUUID = getUUIDFromListCollections(db, simpleCollection);
const simpleCollectionSearchCmd = {
    search: simpleCollection,
    collectionUUID: simpleCollUUID,
    query: searchQuery,
    $db: "test"
};

// If the aggregation command does not specify a collation, $search should succeed.
{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: simpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(simpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(simpleCollection, {$search: searchQuery}));

// If the collation in the aggregation command is simple, $search should succeed.
{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: simpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(simpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(simpleCollection, {$search: searchQuery}, simpleCollation));

// $searchMeta should succeed if the aggregation command does not specify a collation.
{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: simpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(simpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1,
                vars: {SEARCH_META: {value: 42}}
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(simpleCollection, {$searchMeta: searchQuery}));

// $seachMeta should succeed if the aggregation command has a simple collation.
{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: simpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(simpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1,
                vars: {SEARCH_META: {value: 42}}
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(simpleCollection, {$searchMeta: searchQuery}, simpleCollation));

// If the collation in the aggregation command is non-simple, $search and $searchMeta should fail.
assert.commandFailedWithCode(runCmd(simpleCollection, {$search: searchQuery}, nonSimpleCollation),
                             searchCollationNonSimpleErrCode);
assert.commandFailedWithCode(
    runCmd(simpleCollection, {$searchMeta: searchQuery}, nonSimpleCollation),
    searchMetaCollationNonSimpleErrCode);

// Create a collection where the default collation is non-simple.
assert.commandWorked(db.createCollection(nonSimpleCollection, {collation: nonSimpleCollation}));
populateColl(db.getCollection(nonSimpleCollection));
const nonSimpleCollUUID = getUUIDFromListCollections(db, nonSimpleCollection);
const nonSimpleCollectionSearchCmd = {
    search: nonSimpleCollection,
    collectionUUID: nonSimpleCollUUID,
    query: searchQuery,
    $db: "test"
};

// The command's collation (if specified) overrides the collection default, so the query should
// succeed if the command's collation is simple and fail otherwise.
{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: nonSimpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(nonSimpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1,
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(nonSimpleCollection, {$search: searchQuery}, simpleCollation));

{
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: nonSimpleCollectionSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: db.getCollection(nonSimpleCollection).getFullName(),
                    nextBatch: [{_id: 1, score: 0.987}]
                },
                ok: 1,
                vars: {SEARCH_META: {value: 42}}
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}
assert.commandWorked(runCmd(nonSimpleCollection, {$searchMeta: searchQuery}, simpleCollation));

assert.commandFailedWithCode(
    runCmd(nonSimpleCollection, {$search: searchQuery}, nonSimpleCollation),
    searchCollationNonSimpleErrCode);
assert.commandFailedWithCode(
    runCmd(nonSimpleCollection, {$searchMeta: searchQuery}, nonSimpleCollation),
    searchMetaCollationNonSimpleErrCode);

// The commands should also fail when they don't specify a collation since they inherit the
// collection default collation.
assert.commandFailedWithCode(runCmd(nonSimpleCollection, {$search: searchQuery}),
                             searchCollationNonSimpleErrCode);
assert.commandFailedWithCode(runCmd(nonSimpleCollection, {$searchMeta: searchQuery}),
                             searchMetaCollationNonSimpleErrCode);

MongoRunner.stopMongod(conn);
mongotmock.stop();
})();
