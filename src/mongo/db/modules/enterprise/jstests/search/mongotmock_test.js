/**
 * Test mongotmock.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");

const mongotMock = new MongotMock();
mongotMock.start();

const conn = mongotMock.getConnection();
const testDB = conn.getDB("test");

{
    // Ensure the mock returns the correct responses and validates the 'expected' commands.
    // These examples do not obey the find/getMore protocol.
    const cursorId = NumberLong(123);
    const searchCmd = {search: "a UUID"};
    const history = [
        {expectedCommand: searchCmd, response: {ok: 1, foo: 1}},
        {expectedCommand: {getMore: cursorId, collection: "abc"}, response: {ok: 1, foo: 2}},
        {expectedCommand: {getMore: cursorId, collection: "abc"}, response: {ok: 1, foo: 3}},
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: history}));

    // Now run a search command.
    let resp = assert.commandWorked(testDB.runCommand(searchCmd));
    assert.eq(resp, {ok: 1, foo: 1});

    // Run a getMore which succeeds.
    resp = assert.commandWorked(testDB.runCommand({getMore: NumberLong(123), collection: "abc"}));
    assert.eq(resp, {ok: 1, foo: 2});

    // Check the remaining history on the mock. There should be one more queued command.
    resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 1);

    // Run another getMore which should succeed.
    resp = assert.commandWorked(testDB.runCommand({getMore: NumberLong(123), collection: "abc"}));
    assert.eq(resp, {ok: 1, foo: 3});

    // Run another getMore. This should fail because there are no more queued responses for the
    // cursor id.
    assert.commandFailedWithCode(testDB.runCommand({getMore: NumberLong(123), collection: "abc"}),
                                 31087);

    // Check the remaining history on the mock. There should be 0 remaining queued commands.
    resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);
}

{
    // Test some edge and error cases.
    const cursorId = NumberLong(123);
    const searchCmd = {search: "a UUID"};
    const history = [
        {expectedCommand: searchCmd, response: {ok: 1}},
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: history}));

    // We should be able to set the mock responses again to the same thing without issue.
    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: history}));

    // Run setMockResponses on cursor id of 0.
    assert.commandFailedWithCode(
        testDB.runCommand({setMockResponses: 1, cursorId: NumberLong(0), history: history}),
        ErrorCodes.InvalidOptions);

    // Run getMore on cursor id before it's ready.
    assert.commandFailedWithCode(testDB.runCommand({getMore: NumberLong(123), collection: "abc"}),
                                 31088);

    // Run getMore on invalid cursor id.
    assert.commandFailedWithCode(testDB.runCommand({getMore: NumberLong(777), collection: "abc"}),
                                 31089);

    // Run a search which doesn't match its 'expectedCommand'.
    assert.commandFailedWithCode(testDB.runCommand({search: "a different UUID"}), 31086);

    // Reset the state associated with the cursor id and run a search command which
    // succeeds.
    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    assert.commandWorked(testDB.runCommand({search: "a UUID"}));
    // Run another search command. We did not set up any state on the mock for another
    // client, though, so this should fail.
    assert.commandFailedWithCode(testDB.runCommand({search: "a UUID"}), 31094);
}

//
// The client in the remaining tests is well-behaving and obeys the find/getMore cursor
// iteration protocol.
//

// Open a cursor and exhaust it.
{
    const cursorId = NumberLong(123);
    const searchCmd = {search: "a UUID"};
    const cursorHistory = [
        {
            expectedCommand: searchCmd,
            response:
                {ok: 1, cursor: {firstBatch: [{_id: 0}, {_id: 1}], id: cursorId, ns: "testColl"}}
        },
        {
            expectedCommand: {getMore: cursorId, collection: "testColl"},
            response: {
                ok: 1,
                cursor: {
                    id: cursorId,
                    ns: "testColl",
                    nextBatch: [
                        {_id: 2},
                        {_id: 3},
                    ]
                }
            }
        },
        {
            expectedCommand: {getMore: cursorId, collection: "testColl"},
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: "testColl",
                    nextBatch: [
                        {_id: 4},
                    ]
                }
            }
        },
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: cursorHistory}));
    let resp = assert.commandWorked(testDB.runCommand(searchCmd));

    const cursor = new DBCommandCursor(testDB, resp);
    const arr = cursor.toArray();
    assert.eq(arr, [{_id: 0}, {_id: 1}, {_id: 2}, {_id: 3}, {_id: 4}]);

    // Make sure there are no remaining queued responses.
    resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);
}

// Open a cursor, but don't exhaust it, checking the 'killCursors' functionality of mongotmock.
{
    const cursorId = NumberLong(123);
    const searchCmd = {search: "a UUID"};
    const cursorHistory = [
        {
            expectedCommand: searchCmd,
            response:
                {ok: 1, cursor: {firstBatch: [{_id: 0}, {_id: 1}], id: cursorId, ns: "testColl"}}
        },
        {
            expectedCommand: {killCursors: "testColl", cursors: [cursorId]},
            response: {
                cursorsKilled: [cursorId],
                cursorsNotFound: [],
                cursorsAlive: [],
                cursorsUnknown: [],
                ok: 1,
            }
        },
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: cursorHistory}));

    let resp = assert.commandWorked(testDB.runCommand(searchCmd));

    {
        const cursor = new DBCommandCursor(testDB, resp);

        const next = cursor.next();
        assert.eq(next, {_id: 0});

        // Don't iterate the cursor any more! We want to make sure the DBCommandCursor has to
        // kill it.
        cursor.close();
    }

    // Make sure there are no remaining queued responses.
    resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);
}

// Test with multiple clients.
{
    const searchCmd = {search: "a UUID"};

    const cursorIdA = NumberLong(123);
    const cursorAHistory = [
        {
            expectedCommand: searchCmd,
            response: {
                ok: 1,
                cursor: {firstBatch: [{_id: "cursor A"}, {_id: 1}], id: cursorIdA, ns: "testColl"}
            }
        },
        {
            expectedCommand: {getMore: cursorIdA, collection: "testColl"},
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: "testColl",
                    nextBatch: [
                        {_id: 2},
                        {_id: 3},
                    ]
                }
            }
        },
    ];

    const cursorIdB = NumberLong(456);
    const cursorBHistory = [
        {
            expectedCommand: searchCmd,
            response: {
                ok: 1,
                cursor: {firstBatch: [{_id: "cursor B"}, {_id: 1}], id: cursorIdB, ns: "testColl"}
            }
        },
        {
            expectedCommand: {getMore: cursorIdB, collection: "testColl"},
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: "testColl",
                    nextBatch: [
                        {_id: 2},
                        {_id: 3},
                    ]
                }
            }
        },
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorIdA, history: cursorAHistory}));
    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorIdB, history: cursorBHistory}));

    let responses = [
        assert.commandWorked(testDB.runCommand(searchCmd)),
        assert.commandWorked(testDB.runCommand(searchCmd))
    ];

    const cursors =
        [new DBCommandCursor(testDB, responses[0]), new DBCommandCursor(testDB, responses[1])];

    // The mock responses should respect a FIFO order. The first cursor should get the
    // responses for cursor A, and the second cursor should get the responses for cursor B.
    {
        const firstDoc = cursors[0].next();
        assert.eq(firstDoc._id, "cursor A");
    }

    {
        const firstDoc = cursors[1].next();
        assert.eq(firstDoc._id, "cursor B");
    }

    // Iterate the two cursors together.
    const nDocsPerCursor = 4;
    for (let i = 1; i < nDocsPerCursor; i++) {
        for (let c of cursors) {
            const doc = c.next();
            assert.eq(doc._id, i);
        }
    }

    // Make sure there are no remaining queued responses.
    const resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);
}

// Test that mongotmock can return a merging pipeline.
{
    const cursorId = NumberLong(123);
    const pipelineCmd = {
        "planShardedSearch": "collName",
        "db": testDB.getName(),
        "collectionUUID": "522cdf5e-54fc-4230-9d45-49da990e8ea7",
        "query": {"text": {"path": "title", "query": "godfather"}}

    };
    const cursorHistory = [
        {
            expectedCommand: pipelineCmd,
            // Real metaPipelines will be significantly larger, but this is fine for testing the
            // mock.
            response: {
                ok: 1,
                protocolVersion: 1,
                metaPipeline: [{$documents: [{facetOne: 1, facetTwo: 2}]}]
            }
        },
    ];

    assert.commandWorked(
        testDB.runCommand({setMockResponses: 1, cursorId: cursorId, history: cursorHistory}));
    let resp = assert.commandWorked(testDB.runCommand(pipelineCmd));
    assert.eq(resp, cursorHistory[0].response);

    // Make sure there are no remaining queued responses.
    resp = assert.commandWorked(testDB.runCommand({getQueuedResponses: 1}));
    assert.eq(resp.numRemainingResponses, 0);
}

mongotMock.stop();
}());
