/**
 * Control mongotmock.
 */

/**
 * Helper to create an expected command for mongot.
 *
 * @param {Object} query - The query to be recieved by mongot.
 * @param {String} collName - The collection name.
 * @param {String} db - The database name.
 * @param {BinaryType} collectionUUID - the binary representation of a collection's UUID.
 * @param {Int} protocolVersion - If present, the version of mongot's merging logic.
 */
function mongotCommandForQuery(query, collName, db, collectionUUID, protocolVersion = null) {
    if (protocolVersion === null) {
        return {search: collName, $db: db, collectionUUID, query};
    }
    return {search: collName, $db: db, collectionUUID, query, intermediate: protocolVersion};
}

/**
 * Helper to create an expected response from mongot with a batch of results.
 *
 * @param {Array} nextBatch - Array of documents to be returned in this response.
 * @param {Number} id - The mongot cursor ID.
 * @param {String} ns - The namespace of the collection our response is for.
 * @param {Boolean} ok - True when this response is not from an error.
 */
function mongotResponseForBatch(nextBatch, id, ns, ok) {
    return {ok, cursor: {id, ns, nextBatch}};
}

/**
 * Same as above but for multiple cursors.
 */
function mongotMultiCursorResponseForBatch(
    firstCursorBatch, firstId, secondCursorBatch, secondId, ns, ok) {
    return {
        ok,
        cursors: [
            {cursor: {id: firstId, ns, nextBatch: firstCursorBatch, type: "results"}, ok},
            {cursor: {id: secondId, ns, nextBatch: secondCursorBatch, type: "meta"}, ok}
        ]
    };
}

/**
 * Helper to set a generic merge pipeline for tests that don't care about metadata. Defaults to
 * setting on the mongot partnered with mongos unless 'overrideMongot' is passed in.
 */
function setGenericMergePipeline(collName, query, dbName, stWithMock) {
    const mergingPipelineHistory = [{
        expectedCommand: {planShardedSearch: collName, query: query, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // Tests calling this don't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    }];
    const mongot = stWithMock.getMockConnectedToHost(stWithMock.st.s);
    mongot.setMockResponses(mergingPipelineHistory, 1423);
}

class MongotMock {
    /**
     * Create a new mongotmock.
     */
    constructor(options) {
        this.mongotMock = "mongotmock";
        this.pid = undefined;
        this.port = -1;
        this.conn = undefined;
        this.dataDir = (options && options.dataDir) || MongoRunner.dataDir + "/mongotmock";
        resetDbpath(this.dataDir);
    }

    /**
     *  Start mongotmock and wait for it to start.
     */
    start() {
        this.port = allocatePort();
        print("mongotmock: " + this.port);

        const conn_str = this.dataDir + "/mongocryptd.sock";
        const args = [this.mongotMock];

        args.push("--port=" + this.port);
        // mongotmock uses mongocryptd.sock.
        args.push("--unixSocketPrefix=" + this.dataDir);

        args.push("--setParameter");
        args.push("enableTestCommands=1");
        args.push("-vvv");

        args.push("--pidfilepath=" + this.dataDir + "/cryptd.pid");

        if (TestData && TestData.auth) {
            args.push("--clusterAuthMode=keyFile");
            args.push("--keyFile=" + TestData.keyFile);
        }

        this.pid = _startMongoProgram({args: args});

        assert(checkProgram(this.pid));

        // Wait for connection to be established with server
        var conn = null;
        const pid = this.pid;
        const port = this.port;

        assert.soon(function() {
            try {
                conn = new Mongo(conn_str);
                conn.pid = pid;
                return true;
            } catch (e) {
                var res = checkProgram(pid);
                if (!res.alive) {
                    print("Could not start mongo program at " + conn_str +
                          ", process ended with exit code: " + res.exitCode);
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + conn_str, 30 * 1000);

        this.conn = conn;
        print("mongotmock sucessfully started.");
    }

    /**
     *  Stop mongotmock, asserting that it shutdown cleanly.
     */
    stop() {
        // Check the remaining history on the mock. There should be 0 remaining queued commands.
        this.assertEmpty();

        return stopMongoProgramByPid(this.pid);
    }

    /**
     * Returns a connection to mongotmock.
     */
    getConnection() {
        return this.conn;
    }

    /**
     * Convenience function to set expected commands and responses for the mock mongot.
     *
     * @param {Array} expectedMongotMockCmdsAndResponses - Array of [expectedCommand, response]
     * pairs for the mock mongot.
     * @param {Number} cursorId - The mongot cursor ID.
     * @param {Number} additionalCursorId - If the initial command will return multiple cursors, and
     *     there is no getMore to set the response for the state, pass this instead.
     */
    setMockResponses(expectedMongotMockCmdsAndResponses, cursorId, additionalCursorId = 0) {
        const connection = this.getConnection();
        const setMockResponsesCommand = {
            setMockResponses: 1,
            cursorId: NumberLong(cursorId),
            history: expectedMongotMockCmdsAndResponses,
        };
        assert.commandWorked(connection.getDB("mongotmock").runCommand(setMockResponsesCommand));
        if (additionalCursorId !== 0) {
            assert.commandWorked(connection.getDB("mongotmock").runCommand({
                allowMultiCursorResponse: 1,
                cursorId: NumberLong(additionalCursorId)
            }));
        }
    }

    /**
     * Verify that no responses remain enqueued in the mock. Call this in between consecutive tests.
     */
    assertEmpty() {
        const connection = this.getConnection();
        const resp = assert.commandWorked(connection.adminCommand({getQueuedResponses: 1}));
        assert.eq(resp.numRemainingResponses, 0, resp);
    }

    /**
     * Sets the manageSearchIndex mongot mock command response to return
     * 'expectedManageSearchIndexResponse' to a single caller.
     */
    setMockSearchIndexCommandResponse(expectedManageSearchIndexResponse) {
        const connection = this.getConnection();
        const setManageSearchIndexAtlasResponseCommand = {
            setManageSearchIndexAtlasResponse: 1,
            manageSearchIndexResponse: expectedManageSearchIndexResponse
        };
        assert.commandWorked(
            connection.getDB('mongotmock').runCommand(setManageSearchIndexAtlasResponseCommand));
    }

    /**
     * Calls the manageSearchIndex mongot mock command to get the mock response.
     */
    callManageSearchIndexCommand() {
        const connection = this.getConnection();
        const manageSearchIndexCommand = {manageSearchIndex: 1};
        return assert.commandWorked(
            connection.getDB('mongotmock').runCommand(manageSearchIndexCommand));
    }
}
