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
 */
function mongotCommandForQuery(query, collName, db, collectionUUID) {
    // Note - this will change with the imminent merge of SERVER-41076, the command format change.
    // It will eventually be {searchBeta: collName, $db: db, collectionUUID, query}.
    return {searchBeta: collectionUUID, $db: db, query};
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

class MongotMock {
    /**
    * Create a new mongotmock.
    */
    constructor() {
        this.mongotMock = "mongotmock";
        this.pid = undefined;
        this.port = -1;
        this.conn = undefined;
    }

    /**
     *  Start mongotmock and wait for it to start.
     */
    start() {
        this.port = allocatePort();
        print("mongotmock: " + this.port);

        const conn_str = MongoRunner.dataDir + "/mongocryptd.sock";
        const args = [this.mongotMock];

        args.push("--port=" + this.port);
        // mongotmock uses mongocryptd.sock.
        args.push("--unixSocketPrefix=" + MongoRunner.dataDir);

        args.push("--setParameter");
        args.push("enableTestCommands=1");
        args.push("-vvv");

        args.push("--pidfilepath=" + MongoRunner.dataDir + "/cryptd.pid");

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
        const connection = this.getConnection();
        // Check the remaining history on the mock. There should be 0 remaining queued commands.
        const resp = assert.commandWorked(connection.adminCommand({getQueuedResponses: 1}));
        assert.eq(resp.numRemainingResponses, 0);

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
     */
    setMockResponses(expectedMongotMockCmdsAndResponses, cursorId) {
        const connection = this.getConnection();
        const setMockResponsesCommand = {
            setMockResponses: 1,
            cursorId: NumberLong(cursorId),
            history: expectedMongotMockCmdsAndResponses,
        };
        assert.commandWorked(connection.getDB("mongotmock").runCommand(setMockResponsesCommand));
    }
}
