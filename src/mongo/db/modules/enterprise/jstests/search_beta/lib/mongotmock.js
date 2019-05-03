/**
 * Control mongotmock.
 */
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
}
