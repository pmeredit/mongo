/**
 * Control MongoCryptD
 */
class MongoCryptD {
    /**
    * Create a new mongocryptd.
    */
    constructor() {
        this.mongocryptd = "mongocryptd";
        if (_isWindows()) {
            this.mongocryptd = "mongocryptd.exe";
        }
        this.pid = undefined;
        this.port = -1;
        this.conn = undefined;
    }

    /**
     * Get the Port.
     *
     * @return {number} port number of mongocryptd
     */
    getPort() {
        return this.port;
    }

    /**
     *  Start MongoCryptd and wait for it to start
     *
     *  @param idleTimeoutSecs Idle Timeout in seconds
     */
    start(idleTimeoutSecs = 0) {
        this.port = allocatePort();
        print("Mongocryptd: " + this.port);

        let conn_str;
        let args;

        args = [this.mongocryptd];

        if (_isWindows()) {
            conn_str = "127.0.0.1:" + this.port;
            args.push("--port=" + this.port);
        } else {
            // TODO - set sock directory
            conn_str = "/tmp/mongocryptd.sock";
        }

        args.push("--setParameter");
        args.push("enableTestCommands=1");
        args.push("-vvv");

        if (idleTimeoutSecs > 0) {
            args.push("--idleShutdownTimeoutSecs=" + idleTimeoutSecs);
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
                    print("Could not start mongo program at " + port +
                          ", process ended with exit code: " + res.exitCode);
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + this.port, 30 * 1000);

        this.conn = conn;
        print("Mongocryptd sucessfully started.");
    }

    /**
     *  Stop MongoCryptd.
     *
     *  @returns exit code of program
     */
    stop() {
        return stopMongoProgramByPid(this.pid);
    }

    /**
     * Get a new connection to mongocryptd
     *
     * @return {Mongo} connection
     */
    getConnection() {
        return this.conn;
    }
}
