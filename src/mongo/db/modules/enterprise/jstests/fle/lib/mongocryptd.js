/**
 * Control MongoCryptD
 */
export class MongoCryptD {
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

        args.push("--port=" + this.port);
        if (_isWindows()) {
            conn_str = "127.0.0.1:" + this.port;
        } else {
            conn_str = MongoRunner.dataDir + "/mongocryptd.sock";
            conn_str = conn_str.replace(new RegExp('/', 'g'), '%2F');
            args.push("--unixSocketPrefix=" + MongoRunner.dataDir);
        }
        conn_str = "mongodb://" + conn_str + "/?ssl=false";

        // Convert key-value pairs (e.g. from test suite config or command-line flags) into
        // --setParameter key=value arguments
        if (TestData !== undefined && TestData.setParametersMongocryptd) {
            const setParams = TestData.setParametersMongocryptd;
            for (let [paramName, paramValue] of Object.entries(setParams)) {
                // --setParameter takes boolean values as lowercase strings.
                if (typeof paramValue === "boolean") {
                    paramValue = paramValue ? "true" : "false";
                }

                args.push("--setParameter");
                args.push(paramName + "=" + paramValue);
            }
        }

        args.push("--setParameter");
        args.push("enableTestCommands=1");
        args.push("-vvv");

        this.pidFile = MongoRunner.dataDir + "/cryptd.pid";
        args.push("--pidfilepath=" + this.pidFile);

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
                    print("Could not start mongo program at " + conn_str +
                          ", process ended with exit code: " + res.exitCode);
                    return true;
                }
            }
            return false;
        }, "unable to connect to mongo program on port " + conn_str, 30 * 1000);

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

    /**
     * Read the pid file as JSON
     *
     * @return {Object} a JSON object
     */
    readPidFile() {
        const result = cat(this.pidFile);

        try {
            return JSON.parse(result);
        } catch (e) {
            jsTestLog("Failed to parse: " + result + "\n" + result);
            throw e;
        }
    }

    /**
     * Get the PID
     *
     * @return {integer} process id
     */
    getPid() {
        return this.pid;
    }

    /**
     * Get the Port
     *
     * @return {integer} port
     */
    getPort() {
        return this.port;
    }
}
