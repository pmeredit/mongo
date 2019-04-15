/**
 * Starts a mock KMS Server to test
 * FLE encryption and decryption.
 */

class MockKMSServer {
    /**
    * Create a new webserver.
    *
    * @param {string} fault_type
    * @param {bool} disableFaultsOnStartup optionally disable fault on startup
    */
    constructor() {
        this.python = "python3";

        if (_isWindows()) {
            this.python = "python.exe";
        }

        print("Using python interpreter: " + this.python);

        this.web_server_py = "src/mongo/db/modules/enterprise/jstests/fle/lib/kms_http_server.py";
        this.port = -1;
    }

    start() {
        this.port = allocatePort();
        print("Mock Web server is listening on port: " + this.port);

        let args = [this.python, "-u", this.web_server_py, "--port=" + this.port];
        this.pid = _startMongoProgram({args: args});
        assert(checkProgram(this.pid));

        assert.soon(function() {
            return rawMongoProgramOutput().search("Mock KMS Server Listening") !== -1;
        });
        print("Mock KMS Server successfully started");
    }

    /**
     * Get the URL.
     *
     * @return {string} url of http server
     */
    getURL() {
        return "https://localhost:" + this.port;
    }

    stop() {
        stopMongoProgramByPid(this.pid);
    }
}