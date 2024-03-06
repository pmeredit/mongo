/**
 * Starts a mock AWS STS Server
 */

// These faults must match the list of faults in sts_http_server.py, see the
// SUPPORTED_FAULT_TYPES list in sts_http_server.py
const STS_FAULT_403 = "fault_403";

class MockSTSServer {
    /**
     * Create a new webserver.
     *
     * @param {string} fault_type
     */
    constructor(fault_type) {
        this.python = "python3";
        this.fault_type = fault_type;

        if (_isWindows()) {
            this.python = "python.exe";
        }

        print("Using python interpreter: " + this.python);

        this.web_server_py =
            "src/mongo/db/modules/enterprise/jstests/external_auth/lib/sts_http_server.py";
        this.port = allocatePort();
    }

    /**
     * Start a web server
     */
    start() {
        print("Mock STS Web server is listening on port: " + this.port);

        let args = [
            this.python,
            "-u",
            this.web_server_py,
            "--port=" + this.port,
        ];

        if (this.fault_type) {
            args.push("--fault=" + this.fault_type);
        }

        clearRawMongoProgramOutput();

        this.pid = _startMongoProgram({args: args});
        assert(checkProgram(this.pid));

        assert.soon(function() {
            return rawMongoProgramOutput().search("Mock STS Web Server Listening") !== -1;
        });
        sleep(1000);
        print("Mock KMS Server successfully started");
    }

    /**
     * Get the URL.
     *
     * @return {string} url of http server
     */
    getURL() {
        return "http://localhost:" + this.port;
    }

    /**
     * Stop the web server
     */
    stop() {
        stopMongoProgramByPid(this.pid);
    }
}

// Load up common variables from a json file.
const aws_common =
    JSON.parse(cat("src/mongo/db/modules/enterprise/jstests/external_auth/lib/aws_common.json"));
