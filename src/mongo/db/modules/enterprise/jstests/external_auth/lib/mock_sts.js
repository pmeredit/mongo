/**
 * Starts a mock AWS STS Server
 */

// These faults must match the list of faults in sts_http_server.py, see the
// SUPPORTED_FAULT_TYPES list in sts_http_server.py
const STS_FAULT_403 = "fault_403";

const MOCK_AWS_ACCOUNT_ARN = 'arn:aws:iam::123456789012:user/Alice';
const MOCK_AWS_ACCOUNT_ID = 'permanentuser';
const MOCK_AWS_ACCOUNT_SECRET_KEY = 'FAKEFAKEFAKEFAKEFAKEfakefakefakefakefake';

const MOCK_AWS_TEMP_ACCOUNT_ARN = 'arn:aws:iam::123456789012:user/Bob';
const MOCK_AWS_TEMP_ACCOUNT_ID = 'tempuser';
const MOCK_AWS_TEMP_ACCOUNT_SECRET_KEY = 'fakefakefakefakefakeFAKEFAKEFAKEFAKEFAKE';
const MOCK_AWS_TEMP_ACCOUNT_SESSION_TOKEN = 'FAKETEMPORARYSESSIONTOKENfaketemporarysessiontoken';

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
        this.port = -1;
    }

    /**
     * Start a web server
     */
    start() {
        this.port = allocatePort();
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
        assert(this.port != -1);

        return "http://localhost:" + this.port;
    }

    /**
     * Stop the web server
     */
    stop() {
        stopMongoProgramByPid(this.pid);
    }
}
