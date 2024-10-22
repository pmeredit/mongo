/**
 * Starts a mock AWS EC2 Metadata Server
 */

import {getPython3Binary} from "jstests/libs/python.js";

// These faults must match the list of faults in sts_http_server.py, see the
// SUPPORTED_FAULT_TYPES list in ec2_metadata_http_server.py
export const EC2_FAULT_500 = "fault_500";

export class MockEC2MetadataServer {
    /**
     * Create a new webserver.
     *
     * @param {string} fault_type
     */
    constructor(fault_type) {
        this.python = getPython3Binary();
        this.fault_type = fault_type;

        print("Using python interpreter: " + this.python);

        this.web_server_py =
            "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ec2_metadata_http_server.py";
        this.port = -1;
    }

    /**
     * Start a web server
     */
    start() {
        this.port = allocatePort();
        print("Mock EC2 Metadata Web server is listening on port: " + this.port);

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
            return rawMongoProgramOutput(".*").search(
                       "Mock EC2 Instance Metadata Web Server Listening") !== -1;
        });
        sleep(1000);
        print("Mock EC2 Metadata Server successfully started");
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
