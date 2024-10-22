/**
 * Starts a mock AWS ECS Metadata Server
 */

import {getPython3Binary} from "jstests/libs/python.js";

// These faults must match the list of faults in ecs_metadata_http_server.py, see the
// SUPPORTED_FAULT_TYPES list in ecs_metadata_http_server.py
export const ECS_FAULT_500 = "fault_500";

export const MOCK_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI =
    "/v2/credentials/e619b4a8-9c02-47ac-b941-52f3b6cf5d06";

export class MockECSMetadataServer {
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
            "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ecs_metadata_http_server.py";
        this.port = -1;
    }

    /**
     * Start a web server
     */
    start() {
        this.port = allocatePort();
        print("Mock ECS Metadata Web server is listening on port: " + this.port);

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
                       "Mock ECS Instance Metadata Web Server Listening") !== -1;
        });
        sleep(1000);
        print("Mock ECS Metadata Server successfully started");
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
