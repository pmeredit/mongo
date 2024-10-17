/**
 * Starts a jstest REST server
 */

import {getPython3Binary} from "jstests/libs/python.js";

export class TestRESTServer {
    /**
     * Create a new server.
     */
    constructor() {
        this.python = getPython3Binary();

        print("Using python interpreter: " + this.python);

        this.web_server_py =
            "src/mongo/db/modules/enterprise/jstests/streams/external_api/lib/rest_server.py";
        this.port = allocatePort();
    }

    /**
     * Start server
     */
    start() {
        print("Test REST server program is starting");

        let args = [
            this.python,
            this.web_server_py,
            "-p",
            this.port,
            "-d",
            this.getPayloadDirectory(),
        ];

        this.pid = _startMongoProgram({args: args});
        assert(checkProgram(this.pid));

        assert.soon(() => {
            return rawMongoProgramOutput().search("Running on localhost:" + this.port) !== -1;
        });

        print("Test REST Server successfully started");
    }

    /**
     * Get location of payloads
     */
    getPayloadDirectory() {
        return "/tmp/rest_server." + this.port;
    }

    /**
     * Clean up temporary files
     */
    cleanTempFiles() {
        removeFile(this.getPayloadDirectory());
    }

    /**
     * Get port
     */
    getPort() {
        return this.port;
    }

    /**
     * Stop the web server
     */
    stop() {
        stopMongoProgramByPid(this.pid);
    }
}