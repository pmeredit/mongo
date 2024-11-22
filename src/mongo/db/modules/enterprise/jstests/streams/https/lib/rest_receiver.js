/**
 * Starts a jstest REST server
 */

import {getPython3Binary} from "jstests/libs/python.js";

export const InsufficientCurlVersionError =
    new Error("Insufficient curl version to run $http operator");
export class TestRESTServer {
    /**
     * Create a new server.
     */
    constructor() {
        this.python = getPython3Binary();

        print("Using python interpreter: " + this.python);

        this.web_server_py =
            "src/mongo/db/modules/enterprise/jstests/streams/https/lib/rest_server.py";
        this.port = allocatePort();
    }

    /**
     * Checks dependencies and starts server. Throws an error if dependency check fails.
     */
    tryStart() {
        print("Checking test server dependencies");

        let args = [
            this.python,
            this.web_server_py,
            "-c",
        ];

        this.pid = _startMongoProgram({args: args});
        let output;
        assert.soon(() => {
            output = checkProgram(this.pid);
            return output.exitCode !== undefined;
        });

        if (rawMongoProgramOutput(".*").search("Insufficient curl version") !== -1) {
            throw InsufficientCurlVersionError;
        }

        this._start();
    }

    /**
     * Starts server
     */
    _start() {
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
            return rawMongoProgramOutput(".*").search("Running on localhost:" + this.port) !== -1;
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
