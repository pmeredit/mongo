/**
 * Starts an OIDC key server.
 */

class OIDCKeyServer {
    /**
     * Create a new webserver.
     */
    constructor() {
        pwd = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
        this.python = "python3";
        this.jwk = pwd + '/oidc_keys.json';

        if (_isWindows()) {
            this.python = "python.exe";
        }

        print("Using python interpreter: " + this.python);

        this.web_server_py = pwd + '/oidc_key_server.py';
        this.port = allocatePort();
    }

    /**
     * Start a web server
     */
    start() {
        print("OIDC Key server is listening on port: " + this.port);

        const args = [
            this.python,
            "-u",
            this.web_server_py,
            "--port=" + this.port,
            this.jwk,
        ];

        clearRawMongoProgramOutput();

        this.pid = _startMongoProgram({args: args});
        assert(checkProgram(this.pid));

        assert.soon(function() {
            return rawMongoProgramOutput().search("OIDC Key Server Listening") !== -1;
        });
        sleep(1000);
        print("OIDC Key Server successfully started");
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
