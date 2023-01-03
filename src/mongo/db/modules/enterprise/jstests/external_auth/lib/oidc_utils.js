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
        this.jwk = JSON.stringify({
            "custom-key-1": pwd + '/custom-key-1.json',
            "custom-key-2": pwd + '/custom-key-2.json',
            "custom-key-3": pwd + '/custom-key-3.json',
        });

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

/**
 * ProgramRunner only provides output that has been multiplexed with other
 * program output under the tester's process space.
 * Demultiplex it by capturing the pid and filtering for just those lines.
 */
function runProgramAndCaptureOutput(args) {
    clearRawMongoProgramOutput();
    const pid = _startMongoProgram({args: args});
    assert(checkProgram(pid));
    assert.eq(waitProgram(pid), 0, "Process failed: " + tojson(args));

    const prefix = 'sh' + NumberInt(pid) + '| ';
    let output = '';
    rawMongoProgramOutput().split("\n").forEach(function(line) {
        if (line.startsWith(prefix)) {
            output = output + line.substr(prefix.length) + "\n";
        }
    });

    assert.neq(output.length, 0, "Process produced no output");
    return output;
}

/**
 * Sign a JWT.
 */
function OIDCsignJWT(header, token, key = undefined, algo = undefined) {
    const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
    if (key === undefined) {
        key = LIB + 'custom-key-1.priv';
        if (header['kid'] === undefined) {
            header['kid'] = 'custom-key-1';
        }
    }

    const args = [
        _isWindows() ? 'python.exe' : 'python3',
        LIB + 'sign_jwt.py',
        '--header',
        JSON.stringify(header),
        '--token',
        JSON.stringify(token),
        '--key',
        key,
    ];
    if (algo != undefined) {
        args.push('--algorithm');
        args.push(algo);
    }
    print("Signing token: " + JSON.stringify(args));
    return runProgramAndCaptureOutput(args).trim();
}

/**
 * Generate a BSON payload.
 */
function OIDCgenerateBSON(payload) {
    const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';

    const args = [
        _isWindows() ? 'python.exe' : 'python3',
        LIB + 'gen_bson.py',
        '--payload',
        JSON.stringify(payload),
    ];
    print("Generating BSON payload: " + JSON.stringify(args));
    return BinData(0, runProgramAndCaptureOutput(args).trim());
}
