/**
 * Starts an OIDC key server.
 */
import {getPython3Binary} from "jstests/libs/python.js";

export class OIDCKeyServer {
    /**
     * Create a new webserver.
     */
    constructor(jwk_map) {
        const pwd = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
        this.python = getPython3Binary();

        this.jwk_map = jwk_map;

        print("Using python interpreter: " + this.python);

        this.web_server_py = pwd + '/oidc_key_server.py';
        this.port = allocatePort();
        this.pid = -1;
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
            this.jwk_map,
        ];

        clearRawMongoProgramOutput();

        this.pid = _startMongoProgram({args: args});
        assert(checkProgram(this.pid));

        assert.soon(function() {
            return rawMongoProgramOutput(".*").search("OIDC Key Server Listening") !== -1;
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
     * Send a request to the key server to rotate the keys.
     */
    rotateKeys(conn, keys) {
        const request = this.getURL() + '/rotateKeys?map=' + JSON.stringify(keys);
        assert.commandWorked(conn.adminCommand({httpClientRequest: 1, uri: request}));
    }

    /**
     * Stop the web server. A no-op if the server is already not running.
     */
    stop() {
        if (this.pid !== -1) {
            stopMongoProgramByPid(this.pid);
            this.pid = -1;
        }
    }
}

/**
 * ProgramRunner only provides output that has been multiplexed with other
 * program output under the tester's process space.
 * Demultiplex it by capturing the pid and filtering for just those lines.
 */
export function runProgramAndCaptureOutput(args) {
    clearRawMongoProgramOutput();
    const pid = _startMongoProgram({args: args});
    assert(checkProgram(pid));
    assert.eq(waitProgram(pid), 0, "Process failed: " + tojson(args));

    const prefix = 'sh' + NumberInt(pid) + '| ';
    let output = '';
    rawMongoProgramOutput(".*").split("\n").forEach(function(line) {
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
export function OIDCsignJWT(header, token, key = undefined, algo = undefined) {
    const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
    if (key === undefined) {
        key = LIB + 'custom-key-1.priv';
        if (header['kid'] === undefined) {
            header['kid'] = 'custom-key-1';
        }
    }

    algo = (algo === undefined) ? header['alg'] : algo;
    if (algo === undefined) {
        throw new Error("Must specify an algorithm for signing JWT!");
    }

    const args = [
        getPython3Binary(),
        LIB + 'sign_jwt.py',
        '--header',
        JSON.stringify(header),
        '--token',
        JSON.stringify(token),
        '--key',
        key,
        '--algorithm',
        algo,
    ];

    print("Signing token: " + JSON.stringify(args));
    return runProgramAndCaptureOutput(args).trim();
}

export function OIDCGenerateBSONtoFile(payload, file_out_path) {
    const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';

    return runNonMongoProgramQuietly(getPython3Binary(),
                                     LIB + 'gen_bson.py',
                                     '--payload',
                                     JSON.stringify(payload),
                                     '--output_file=' + file_out_path);
}

/**
 * Generate a BSON payload.
 */
export function OIDCgenerateBSON(payload) {
    const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';

    const args = [getPython3Binary(), LIB + 'gen_bson.py', '--payload', JSON.stringify(payload)];

    print("Generating BSON payload: " + JSON.stringify(args));
    return BinData(0, runProgramAndCaptureOutput(args).trim());
}

/**
 * Attempt to authenticate to $external using MONGODB-OIDC and an access token
 * @returns whether auth succeeds
 */
export function tryTokenAuth(conn, token) {
    return conn.getDB('$external').auth({oidcAccessToken: token, mechanism: 'MONGODB-OIDC'});
}
