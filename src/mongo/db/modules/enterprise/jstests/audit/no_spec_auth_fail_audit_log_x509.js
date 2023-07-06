// Verify that a failed speculative authentication attempt does not cause an audit log against a
// server that authenticates with X509.
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const SERVER_CERT = "jstests/libs/server.pem";
const CA_CERT = "jstests/libs/ca.pem";
const CLIENT_CERT = "jstests/libs/client.pem";

const x509_options = {
    sslMode: "allowSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT,
    setParameter: {
        authenticationMechanisms: "MONGODB-X509",
    },
    auth: "",
};

const mongo = MongoRunner.runMongodAuditLogger(x509_options);
const audit = mongo.auditSpooler();

const CLIENT_USER = "CN=client,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US";
const ext = mongo.getDB("$external");
ext.createUser({
    user: CLIENT_USER,
    roles: [
        {'role': 'readWriteAnyDatabase', 'db': 'admin'},
    ]
});

// Run speculative auth with SCRAM-SHA-256 (which is not included in the authenticationMechanisms of
// the server, so the authentication should fail).
const clientPayload = 'biwsbj1hZG1pbixyPWRlYWRiZWVmY2FmZWJhMTE=';
assert.commandWorked(ext.runCommand({
    hello: 1,
    speculativeAuthenticate:
        {saslStart: 1, mechanism: "SCRAM-SHA-256", payload: clientPayload, db: "$external"}
}));

// Run a command through the shell in order to authenticate using X509.
audit.fastForward();
function shellCmd() {
    const test = db.getSiblingDB("test");
    test.foo.findOne();
}
const args = [
    'mongo',
    '--tls',
    '--tlsAllowInvalidHostnames',
    `--tlsCAFile=${CA_CERT}`,
    `--tlsCertificateKeyFile=${CLIENT_CERT}`,
    '--authenticationMechanism=MONGODB-X509',
    `mongodb://${mongo.host}`,
    '--eval',
    `(${shellCmd.toString()})();`
];
let result = _runMongoProgram(...args);
assert(result == ErrorCodes.OK, 'TLS subshell did not succeed');

// Assert that the failed speculative authenticate did not cause an audit log event.
audit.assertNoEntry("authenticate",
                    {user: CLIENT_USER, db: "$external", mechanism: "SCRAM-SHA-256"});

MongoRunner.stopMongod(mongo);
