// Verify auth is sent to audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const SERVER_CERT = "jstests/libs/server.pem";
const SERVER_USER = "CN=server,OU=Kernel,O=MongoDB,L=New York City,ST=New York,C=US";

const CA_CERT = "jstests/libs/ca.pem";

const x509_options = {
    sslMode: "allowSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT
};

const port = allocatePort();

function runTest({options, func}) {
    const mongod = MongoRunner.runMongodAuditLogger(Object.merge(options, {port: port, auth: ''}));
    func({conn: mongod, audit: mongod.auditSpooler()});

    MongoRunner.stopMongod(mongod);
}

function checkScram({authmech, conn, audit}) {
    const admin = conn.getDB("admin");
    const db = conn.getDB("test");

    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
    assert(admin.auth("admin", "pwd"));

    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "pwd", roles: []}));
    admin.logout();

    let runWithCleanAudit = function(desc, func) {
        print(`Testing audit log for ${desc}`);
        audit.fastForward();
        func();
        audit.assertNoNewEntries("authenticate");
    };

    // Check for positive auditing of authentications.
    runWithCleanAudit("positive-auth", function() {
        assert(db.auth({mechanism: authmech, user: "user1", pwd: "pwd"}));
        const success =
            audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
        assert.eq(success.result, 0);
    });

    // Check that connecting via shell only audits once.
    runWithCleanAudit("positive-auth-via-shell", function() {
        const uri = 'mongodb://user1:pwd@localhost:' + port + '/test';
        const cmd = 'db.coll1.find({});';
        const shell = runMongoProgram('mongo', uri, '--eval', cmd);
        const shellSuccess =
            audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
        assert.eq(shellSuccess.result, 0);
    });

    // Negative auditing (incorrect password).
    runWithCleanAudit("incorrect-password", function() {
        assert(!db.auth({mechanism: authmech, user: "user1", pwd: "wrong_pwd"}));
        const pwdFailure =
            audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
        assert.eq(pwdFailure.result, ErrorCodes.AuthenticationFailed);
    });

    // Negative auditing (unknown user).
    runWithCleanAudit("unknown-user", function() {
        assert(!db.auth({mechanism: authmech, user: "unknown_user", pwd: "pwd"}));
        const userFailure = audit.assertEntry(
            "authenticate", {user: "unknown_user", db: "test", mechanism: authmech});
        assert.eq(userFailure.result, ErrorCodes.AuthenticationFailed);
    });

    // Negative auditing (unknown mechanism).
    // Explicitly call saslStart to avoid hitting client failure at unknown mechanism.
    runWithCleanAudit("unknown-mechanism", function() {
        assert.commandFailed(db.runCommand({saslStart: 1, mechanism: "HAXX", payload: ""}));
        const mechFailure =
            audit.assertEntry("authenticate", {user: "", db: "test", mechanism: "HAXX"});
        assert.eq(mechFailure.result, ErrorCodes.MechanismUnavailable);
    });

    // Negative auditing (unknown mechanism, known user).
    runWithCleanAudit("unknown-mechanism-known-user", function() {
        assert.commandFailed(db.runCommand({authenticate: 1, mechanism: "HAXX", user: "user1"}));
        const mechFailure =
            audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: "HAXX"});
        assert.eq(mechFailure.result, ErrorCodes.MechanismUnavailable);
    });
}

function checkX509({conn, audit}) {
    const CLIENT_CERT = "jstests/libs/client.pem";
    const CLIENT_USER = "CN=client,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US";

    const external = conn.getDB("$external");
    external.createUser({
        user: CLIENT_USER,
        roles: [
            {'role': 'userAdminAnyDatabase', 'db': 'admin'},
            {'role': 'readWriteAnyDatabase', 'db': 'admin'},
            {'role': 'clusterMonitor', 'db': 'admin'},
        ]
    });

    // Localhost exception should not be in place anymore
    const test = conn.getDB("test");
    assert.throws(function() {
        test.foo.findOne();
    }, [], "read without login");

    let runWithCleanAudit = function(desc, func) {
        print(`Testing audit log for ${desc}`);
        audit.fastForward();
        func();
        audit.assertNoNewEntries("authenticate");
    };

    function runTlsShell(func) {
        const args = [
            'mongo',
            '--tls',
            '--tlsAllowInvalidHostnames',
            `--tlsCAFile=${CA_CERT}`,
            `--tlsCertificateKeyFile=${CLIENT_CERT}`,
            '--authenticationMechanism=MONGODB-X509',
            `mongodb://${conn.host}`,
            '--eval',
            `(${func.toString()})();`
        ];
        let result = _runMongoProgram(...args);
        assert(result == ErrorCodes.OK, 'TLS subshell did not succeed');
    }

    runWithCleanAudit('positive-auth', function() {
        runTlsShell(function() {
            const test = db.getSiblingDB("test");
            test.foo.findOne();
        });

        const auditResult = audit.assertEntry(
            "authenticate", {user: CLIENT_USER, db: "$external", mechanism: "MONGODB-X509"});
        assert.eq(auditResult.result, ErrorCodes.OK);
    });

    runWithCleanAudit('invalid-db', function() {
        runTlsShell(function() {
            const nowhere = db.getSiblingDB("nowhere");
            assert.commandFailed(nowhere.runCommand({authenticate: 1, mechanism: "MONGODB-X509"}));
        });

        const auditResult = audit.assertEntry(
            "authenticate", {user: CLIENT_USER, db: "nowhere", mechanism: "MONGODB-X509"});
        assert.eq(auditResult.result, ErrorCodes.ProtocolError);
    });

    runWithCleanAudit('invalid-user', function() {
        runTlsShell(function() {
            const external = db.getSiblingDB("$external");
            assert.commandFailed(
                external.runCommand({authenticate: 1, mechanism: "MONGODB-X509", user: "no one"}));
        });

        const auditResult = audit.assertEntry(
            "authenticate", {user: "no one", db: "$external", mechanism: "MONGODB-X509"});
        assert.eq(auditResult.result, ErrorCodes.AuthenticationFailed);
    });

    runWithCleanAudit('mismatched-user', function() {
        runTlsShell(function() {
            const external = db.getSiblingDB("$external");
            external.createUser({
                user: "someone",
                roles: [
                    {'role': 'userAdminAnyDatabase', 'db': 'admin'},
                    {'role': 'readWriteAnyDatabase', 'db': 'admin'},
                    {'role': 'clusterMonitor', 'db': 'admin'},
                ]
            });
        });

        runTlsShell(function() {
            const external = db.getSiblingDB("$external");
            assert.commandFailed(
                external.runCommand({authenticate: 1, mechanism: "MONGODB-X509", user: "someone"}));
        });

        const auditResult = audit.assertEntry(
            "authenticate", {user: "someone", db: "$external", mechanism: "MONGODB-X509"});
        assert.eq(auditResult.result, ErrorCodes.AuthenticationFailed);
    });
}

// Be specific about the mechanism in case the default changes
runTest({
    options: {
        setParameter: {
            authenticationMechanisms: "SCRAM-SHA-1",
        }
    },
    func: function({conn, audit}) {
        checkScram({authmech: "SCRAM-SHA-1", conn: conn, audit: audit});
    }
});

runTest({
    options: {
        setParameter: {
            authenticationMechanisms: "SCRAM-SHA-256",
        }
    },
    func: function({conn, audit}) {
        checkScram({authmech: "SCRAM-SHA-256", conn: conn, audit: audit});
    }
});

runTest({
    options: Object.merge(x509_options, {
        setParameter: {
            authenticationMechanisms: "MONGODB-X509",
        }
    }),
    func: function({conn, audit}) {
        checkX509({conn: conn, audit: audit});
    }
});
})();
