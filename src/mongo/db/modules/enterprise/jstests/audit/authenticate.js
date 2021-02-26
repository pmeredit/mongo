// Verify auth is sent to audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

const SERVER_CERT = "jstests/libs/server.pem";
const SERVER_USER = "CN=server,OU=Kernel,O=MongoDB,L=New York City,ST=New York,C=US";

const CA_CERT = "jstests/libs/ca.pem";

const x509_options = {
    sslMode: "allowSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT
};

const mock_sts = new MockSTSServer();

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

function checkIam({conn, audit}) {
    const admin = conn.getDB("admin");

    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
    assert(admin.auth("admin", "pwd"));

    const external = conn.getDB("$external");
    assert.commandWorked(external.runCommand({createUser: MOCK_AWS_ACCOUNT_ARN, roles: []}));
    assert.commandWorked(external.runCommand({createUser: MOCK_AWS_TEMP_ACCOUNT_ARN, roles: []}));
    assert.commandWorked(
        external.runCommand({createUser: MOCK_AWS_ACCOUNT_ASSUME_ROLE_GENERAL_ARN, roles: []}));
    admin.logout();

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

    let checkAuth = function({authObj, auditObj, code}) {
        const authBaseObj = {mechanism: 'MONGODB-AWS'};
        if (code === ErrorCodes.OK) {
            assert(external.auth(Object.merge(authBaseObj, authObj)), "Authentication failed");
        } else {
            assert(!external.auth(Object.merge(authBaseObj, authObj)), "Authentication succeeded");
        }

        const auditBaseObj = {db: "$external", mechanism: "MONGODB-AWS"};
        const auditResult = audit.assertEntry("authenticate", Object.merge(auditObj, auditBaseObj));
        assert.eq(auditResult.result, code);
    };

    mock_sts.start();

    runWithCleanAudit("valid-perm", function() {
        checkAuth({
            authObj: {user: MOCK_AWS_ACCOUNT_ID, pwd: MOCK_AWS_ACCOUNT_SECRET_KEY},
            auditObj: {user: MOCK_AWS_ACCOUNT_ARN},
            code: ErrorCodes.OK
        });
    });

    runWithCleanAudit("valid-temp", function() {
        checkAuth({
            authObj: {
                user: MOCK_AWS_TEMP_ACCOUNT_ID,
                pwd: MOCK_AWS_TEMP_ACCOUNT_SECRET_KEY,
                awsIamSessionToken: MOCK_AWS_TEMP_ACCOUNT_SESSION_TOKEN,
            },
            auditObj: {user: MOCK_AWS_TEMP_ACCOUNT_ARN},
            code: ErrorCodes.OK
        });
    });

    runWithCleanAudit("valid-assume-role", function() {
        checkAuth({
            authObj: {
                user: MOCK_AWS_ACCOUNT_ASSUME_ROLE_ID,
                pwd: MOCK_AWS_ACCOUNT_ASSUME_ROLE_SECRET_KEY,
                awsIamSessionToken: MOCK_AWS_ACCOUNT_ASSUME_ROLE_SESSION_TOKEN,
            },
            auditObj: {user: MOCK_AWS_ACCOUNT_ASSUME_ROLE_GENERAL_ARN},
            code: ErrorCodes.OK
        });
    });

    runWithCleanAudit("invalid-user", function() {
        const badUser = "nobody";
        checkAuth({
            authObj: {user: badUser, pwd: MOCK_AWS_ACCOUNT_SECRET_KEY},
            auditObj: {
                // We use the initial user name since we could not resolve the ARN.
                user: badUser
            },
            code: ErrorCodes.AuthenticationFailed
        });
    });

    runWithCleanAudit("invalid-key", function() {
        const badKey = "sesame";
        checkAuth({
            authObj: {user: MOCK_AWS_ACCOUNT_ID, pwd: badKey},
            auditObj: {
                // We use the initial user name since we could not resolve the ARN.
                user: MOCK_AWS_ACCOUNT_ID
            },
            code: ErrorCodes.AuthenticationFailed
        });
    });

    mock_sts.stop();

    runWithCleanAudit("valid-perm-no-server", function() {
        checkAuth({
            authObj: {user: MOCK_AWS_ACCOUNT_ID, pwd: MOCK_AWS_ACCOUNT_SECRET_KEY},
            auditObj: {
                // We use the initial user name since we could not resolve the ARN.
                user: MOCK_AWS_ACCOUNT_ID
            },
            code: ErrorCodes.AuthenticationFailed
        });
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

runTest({
    options: {
        setParameter:
            {authenticationMechanisms: "MONGODB-AWS,SCRAM-SHA-256", awsSTSUrl: mock_sts.getURL()}
    },
    func: function({conn, audit}) {
        checkIam({conn: conn, audit: audit});
    }
});
})();
