"use strict";

// Some library functions for LDAP authorization jstests

var baseLDAPUrls = ["ldaptest.10gen.cc"];

// relative path to the root dir of the enterprise module

var assetsPath = pwd() + "/src/mongo/db/modules/enterprise/jstests/external_auth/assets/";

// Saslauthd configuration file
var saslauthdConfigFile = assetsPath + "saslauthd.conf";

// Saslauthd working directory
var saslauthdPath = "/tmp/test-externAuth-saslauthd";

// this ensures different machines can all use the same certificates
var saslHostName = "localhost";

// default DN suffix for users
var defaultUserDNSuffix = "ou=Users,dc=10gen,dc=cc";

// default user for testing LDAP authorization
var adminUser = "ldapz_admin";
var adminUserDN = "cn=ldapz_admin," + defaultUserDNSuffix;

// default password used by most users
var defaultPwd = "Secret123";

// default role assigned to most users
var defaultRole = "cn=testWriter,ou=Groups,dc=10gen,dc=cc";

// Simple and SASL bind requests may need different names to refer to the
// same user entity.
var simpleAuthenticationUser = "cn=ldapz_ldap_bind," + defaultUserDNSuffix;
var saslAuthenticationUser = "ldapz_ldap_bind";

function LDAPTestConfigGenerator() {
    Object.defineProperties(this, {
        "authenticationMechanisms": {
            "set": function(mechs) {
                if (!Array.isArray(mechs)) {
                    throw "authenticationMechanisms must be an Array";
                }
                if (!mechs.includes("SCRAM-SHA-1")) {
                    throw "authenticationMechanisms must include SCRAM-SHA-1";
                }

                this._authenticationMechanisms = mechs;
            },
            "get": function() {
                return this._authenticationMechanisms;
            },
        }
    });

    this.auth = "";
    this.authenticationMechanisms = ["PLAIN", "SCRAM-SHA-1"];
    this.useSaslauthd = false;

    this.ldapServers = baseLDAPUrls;
    this.ldapTransportSecurity = "none";
    this.ldapAuthzQueryTemplate = "cn={USER}," + defaultUserDNSuffix + "?memberOf";
    this.ldapBindMethod = "simple";
    this.ldapBindSaslMechanisms = "DIGEST-MD5";
    this.ldapQueryUser = undefined;
    this.ldapQueryPassword = "Admin001";
    this.ldapUserToDNMapping = undefined;

    this.generateEnvConfig = function() {
        return {
            // override default config file
            "KRB5_CONFIG": assetsPath + "krb5.conf",
            // debug information
            "KRB5_TRACE": "/dev/stdout",
            // used only for kerberos authentication
            "KRB5_KTNAME": "jstests/libs/mockservice.keytab",
            // used only for the kerberos SASL bind
            "KRB5_CLIENT_KTNAME": assetsPath + "ldapz_ldap_bind.keytab",
            // used only for TLS
            "LDAPTLS_CACERT": assetsPath + "ldaptest-ca.pem",
            // force libldap to pass hostnames down to Cyrus SASL, rather than "helping" by
            // resolving them to IPs
            "LDAPSASL_NOCANON": "on",
        };
    };

    this.generateMongodConfig = function() {
        var config = {};

        // ensures x509 authn tests can run with SSL
        config.sslMode = "preferSSL";
        config.sslPEMKeyFile = "jstests/libs/server.pem";
        config.sslCAFile = "jstests/libs/ca.pem";
        config.sslAllowInvalidHostnames = "";
        config.clusterAuthMode = "x509";

        config.ldapServers = this.ldapServers.join(",");
        config.ldapTransportSecurity = this.ldapTransportSecurity;

        config.ldapAuthzQueryTemplate = this.ldapAuthzQueryTemplate;

        config.ldapBindMethod = this.ldapBindMethod;
        if (!(this.ldapBindMethod === "simple")) {
            config.ldapBindSaslMechanisms = this.ldapBindSaslMechanisms;
        }

        if (this.ldapQueryUser === undefined) {
            if (this.ldapBindMethod === "simple") {
                config.ldapQueryUser = simpleAuthenticationUser;
            } else if (this.ldapBindMethod === "sasl") {
                config.ldapQueryUser = ldapz_ldap_bind;
            }
        } else {
            config.ldapQueryUser = this.ldapQueryUser;
        }
        config.ldapQueryPassword = this.ldapQueryPassword;
        if (!(this.ldapUserToDNMapping === undefined)) {
            // Some string rewriting is needed here. Quotes need to be single quotes to make Windows
            // argument parsing happy, and JSONified escaped unicode characters need to be
            // reformatted, because stringify will try to escape the escape characters.
            config.ldapUserToDNMapping = JSON.stringify(this.ldapUserToDNMapping)
                                             .replace(/"/g, "'")
                                             .replace(/\\\\u/g, "\\u");
        }

        var setParameter = {
            authenticationMechanisms: this.authenticationMechanisms,
            saslHostName: saslHostName,
            saslServiceName: "mockservice"
        };
        if (this.useSaslauthd === true) {
            setParameter.saslauthdPath = saslauthdPath + "/mux";
        }
        config.setParameter = setParameter;
        config.env = this.generateEnvConfig();

        print(tojson(config));
        return config;
    };

    this.generateReplicaSetConfig = function() {
        var mongodConfig = this.generateMongodConfig();
        mongodConfig.replSet = "ldapAuthzReplset";

        return {
            name: "ldapAuthzReplset",
            nodes: {n0: mongodConfig, n1: mongodConfig, n2: mongodConfig},
            useHostName: true
        };
    };

    this.generateShardingConfig = function() {
        var mongodConfig = this.generateMongodConfig();

        var config = {};
        config.name = "ldapAuthzSharding";
        config.shards = 2;
        config.mongos = 2;

        var other = {};
        other.enableBalancer = true;
        other.shardOptions = Object.extend({}, mongodConfig, true);
        other.configOptions = Object.extend({}, mongodConfig, true);
        other.useHostname = true;
        other.mongosOptions = Object.extend({}, mongodConfig, true);
        config.other = other;

        return config;

    };
}

// a helper function that runs auth and verify the result
// should be called from the tests themselves
function authAndVerify(m, authArgs) {
    // m won't exist for tests using SSL
    if (!m) {
        m = db.getMongo();
    }

    var externalDB = m.getDB("$external");

    externalDB.auth(authArgs.authOptions);

    var status = externalDB.runCommand({"connectionStatus": 1});

    // The default user and role used for most of the tests
    var authInfo = {
        "authenticatedUsers": [{"user": authArgs.user, "db": "$external"}],
        "authenticatedUserRoles": [{"role": defaultRole, "db": "admin"}]
    };

    assert.eq(status.authInfo, authInfo, "unexpected authorization status");

    externalDB.logout();
}

// setup the tests with the right users and pre-populated data
// does not need to be called from the tests
function setupTest(m) {
    // Setting up custom roles
    var adminDB = m.getDB("admin");

    adminDB.createUser({
        user: "siteRootAdmin",
        pwd: "secret",
        roles: [
            {role: "root", db: "admin"},
            {role: "userAdminAnyDatabase", db: "admin"},
            {role: "clusterAdmin", db: "admin"}
        ]
    });

    adminDB.auth("siteRootAdmin", "secret");

    // This is the default role used for basic permissions verification.
    // Tests should use this role instead of creating new ones whenever possible
    var customRole = {
        createRole: defaultRole,
        privileges: [{resource: {db: "test", collection: ""}, actions: ["insert"]}],
        roles: []
    };

    assert.commandWorked(adminDB.runCommand(customRole), "role creation failed");

    adminDB.logout();
}

// Calls testCallback with callbackOptions on single mongod, replset
// and a sharded cluster with defaultConfig + additionalConfig.
// Individual tests should implement the testCallback function
function runTests(testCallback, configGenerator, callbackOptions) {
    // single mongod
    var m = MongoRunner.runMongod(configGenerator.generateMongodConfig());
    setupTest(m);
    testCallback(m, callbackOptions);
    MongoRunner.stopMongod(m);

    // replset
    var rst = new ReplSetTest(configGenerator.generateReplicaSetConfig());
    rst.startSet();
    rst.initiate();
    rst.awaitSecondaryNodes();

    var primary = rst.getPrimary();
    setupTest(primary);
    // TODO: run test on secondary as well?
    testCallback(primary, callbackOptions);
    rst.stopSet();

    // sharded
    var st = new ShardingTest(configGenerator.generateShardingConfig());
    setupTest(st.s0);
    testCallback(st.s0, callbackOptions);
    st.stop();
}

function withSaslauthd(saslAuthdConfigFile, configGenerator, callback) {
    if (_isWindows()) {
        print("saslauthd may not be spawned on Windows. Skipping saslauthd test.");
        return;
    }

    if (!(configGenerator.useSaslauthd === true)) {
        throw "Tests which use saslauthd must be configured to use saslauthd";
    }

    print("Spawning saslauthd");
    var pid = _startMongoProgram(
        "saslauthd", "-V", "-a", "ldap", "-m", saslauthdPath, "-n", "1", "-O", saslAuthdConfigFile);

    try {
        callback();
    } finally {
        print("Cleaning up saslauthd at pid " + tojson(pid));
        stopMongoProgramByPid(pid);
        var truePid = cat(saslauthdPath + "/saslauthd.pid").trim();
        print("Terminating saslauthd child process at pid " + tojson(truePid));
        run("/bin/kill", truePid);
        print("Done!");
    }
}
