/**
 * Starts, manages, and stops a mock LDAP server.
 */
import {getPython3Binary} from "jstests/libs/python.js";
import {
    defaultUserDNSuffix,
    LDAPTestConfigGenerator
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

export class MockLDAPServer {
    /**
     * Creates a new LDAP server.
     *
     * If only ditFile is specified, then the directory specified in LDIF in that file will be
     * loaded in and used as the LDAP server's directory for binds and searches.
     *
     * If only referralUri is specified, then all binds and searches will return
     * a referral to referral_uri. Liveness checks (which involves a rootDSE search) will
     * be performed locally without a referral.
     *
     * If both ditFile and referralUri are specified, then binds automatically use the local ditFile
     * by default while searches use the referralUri. This configuration is available since WinLDAP
     * only chases referrals for search operations by default.
     *
     * At least one of ditFile or referralUri must be specified.
     *
     * If delay is specified, then all searches and binds will be delayed by the
     * specified number of seconds. The default is 0.
     */
    constructor(ditFile, referralUri, delay = 0) {
        this.path = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapmockserver.py';
        this.port = allocatePort();
        this.pid = -1;  // Represents a server that is not running.
        this.ditFile = ditFile;
        this.referralUri = referralUri;
        this.delay = delay;

        this.python = getPython3Binary();
    }

    /**
     * Start the LDAP server.
     */
    start() {
        clearRawMongoProgramOutput();

        assert(this.ditFile || this.referralUri);

        if (this.ditFile) {
            if (this.referralUri) {
                this.pid = startMongoProgramNoConnect(this.python,
                                                      "-u",
                                                      this.path,
                                                      "--port",
                                                      this.port,
                                                      "--dit-file",
                                                      this.ditFile,
                                                      "--referral-uri",
                                                      this.referralUri,
                                                      "--delay",
                                                      this.delay);
            } else {
                this.pid = startMongoProgramNoConnect(this.python,
                                                      "-u",
                                                      this.path,
                                                      "--port",
                                                      this.port,
                                                      "--dit-file",
                                                      this.ditFile,
                                                      "--delay",
                                                      this.delay);
            }
        } else {
            this.pid = startMongoProgramNoConnect(this.python,
                                                  "-u",
                                                  this.path,
                                                  "--port",
                                                  this.port,
                                                  "--referral-uri",
                                                  this.referralUri,
                                                  "--delay",
                                                  this.delay);
        }

        assert(checkProgram(this.pid));
        assert.soon(() => rawMongoProgramOutput(".*").search("LDAPServerFactory starting on " +
                                                             this.port) !== -1);

        print("Mock LDAP server started on port " + this.port + " with delay " + this.delay);
    }

    /**
     * Get the host and port.
     *
     * @return {string} host:port of LDAP server
     */
    getHostAndPort() {
        return `localhost:${this.port}`;
    }

    /**
     * Get the port.
     *
     * @return {int} port of LDAP server
     */
    getPort() {
        return this.port;
    }

    /**
     * Get the PID.
     *
     * @return {int} PID of LDAP server
     */
    getPid() {
        return this.pid;
    }

    /**
     * Stop the LDAP server.
     */
    stop() {
        print("Shutting down LDAP server: " + this.getHostAndPort());
        stopMongoProgramByPid(this.pid);
        this.pid = -1;
    }

    /**
     * Restart the LDAP server.
     */
    restart() {
        print("Restarting LDAP server: " + this.getHostAndPort());
        this.stop();
        this.start();
    }

    /**
     * Check whether the LDAP server is currently running/associated with a live PID.
     */
    isRunning() {
        return this.pid > -1;
    }

    /**
     * Reset the LDAP server's delay. Requires a restart.
     */
    setDelay(delay) {
        if (delay !== this.delay) {
            this.delay = delay;
            this.restart();
        }
    }
}

/**
 * Creates an LDAP client that can be used to perform write operations against the specified LDAP
 * server.
 */
export class LDAPWriteClient {
    constructor(mockServerPort) {
        this.path = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapclient.py';
        this.mockServerPort = mockServerPort;

        this.python = getPython3Binary();
    }

    executeWriteOp(group, userDN, modifyAction) {
        const ldapWriteClientPid = startMongoProgramNoConnect(this.python,
                                                              "-u",
                                                              this.path,
                                                              "--targetPort",
                                                              this.mockServerPort,
                                                              "--group",
                                                              group,
                                                              "--user",
                                                              userDN,
                                                              "--modifyAction",
                                                              modifyAction);
        assert.eq(waitProgram(ldapWriteClientPid), 0);
    }
}

/**
 * Starts, manages, and stops a proxy that intercepts requests/responses to a backing LDAP server.
 *
 */
export class LDAPProxy {
    /**
     * Create a new LDAP proxy with the provided options.
     */
    constructor(targetHost, targetPort, delay) {
        this.proxyPath = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapproxy.py';
        this.proxyPort = allocatePort();
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.delay = delay;

        this.python = 'python3';
        if (_isWindows()) {
            this.python = 'python.exe';
        }
    }

    /**
     * Start the LDAP proxy.
     */
    start() {
        clearRawMongoProgramOutput();

        this.pid = startMongoProgramNoConnect(this.python,
                                              this.proxyPath,
                                              '--port',
                                              this.proxyPort,
                                              '--targetHost',
                                              this.targetHost,
                                              '--targetPort',
                                              this.targetPort,
                                              '--delay',
                                              this.delay);

        assert(checkProgram(this.pid));
        assert.soon(() => rawMongoProgramOutput(".*").search("ServerFactory starting on " +
                                                             this.proxyPort) !== -1);

        print("LDAP proxy started on port", this.proxyPort);
    }

    /**
     * Use the LDAP proxy to run a single-shot root DSE query against the LDAP server.
     */
    runTestClient() {
        const python = this.python;
        const proxyPath = this.proxyPath;
        const targetHost = this.targetHost;
        const targetPort = this.targetPort;
        assert.soon(function() {
            let exitCode = runNonMongoProgram(python,
                                              proxyPath,
                                              "--testClient",
                                              "--targetHost",
                                              targetHost,
                                              "--targetPort",
                                              targetPort);
            return exitCode == 0;
        });
    }

    /**
     * Get the host and port.
     *
     * @return {string} host:port of LDAP proxy
     */
    getHostAndPort() {
        return `localhost:${this.proxyPort}`;
    }

    /**
     * Get the port.
     *
     * @return {int} port of LDAP proxy
     */
    getPort() {
        return this.proxyPort;
    }

    /**
     * Get the PID.
     *
     * @return {int} PID of LDAP proxy
     */
    getPid() {
        return this.pid;
    }

    /**
     * Stop the LDAP proxy.
     */
    stop() {
        print("Shutting down the LDAP proxy");
        stopMongoProgramByPid(this.pid);
    }

    /**
     * Restart the LDAP proxy.
     */
    restart() {
        print("Restarting the LDAP proxy");
        this.stop();
        this.start();
    }

    /**
     * Reset the LDAP proxy's delay. Requires a restart.
     */
    setDelay(delay) {
        this.delay = delay;
        this.restart();
    }
}

// Instantiates the LDAP config generator with the common params needed for both mongod and mongos.
export function setupConfigGenerator(
    mockServerHostAndPort, authzManagerCacheSize = 100, shouldUseConnectionPool = true) {
    let ldapConfig = new LDAPTestConfigGenerator();
    ldapConfig.ldapServers = [mockServerHostAndPort];
    ldapConfig.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", substitution: "cn={0}," + defaultUserDNSuffix},
    ];
    ldapConfig.ldapAuthzQueryTemplate = "{USER}?memberOf";
    ldapConfig.authorizationManagerCacheSize = authzManagerCacheSize;
    ldapConfig.ldapUseConnectionPool = shouldUseConnectionPool;

    return ldapConfig;
}
