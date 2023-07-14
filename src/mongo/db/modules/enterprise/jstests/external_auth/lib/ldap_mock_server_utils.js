/**
 * Starts, manages, and stops a mock LDAP server.
 */
class MockLDAPServer {
    /**
     * Create a new LDAP server. If referral_uri is specified, then all search requests will return
     * a referral to referral_uri. Liveness checks (which involves a rootDSE search) and binds will
     * be performed locally without a referral.
     */
    constructor(referralUri) {
        this.path = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapmockserver.py';
        this.port = allocatePort();
        this.referralUri = referralUri;

        this.python = 'python3';
        if (_isWindows()) {
            this.python = 'python.exe';
        }
    }

    /**
     * Start the LDAP server.
     */
    start() {
        clearRawMongoProgramOutput();

        if (this.referralUri) {
            this.pid = startMongoProgramNoConnect(this.python,
                                                  "-u",
                                                  this.path,
                                                  "--port",
                                                  this.port,
                                                  "--referral-uri",
                                                  this.referralUri);
        } else {
            this.pid =
                startMongoProgramNoConnect(this.python, "-u", this.path, "--port", this.port);
        }

        assert(checkProgram(this.pid));
        assert.soon(() => rawMongoProgramOutput().search("LDAPServerFactory starting on " +
                                                         this.port) !== -1);

        print("Mock LDAP server started on port", this.port);
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
     * Stop the LDAP server
     */
    stop() {
        print("Shutting down LDAP server");
        stopMongoProgramByPid(this.pid);
    }
}

/**
 * Creates an LDAP client that can be used to perform write operations against the specified LDAP
 * server.
 */
class LDAPWriteClient {
    constructor(mockServerPort) {
        this.path = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapclient.py';
        this.mockServerPort = mockServerPort;

        this.python = 'python3';
        if (_isWindows()) {
            this.python = 'python.exe';
        }
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
class LDAPProxy {
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
        assert.soon(() => rawMongoProgramOutput().search("ServerFactory starting on " +
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
function setupConfigGenerator(
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
