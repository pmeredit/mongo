// Validate that Mongo can authenticate against mongod with kerberos
(function() {
    'use strict';

    if (!_isWindows()) {
        return;
    }

    const BASE_PATH = "src/mongo/db/modules/enterprise/jstests/external_auth/lib";
    const WINDOWS_LIB = BASE_PATH + "/windows_lib.js";

    // The users are builtin to the Windows-2016 image and defined here
    // https://github.com/10gen/buildhost-configuration/blob/master/roles/domaincontroller/tasks/main.yml
    const TEST_USER = 'ldaptest1';
    const TEST_PASSWORD = 'MongoDB40!';
    const TEST_DOMAIN = 'SERVER.LDAPTEST.10GEN.CC';
    const TEST_HOST_NAME = hostname();

    // Register SPN, ignore duplicates
    run('setspn.exe', '-A', `mongodb/${TEST_HOST_NAME}.${TEST_DOMAIN}`, 'Administrator');

    let options = {
        setParameter: "authenticationMechanisms=GSSAPI",
        bind_ip_all: "",
        auth: "",
        verbose: 1,
    };

    let m = MongoRunner.runMongod(options);

    let externalDB = m.getDB("$external");

    // Create user
    externalDB.createUser({
        user: `${TEST_USER}@${TEST_DOMAIN}`,
        roles: [
            {role: "root", db: "admin"},
            {role: "userAdminAnyDatabase", db: "admin"},
            {role: "clusterAdmin", db: "admin"}
        ]
    });

    // Lots of escaping to deal with powershell and cmd
    const setupCmd = `load(\\\\"${WINDOWS_LIB}\\\\");`;

    let retVal = run(
        'powershell.exe',
        '-nologo',
        `${BASE_PATH}/runas.ps1`,
        '-UserName',
        TEST_USER,
        '-Password',
        TEST_PASSWORD,
        '-CommandString',
        `\'mongo.exe --host ${TEST_HOST_NAME}.${TEST_DOMAIN} --authenticationMechanism=GSSAPI\
         --authenticationDatabase=$external --username ${TEST_USER}@${TEST_DOMAIN}\
          --port ${m.port} --eval ` +
            setupCmd + `verifyAuth(db,\\\\"${TEST_USER}@${TEST_DOMAIN}\\\\");\'`);

    assert.eq(retVal, 0);

    MongoRunner.stopMongod(m);

})();