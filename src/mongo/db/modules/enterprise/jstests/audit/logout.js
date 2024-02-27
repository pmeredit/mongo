// Verify logout events are sent to audit log.
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kExplicitLogoutMessage = "Logging out on user request";
const kImplicitLogoutMessage = "Client has disconnected";
function parseUser(username) {
    assert(typeof (username) === 'string', "Unsupported value for username: " + tojson(username));
    const parts = username.split('.');
    assert.eq(parts.length, 2, "Invalid number of parts to username: " + tojson(username));
    return {db: parts[0], user: parts[1]};
}

function assertAuthenticate(spooler, username = undefined, mechanism = undefined) {
    const params = username ? parseUser(username) : {};

    if (mechanism !== undefined) {
        assert(typeof (mechanism) === 'string',
               "Unsupported value for mechanism: " + tojson(mechanism));
        params.mechanism = mechanism;
    }

    return spooler.assertEntryRelaxed('authenticate', params);
}

function assertLogout(spooler, username = undefined, reason = undefined) {
    const params = {updatedUsers: []};
    if (reason !== undefined) {
        assert(typeof (reason) === 'string',
               "Unsupported value for reason param: " + tojson(reason));
        params.reason = reason;
    }

    if (username !== undefined) {
        params.initialUsers = [parseUser(username)];
    }

    return spooler.assertEntryRelaxed('logout', params);
}

let runTest = function(conn) {
    const port = conn.port;
    const audit = conn.auditSpooler();
    const admin = conn.getDB("admin");
    const test = conn.getDB("test");

    audit.fastForward();
    // Create users on db test1 and test2 as admin.
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));

    assert(admin.auth("admin", "pwd"));
    assert.commandWorked(test.runCommand({createUser: "explicitUser", pwd: "pwd", roles: []}));
    assert.commandWorked(test.runCommand({createUser: "implicitUser", pwd: "pwd", roles: []}));
    assert.commandWorked(admin.logout());

    // Check that explicit admin logout was recorded in audit log.
    assertLogout(audit, "admin.admin", kExplicitLogoutMessage);
    const kTimeDiffBetweenAuthAndLogoutMS = 15 * 1000;
    const kAcceptableRangeMS = 5 * 1000;
    const kIntervalTimeMS = 2 * 1000;

    // Explicit auth/logout test.
    {
        print('START explicit logout');
        assert(test.auth({user: "explicitUser", pwd: "pwd"}));
        const authEvent = assertAuthenticate(audit, "test.explicitUser");
        jsTest.log(authEvent);
        sleep(kTimeDiffBetweenAuthAndLogoutMS);
        assert(test.logout());
        const logoutEvent = assertLogout(audit, "test.explicitUser", kExplicitLogoutMessage);
        jsTest.log(logoutEvent);
        assert.eq(authEvent.uuid["$binary"], logoutEvent.uuid["$binary"]);

        assert(logoutEvent.param.loginTime !== undefined, "Missing loginTime in logout record");

        const authTime = Date.parse(authEvent.ts["$date"]);
        const loginTime = Date.parse(logoutEvent.param.loginTime["$date"]);
        const logoutTime = Date.parse(logoutEvent.ts["$date"]);

        assert.lt(Math.abs(loginTime - authTime), kAcceptableRangeMS);
        assert.lt(logoutTime - loginTime - kTimeDiffBetweenAuthAndLogoutMS, kAcceptableRangeMS);
        print('SUCCESS explicit logout');
    }

    audit.fastForward();
    // Spawn a separate mongo client to login as user and then quit the shell
    {
        print('START implicit logout');
        const uri = `mongodb://implicitUser:pwd@${conn.host}/test`;
        runMongoProgram('mongo', uri, '--eval', ';');
        const authEvent = assertAuthenticate(audit, "test.implicitUser");
        jsTest.log(authEvent);
        const logoutEvent = assertLogout(audit, "test.implicitUser", kImplicitLogoutMessage);
        jsTest.log(logoutEvent);
        assert.eq(authEvent.uuid["$binary"], logoutEvent.uuid["$binary"]);

        assert(logoutEvent.param.loginTime !== undefined, "Missing loginTime in logout record");
        const authTime = Date.parse(authEvent.ts["$date"]);
        const loginTime = Date.parse(logoutEvent.param.loginTime["$date"]);
        const logoutTime = Date.parse(logoutEvent.ts["$date"]);

        assert.lt(Math.abs(loginTime - authTime), kAcceptableRangeMS);
        print('SUCCESS implicit logout');
    }
};

{
    print('START audit-logout.js for standalone');

    const m = MongoRunner.runMongodAuditLogger({auth: ''});
    runTest(m);

    MongoRunner.stopMongod(m);

    print('SUCCESS implicit logout for standalone');
}

{
    print('START audit-logout.js for Sharded Cluster');

    const st = MongoRunner.runShardedClusterAuditLogger({}, {auth: null});
    const mongos = st.s0;
    runTest(mongos);

    st.stop();
    print('SUCCESS implicit logout for standalone');
}

print("SUCCESS audit-logout.js");
