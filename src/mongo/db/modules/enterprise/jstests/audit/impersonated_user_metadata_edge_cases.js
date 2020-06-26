// Check processing of ImpersonatedUserMetadata

(function() {
'use strict';

const kInvalidValueExpectedObject = 10065;

const kUserTestCases = [
    {test: [], pass: true},
    {test: ['string'], pass: false},
    {test: [{}], pass: false},
    {test: [{user: 'foo'}], pass: false},
    {test: [{db: 'bar'}], pass: false},
    {
        test: [{user: 'foo', db: 'bar'}],
        pass: false,
        code: ErrorCodes.Unauthorized,
        passOnMongos: true
    },
    {test: [{user: 'foo', db: 'bar', unknown: 'baz'}], pass: false, passOnMongos: true},
];

const kRoleTestCases = [
    {test: [], pass: true},
    {test: ['string'], pass: false, code: kInvalidValueExpectedObject},
    {test: [{}], pass: false},
    {test: [{role: 'foo'}], pass: false},
    {test: [{db: 'bar'}], pass: false},
    {
        test: [{role: 'foo', db: 'bar'}],
        pass: false,
        code: ErrorCodes.Unauthorized,
        passOnMongos: true
    },
    {test: [{role: 'foo', db: 'bar', unknown: 'baz'}], pass: false, passOnMongos: true},
];

function setupTests(conn) {
    const admin = conn.getDB('admin');
    assert.commandWorked(admin.runCommand({createUser: 'admin', pwd: 'admin', roles: ['root']}));
}

function runTests(conn, authenticated, isMongos = false) {
    const msg = authenticated ? 'with' : 'without';
    jsTest.log('Running test ' + msg + ' authentication');

    const admin = conn.getDB('admin');
    if (authenticated) {
        admin.auth('admin', 'admin');
    }

    kUserTestCases.forEach(function(user) {
        kRoleTestCases.forEach(function(role) {
            const iumd = {"$impersonatedUsers": user.test, "$impersonatedRoles": role.test};
            const cmd = {isMaster: 1, "$audit": iumd};

            const pass = (user.pass || (isMongos && user.passOnMongos)) &&
                (role.pass || (isMongos && role.passOnMongos));

            const expect = pass ? 'pass' : 'fail';
            jsTest.log("Command should " + expect + ": " + tojson(cmd));
            if (pass) {
                assert.commandWorked(admin.runCommand(cmd));
            } else if (user.code || role.code) {
                const expectedCodes = [ErrorCodes.BadValue];
                if (user.code !== undefined) {
                    expectedCodes.push(user.code);
                }
                if (role.code !== undefined) {
                    expectedCodes.push(role.code);
                }
                assert.commandFailedWithCode(admin.runCommand(cmd), expectedCodes);
            } else {
                assert.commandFailed(admin.runCommand(cmd));
            }
        });
    });

    if (authenticated) {
        admin.logout();
    }
}

{
    jsTest.log('Standalone');
    const mongod = MongoRunner.runMongod({auth: ''});
    setupTests(mongod);
    runTests(mongod, false);
    runTests(mongod, true);
    MongoRunner.stopMongod(mongod);
}

const kKeyFile = 'jstests/libs/key1';
{
    jsTest.log('Replication');
    const rst = new ReplSetTest({nodes: 2, nodeOptions: {auth: ""}, keyFile: kKeyFile});
    rst.startSet();
    rst.initiate();
    rst.awaitSecondaryNodes();

    const primary = rst.getPrimary();
    setupTests(primary);
    runTests(primary, false);
    runTests(primary, true);
    rst.stopSet();
}

{
    jsTest.log('Sharding');
    const st = new ShardingTest({
        mongos: 1,
        config: 1,
        shard: 2,
        keyFile: kKeyFile,
        other:
            {mongosOptions: {auth: null}, configOptions: {auth: null}, shardOptions: {auth: null}}
    });
    setupTests(st.s0);
    runTests(st.s0, false, true);
    runTests(st.s0, true, true);
    st.stop();
}
})();
