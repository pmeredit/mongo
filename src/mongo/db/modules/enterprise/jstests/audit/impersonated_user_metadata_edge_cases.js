// Check processing of ImpersonatedUserMetadata

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

import {ReplSetTest} from "jstests/libs/replsettest.js";

const kInvalidValueExpectedObject = 10065;

const kUserTestCases = [
    {test: 'string', pass: false},
    {test: {}, pass: false},
    {test: {user: 'foo'}, pass: false},
    {test: {db: 'bar'}, pass: false},
    {test: [{user: 'foo', db: 'bar'}], pass: false},
    {test: {user: 'foo', db: 'bar'}, pass: true},
];

const kRoleTestCases = [
    {test: [], pass: true},
    {test: ['string'], pass: false, code: kInvalidValueExpectedObject},
    {test: [{}], pass: false},
    {test: [{role: 'foo'}], pass: false},
    {test: [{db: 'bar'}], pass: false},
    {test: [{role: 'foo', db: 'bar'}], pass: true},
];

const kClientTestCases = [
    {test: undefined, pass: undefined},
    {test: [], pass: false, code: ErrorCodes.TypeMismatch},
    {test: 'string', pass: false, code: ErrorCodes.TypeMismatch},
    {test: {}, pass: false, code: ErrorCodes.IDLFailedToParse},
    {test: {hosts: []}, pass: false, code: ErrorCodes.TypeMismatch},
    {test: {hosts: [{}]}, pass: false, code: ErrorCodes.TypeMismatch},
    {
        test: {hosts: [{ip: '172.23.55.11', port: 23890}]},
        pass: false,
        code: ErrorCodes.TypeMismatch
    },
    {test: {hosts: ['aws_proxy.cc']}, pass: true},
    {test: {hosts: ['172.23.55.11:23890']}, pass: true},
    {
        test: {
            hosts:
                ['172.23.55.11:23890', 'aws_proxy.cc:14089', '[1050:0:0:0:5:600:300c:326b]:21021']
        },
        pass: true
    }
];

function setupTests(conn) {
    const admin = conn.getDB('admin');
    assert.commandWorked(admin.runCommand({createUser: 'admin', pwd: 'admin', roles: ['root']}));
}

function runTests(conn, authenticated, isMongos = false) {
    const msg = authenticated ? 'with' : 'without';
    jsTest.log('Running test ' + msg + ' authentication');

    const admin = conn.getDB('admin');
    const local = conn.getDB('local');
    if (authenticated) {
        local.auth('__system', 'foopdedoop');
    }

    // Run test cases with just $impersonatedClient and empty $impersonatedRoles first.
    kClientTestCases.forEach(function(client) {
        let icmd = {"$impersonatedRoles": []};
        if (client.test !== undefined) {
            icmd["$impersonatedClient"] = client.test;
        }

        const cmd = {hello: 1, "$audit": icmd};
        const pass = client.pass !== undefined ? client.pass : true;

        const expect = (pass && (isMongos || authenticated)) ? 'pass' : 'fail';
        jsTest.log("Command should " + expect + ": " + tojson(cmd));
        if (pass) {
            // Valid formations of $impersonatedClient on replica sets will fail without auth.
            if (!isMongos && !authenticated && client.test) {
                assert.commandFailedWithCode(admin.runCommand(cmd), ErrorCodes.Unauthorized);
            } else {
                assert.commandWorked(admin.runCommand(cmd));
            }
        } else if (client.code) {
            const expectedCodes = [ErrorCodes.BadValue];
            expectedCodes.push(client.code);
            assert.commandFailedWithCode(admin.runCommand(cmd), expectedCodes);
        } else {
            assert.commandFailed(admin.runCommand(cmd));
        }
    });

    kUserTestCases.forEach(function(user) {
        kRoleTestCases.forEach(function(role) {
            kClientTestCases.forEach(function(client) {
                let imd = {
                    "$impersonatedUser": user.test,
                    "$impersonatedRoles": role.test,
                };
                if (client.test !== undefined) {
                    imd["$impersonatedClient"] = client.test;
                }

                const cmd = {hello: 1, "$audit": imd};
                let pass = (user.pass) && (role.pass);
                if (client.pass !== undefined) {
                    pass = pass && (client.pass);
                }

                const expect = (pass && (isMongos || authenticated)) ? 'pass' : 'fail';
                jsTest.log("Command should " + expect + ": " + tojson(cmd));
                if (pass) {
                    // Tests expected to pass on replica sets will fail without auth.
                    if (!isMongos && !authenticated) {
                        assert.commandFailedWithCode(admin.runCommand(cmd),
                                                     ErrorCodes.Unauthorized);
                    } else {
                        assert.commandWorked(admin.runCommand(cmd));
                    }
                } else if (user.code || role.code || client.code) {
                    const expectedCodes = [ErrorCodes.BadValue];
                    if (user.code !== undefined) {
                        expectedCodes.push(user.code);
                    }
                    if (role.code !== undefined) {
                        expectedCodes.push(role.code);
                    }
                    if (client.code !== undefined) {
                        expectedCodes.push(client.code);
                    }
                    assert.commandFailedWithCode(admin.runCommand(cmd), expectedCodes);
                } else {
                    assert.commandFailed(admin.runCommand(cmd));
                }
            });
        });
    });

    if (authenticated) {
        local.logout();
    }
}

function runAuditingTests(conn, auditSpooler) {
    const local = conn.getDB('local');
    local.auth('__system', 'foopdedoop');

    const cmdWithMetadata = {
        createUser: "user1",
        pwd: "user",
        roles: [],
        "$audit": {
            '$impersonatedUser': {user: 'foo', db: 'bar'},
            '$impersonatedRoles': [{role: 'foo', db: 'bar'}],
            '$impersonatedClient': {hosts: ['172.23.55.11:23890']}
        },
    };
    const cmdWithoutMetadata = {
        createUser: "user1",
        pwd: "user",
        roles: [],
    };

    // Running a command with impersonation metadata should cause that metadata to appear in the
    // audit event.
    assert.commandWorked(local.adminCommand(cmdWithMetadata));
    let entry =
        auditSpooler.assertEntryRelaxed("createUser", {user: "user1", db: "admin", roles: []});
    jsTestLog('Entry: ' + tojson(entry));
    assert(entry.hasOwnProperty('remote'));
    assert.eq(Object.keys(entry.remote).length, 2);
    assert.docEq({ip: '172.23.55.11', port: 23890}, entry.remote);
    assert(entry.hasOwnProperty('users'));
    assert(entry.hasOwnProperty('roles'));
    assert.eq(entry.users.length, 1);
    assert.docEq({user: 'foo', db: 'bar'}, entry.users[0]);
    assert.eq(entry.roles.length, 1);
    assert.docEq({role: 'foo', db: 'bar'}, entry.roles[0]);

    // Run the command without impersonation metadata. The audit event should not have that client
    // and user information anymore.
    assert.commandFailed(local.adminCommand(cmdWithoutMetadata));
    entry = auditSpooler.assertEntryRelaxed("createUser", {user: "user1", db: "admin", roles: []});
    jsTestLog('Entry: ' + tojson(entry));
    assert(entry.hasOwnProperty('remote'));
    assert.eq(Object.keys(entry.remote).length, 2);
    assert.neq(entry.remote.ip, '172.23.55.11');
    assert(entry.hasOwnProperty('users'));
    assert(entry.hasOwnProperty('roles'));
    assert.eq(entry.users.length, 1);
    assert.docEq({user: '__system', db: 'local'}, entry.users[0]);
    assert.eq(entry.roles.length, 0);

    local.logout();
}

const kKeyFile = 'jstests/libs/key1';
{
    jsTest.log('Standalone');
    const mongod = MongoRunner.runMongodAuditLogger({auth: '', keyFile: kKeyFile});
    setupTests(mongod);
    runTests(mongod, false);
    runTests(mongod, true);

    const auditSpool = mongod.auditSpooler();
    runAuditingTests(mongod, auditSpool);

    MongoRunner.stopMongod(mongod);
}

{
    jsTest.log('Replication');
    const rst =
        ReplSetTest.runReplSetAuditLogger({nodes: 2, nodeOptions: {auth: ""}, keyFile: kKeyFile});
    rst.awaitSecondaryNodes();

    const primary = rst.getPrimary();
    setupTests(primary);
    runTests(primary, false);
    runTests(primary, true);

    const auditSpool = primary.auditSpooler();
    runAuditingTests(primary, auditSpool);
    rst.stopSet();
}

{
    jsTest.log('Sharding');
    const st = MongoRunner.runShardedClusterAuditLogger({keyFile: kKeyFile}, {auth: null});
    setupTests(st.s0);
    runTests(st.s0, false, true);
    runTests(st.s0, true, true);

    const auditSpool = st.configRS.nodes[0].auditSpooler();
    runAuditingTests(st.configRS.nodes[0], auditSpool);

    st.stop();
}
