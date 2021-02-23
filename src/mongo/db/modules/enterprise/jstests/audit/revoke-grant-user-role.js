// This test verifies two sets of behavior:
// - Audit events are emitted for grantRoleToUser and revokeRoleFromUser commands.
// - Audit events for atype authCheck have relevant role and user information.

(function() {
load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const m =
    MongoRunner.runMongodAuditLogger({auth: '', setParameter: {auditAuthorizationSuccess: true}});
const audit = m.auditSpooler();

function checkForUserRoles({param, users, roles}) {
    assert.soon(() => {
        const log = audit.getAllLines().slice(audit.getCurrentAuditLine());
        const line = audit.getNextEntry();
        if (line.atype !== 'authCheck') {
            return false;
        }

        if (!audit._deepPartialEquals(line.param, param)) {
            return false;
        }

        if (line.users.length != users.length || !audit._deepPartialEquals(line.users, users)) {
            return false;
        }

        if (line.roles.length != roles.length || !audit._deepPartialEquals(line.roles, roles)) {
            return false;
        }

        return true;
    }, 'Unable to find a matching authCheck audit entry');
}

function setUp() {
    const admin = m.getDB('admin');
    admin.createUser({user: 'admin', pwd: 'pwd', roles: ['root']});
    assert(admin.auth('admin', 'pwd'));

    admin.createUser({user: 'user1', pwd: 'pwd', roles: [{role: 'read', db: 'test'}]});
    assert.commandWorked(m.getDB('test').test.insert({_id: 1}));

    audit.assertEntry("createUser",
                      {user: "user1", db: "admin", roles: [{role: 'read', db: 'test'}]});
}

function testRevokeGrant() {
    const admin = m.getDB('admin');
    assert(admin.auth('admin', 'pwd'));

    const reader = new Mongo(m.host);
    const db = reader.getDB('test');
    assert(db.getSiblingDB('admin').auth('user1', 'pwd'));

    jsTestLog('Performing smoke read');
    assert.commandWorked(db.runCommand({find: "test"}),
                         'Read should succeed because we have the role');
    checkForUserRoles({
        users: [{user: "user1", db: "admin"}],
        roles: [{role: 'read', db: 'test'}],
        param: {command: "find", ns: "test.test"}
    });

    jsTestLog('Revoking role from user');
    assert.commandWorked(
        admin.runCommand({revokeRolesFromUser: "user1", roles: [{role: "read", db: 'test'}]}));
    audit.assertEntry("revokeRolesFromUser",
                      {user: "user1", db: "admin", roles: [{role: 'read', db: 'test'}]});
    assert.commandFailedWithCode(db.runCommand({find: "test"}),
                                 ErrorCodes.Unauthorized,
                                 'Read should fail because we lack the role');
    checkForUserRoles({
        users: [{user: "user1", db: "admin"}],
        roles: [],
        param: {command: "find", ns: "test.test"}
    });

    jsTestLog('Granting role from user');
    assert.commandWorked(
        admin.runCommand({grantRolesToUser: "user1", roles: [{role: "read", db: 'test'}]}));
    audit.assertEntry("grantRolesToUser",
                      {user: "user1", db: "admin", roles: [{role: 'read', db: 'test'}]});
    assert.commandWorked(db.runCommand({find: "test"}),
                         'Read should succeed because we have the role again');
    checkForUserRoles({
        users: [{user: "user1", db: "admin"}],
        roles: [{role: 'read', db: 'test'}],
        param: {command: "find", ns: "test.test"}
    });
}

setUp();
testRevokeGrant();

MongoRunner.stopMongod(m);
})();
