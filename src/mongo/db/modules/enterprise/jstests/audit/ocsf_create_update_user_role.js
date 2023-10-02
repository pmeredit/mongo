// Verify {create,update}{User,Role} is sent to audit log
// @tags: [ featureFlagOCSF ]

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kAccountManagementCategory = 3;
const kAccountManagementClass = 3001;

const kActivityCreate = 1;
const kActivityPasswordChange = 3;
const kActivityDelete = 6;
const kActivityAttachPolicy = 7;
const kActivityDetachPolicy = 8;
const kActivityOther = 99;

// Normalize presentation of privileges by sorting actions first,
// then lexically sorting the each privilege as a whole.
// Depends on Privilege serialization consistency.
function sortPrivs(privs) {
    const sorted = privs
                       .map(function(p) {
                           p.actions.sort();
                           return p;
                       })
                       .sort((a, b) => tojson(a).localeCompare(tojson(b)));
}

// Get a specific entry from the spooler ignoring directAuthMutations
function getEntry(spooler, category, cls, activity) {
    while (true) {
        const entry = spooler.assertEntry(category, cls, activity);
        if (!entry.unmapped || !entry.unmapped.directAuthMutation) {
            return entry;
        }
    }
}

function assertUserEntry(spooler, activity, username, groups) {
    const entry = getEntry(spooler, kAccountManagementCategory, kAccountManagementClass, activity);

    try {
        const uid = (kAccountManagementClass * 100) + activity;
        assert.eq(entry.type_uid, uid);

        if (username) {
            assert(entry.user !== undefined, "Missing expected 'user' property");
            assert.eq(entry.user.name, username, "Unexpected username");
        } else {
            assert(entry.user === undefined, "Got unexpected 'user' property");
        }

        if (groups !== undefined) {
            assert(entry.user.groups !== undefined, "Missing expected 'user.groups' property");
            const gotGroups = entry.user.groups.map((g) => g.name).sort().join(',');
            assert.eq(groups.sort().join(','), gotGroups, "Mismatched group names");
        } else {
            assert((entry.user || {}).groups === undefined, "Unexpected 'user.groups' property");
        }

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertRoleEntry(spooler, activity, rolename, groups, privileges, authRestrictions) {
    const entry = getEntry(spooler, kAccountManagementCategory, kAccountManagementClass, activity);

    try {
        const uid = (kAccountManagementClass * 100) + activity;
        assert.eq(entry.type_uid, uid);

        assert(entry.unmapped !== undefined, "Missing expected 'unmapped' property");
        const unmapped = entry.unmapped;

        assert.eq(unmapped.role, rolename, "Unexpected rolename");

        if (groups !== undefined) {
            assert(unmapped.roles !== undefined, "Missing expected 'unmapped.roles' property");
            const gotGroups = unmapped.roles.sort().join(',');
            assert.eq(groups.sort().join(','), gotGroups, "Mismatched group names");
        } else {
            assert(unmapped.roles === undefined, "Unexpected 'unmapped.roles' property");
        }

        if (privileges !== undefined) {
            assert(unmapped.privileges !== undefined,
                   "Missing expected 'unmapped.privileges' property");
            assert.eq(tojson(sortPrivs(privileges)),
                      tojson(sortPrivs(unmapped.privileges)),
                      "Mismatched privileges");
        } else {
            assert(unmapped.privileges === undefined, "Unexpected 'unmapped.privileges' property");
        }

        if (authRestrictions !== undefined) {
            assert(unmapped.authenticationRestrictions !== undefined,
                   "Missing expected 'unmapped.authenticationRestrictions' property");
            const expect = authRestrictions.sort((a, b) => tojson(a).localeCompare(tojson(b)));
            const rests = unmapped.authenticationRestrictions.sort(
                (a, b) => tojson(a).localeCompare(tojson(b)));
            assert.eq(tojson(expect), tojson(rests), "Mismatched authentication restrictions");
        } else {
            assert(unmapped.authenticationRestrictions === undefined,
                   "Unexpected 'unmapped.authenticationRestrictions' property");
        }

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertPolicyChange(spooler, activity, action) {
    const grant = activity == kActivityAttachPolicy;
    assert(grant || (activity == kActivityDetachPolicy));
    const entry = getEntry(spooler, kAccountManagementCategory, kAccountManagementClass, activity);

    try {
        const uid = (kAccountManagementClass * 100) + activity;
        assert.eq(entry.type_uid, uid);

        assert(entry.unmapped !== undefined, "Missing 'unmapped' property");
        assert.eq(action, entry.unmapped.action, "Mismatched 'unmapped.action' property");

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertRolePolicyChange(spooler, activity, role, roles) {
    const action = (activity == kActivityAttachPolicy) ? 'grantRolesToRole' : 'revokeRolesFromRole';
    const entry = assertPolicyChange(spooler, activity, action);

    try {
        assert(entry.user === undefined, "Unexpected 'user' property");
        const unmapped = entry.unmapped;

        assert.eq(unmapped.role, role, "Mismatched 'unmapped.role' property");
        assert(Array.isArray(unmapped.roles),
               "Missing or wrong type for 'unmapped.roles' property");
        const expect = roles.sort().join(',');
        const received = unmapped.roles.sort().join(',');
        assert.eq(expect, received, "Mismatched 'unmapped.roles' property");

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertUserPolicyChange(spooler, activity, user, roles) {
    const action = (activity == kActivityAttachPolicy) ? 'grantRolesToUser' : 'revokeRolesFromUser';
    const entry = assertPolicyChange(spooler, activity, action);

    try {
        assert(entry.user !== undefined, "Missing 'user' property");
        assert.eq(entry.user.name, user, "Mismatched 'user.name' property");
        const unmapped = entry.unmapped;

        assert(unmapped.role === undefined, "Unexpected 'unmapped.role' property");
        assert(Array.isArray(unmapped.roles),
               "Missing or wrong type for 'unmapped.roles' property");
        const expect = roles.sort().join(',');
        const received = unmapped.roles.sort().join(',');
        assert.eq(expect, received, "Mismatched 'unmapped.roles' property");

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertPrivilegePolicyChange(spooler, activity, role, privileges) {
    const action =
        (activity == kActivityAttachPolicy) ? 'grantPrivilegesToRole' : 'revokePrivilegesFromRole';
    const entry = assertPolicyChange(spooler, activity, action);

    try {
        assert(entry.user === undefined, "Unexpected 'user' property");
        const unmapped = entry.unmapped;

        assert(unmapped.roles === undefined, "Unexpected 'unmapped.roles' property");
        assert(unmapped.privileges !== undefined,
               "Missing expected 'unmapped.privileges' property");
        assert.eq(tojson(sortPrivs(privileges)),
                  tojson(sortPrivs(unmapped.privileges)),
                  "Mismatched privileges");

        return entry;
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function assertDeleteEntry(spooler, unmapped) {
    const entry =
        getEntry(spooler, kAccountManagementCategory, kAccountManagementClass, kActivityDelete);

    try {
        const uid = (kAccountManagementClass * 100) + kActivityDelete;
        assert.eq(entry.type_uid, uid);
        assert.eq(tojson(entry.unmapped), tojson(unmapped), "Mismatched 'unmapped' property");
    } catch (e) {
        jsTest.log(entry);
        throw e;
    }
}

function testCreateUser(conn, admin, spooler) {
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({createUser: 'user1', pwd: "user", roles: []}));
    const createUser1 = assertUserEntry(spooler, kActivityCreate, 'admin.user1', []);
    assert.eq(createUser1.unmapped.authenticationRestrictions.length, 0);
    assert.eq(createUser1.unmapped.customData, undefined);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand(
        {createUser: 'user2', pwd: "user", roles: [{role: 'readWrite', db: 'admin'}]}));
    const createUser2 =
        assertUserEntry(spooler, kActivityCreate, 'admin.user2', ['admin.readWrite']);
    assert.eq(createUser2.unmapped.authenticationRestrictions.length, 0);
    assert.eq(createUser2.unmapped.customData, undefined);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({
        createUser: 'user3',
        pwd: "user",
        roles: [],
        customData: {hello: 'world'},
        authenticationRestrictions: []
    }));
    const createUser3 = assertUserEntry(spooler, kActivityCreate, 'admin.user3', []);
    assert.eq(createUser3.unmapped.authenticationRestrictions.length, 0);
    assert.eq(createUser3.unmapped.customData.hello, 'world');

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({
        createUser: 'user4',
        pwd: "user",
        roles: [],
        authenticationRestrictions: [{clientSource: ['::1/128']}]
    }));
    const createUser4 = assertUserEntry(spooler, kActivityCreate, 'admin.user4', []);
    assert.eq(createUser4.unmapped.authenticationRestrictions.length, 1);
    assert.eq(createUser4.unmapped.authenticationRestrictions[0].clientSource, '::1/128');
    assert.eq(createUser4.unmapped.customData, undefined);
}

function testUpdateUser(conn, admin, spooler) {
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({updateUser: 'user1', pwd: "secret"}));
    const update1User1 =
        assertUserEntry(spooler, kActivityPasswordChange, 'admin.user1', undefined);

    spooler.fastForward();
    assert.commandWorked(
        admin.runCommand({updateUser: 'user1', roles: [{role: 'readWrite', db: 'admin'}]}));
    const update2User1 =
        assertUserEntry(spooler, kActivityOther, 'admin.user1', ['admin.readWrite']);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({updateUser: 'user1', roles: []}));
    const update3User1 = assertUserEntry(spooler, kActivityOther, 'admin.user1', []);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand(
        {updateUser: 'user1', authenticationRestrictions: [{serverAddress: ['::1/128']}]}));
    const update4User1 = assertUserEntry(spooler, kActivityOther, 'admin.user1', undefined);
    assert.eq(update4User1.unmapped.authenticationRestrictions.length, 1);
    assert.eq(update4User1.unmapped.authenticationRestrictions[0].serverAddress, '::1/128');

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({updateUser: 'user1', authenticationRestrictions: []}));
    const update5User1 = assertUserEntry(spooler, kActivityOther, 'admin.user1', undefined);
    assert.eq(update5User1.unmapped.authenticationRestrictions.length, 0);
}

function testCreateRole(conn, admin, spooler) {
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({createRole: 'role1', roles: [], privileges: []}));
    assertRoleEntry(spooler, kActivityCreate, 'admin.role1', [], []);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand(
        {createRole: 'role2', roles: [{role: 'role1', db: 'admin'}], privileges: []}));
    assertRoleEntry(spooler, kActivityCreate, 'admin.role2', ['admin.role1'], []);

    spooler.fastForward();
    const role3Privs = [{resource: {db: 'admin', collection: 'foo'}, actions: ['find']}];
    assert.commandWorked(
        admin.runCommand({createRole: 'role3', roles: [], privileges: role3Privs}));
    assertRoleEntry(spooler, kActivityCreate, 'admin.role3', [], role3Privs);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand(
        {createRole: 'role4', roles: [], privileges: [], authenticationRestrictions: []}));
    assertRoleEntry(spooler, kActivityCreate, 'admin.role4', [], []);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({
        createRole: 'role5',
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ['::1/128']}]
    }));
    assertRoleEntry(spooler, kActivityCreate, 'admin.role5', [], [], [{clientSource: ['::1/128']}]);
}

function testUpdateRole(conn, admin, spooler) {
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({updateRole: 'role1', roles: []}));
    assertRoleEntry(spooler, kActivityOther, 'admin.role1', [], undefined);

    spooler.fastForward();
    assert.commandWorked(
        admin.runCommand({updateRole: 'role3', roles: [{role: 'role1', db: 'admin'}]}));
    assertRoleEntry(spooler, kActivityOther, 'admin.role3', ['admin.role1'], undefined);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({updateRole: 'role1', privileges: []}));
    assertRoleEntry(spooler, kActivityOther, 'admin.role1', undefined, []);

    spooler.fastForward();
    const role1Update1Privs = [{resource: {db: 'admin', collection: 'foo'}, actions: ['find']}];
    assert.commandWorked(admin.runCommand({updateRole: 'role1', privileges: role1Update1Privs}));
    assertRoleEntry(spooler, kActivityOther, 'admin.role1', undefined, role1Update1Privs);

    spooler.fastForward();
    const role1Update2Rests = [{clientSource: ['::1/128']}];
    assert.commandWorked(
        admin.runCommand({updateRole: 'role1', authenticationRestrictions: role1Update2Rests}));
    assertRoleEntry(
        spooler, kActivityOther, 'admin.role1', undefined, undefined, role1Update2Rests);
}

function testGrantRevoke(conn, admin, spooler) {
    // RolesToFromRole
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({grantRolesToRole: 'role3', roles: ['role5']}));
    assertRolePolicyChange(spooler, kActivityAttachPolicy, 'admin.role3', ['admin.role5']);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({revokeRolesFromRole: 'role3', roles: ['role5']}));
    assertRolePolicyChange(spooler, kActivityDetachPolicy, 'admin.role3', ['admin.role5']);

    // RolesToFromUser
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({grantRolesToUser: 'user3', roles: ['role4']}));
    assertUserPolicyChange(spooler, kActivityAttachPolicy, 'admin.user3', ['admin.role4']);

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({revokeRolesFromUser: 'user3', roles: ['role4']}));
    assertUserPolicyChange(spooler, kActivityDetachPolicy, 'admin.user3', ['admin.role4']);

    // PrivilegesToFromRole
    const grantRevokePrivilege = {resource: {db: 'admin', collection: "bar"}, actions: ['find']};
    spooler.fastForward();
    assert.commandWorked(
        admin.runCommand({grantPrivilegesToRole: 'role1', privileges: [grantRevokePrivilege]}));
    assertPrivilegePolicyChange(
        spooler, kActivityAttachPolicy, 'admin.role1', [grantRevokePrivilege]);

    spooler.fastForward();
    assert.commandWorked(
        admin.runCommand({revokePrivilegesFromRole: 'role1', privileges: [grantRevokePrivilege]}));
    assertPrivilegePolicyChange(
        spooler, kActivityDetachPolicy, 'admin.role1', [grantRevokePrivilege]);
}

function testDropRole(conn, admin, spooler) {
    spooler.fastForward();
    assert.commandWorked(admin.runCommand({dropRole: 'role1'}));
    assertDeleteEntry(spooler, {role: 'admin.role1'});

    spooler.fastForward();
    assert.commandWorked(admin.runCommand({dropAllRolesFromDatabase: 1}));
    assertDeleteEntry(spooler, {db: 'admin'});
}

function runTest(conn, admin, spooler) {
    assert.commandWorked(admin.runCommand({createUser: 'admin', pwd: "pwd", roles: ['root']}));
    assert(admin.auth('admin', 'pwd'));
    spooler.fastForward();

    testCreateUser(conn, admin, spooler);
    testUpdateUser(conn, admin, spooler);
    testCreateRole(conn, admin, spooler);
    testUpdateRole(conn, admin, spooler);
    testGrantRevoke(conn, admin, spooler);
    testDropRole(conn, admin, spooler);
}

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF account management output on standalone");

    const {conn, audit, admin} = standaloneFixture.startProcess({}, 'JSON', 'ocsf');
    runTest(conn, admin, audit);
    standaloneFixture.stopProcess();
}
