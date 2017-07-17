// Verify {create,update}{User,Role} is sent to audit log

(function() {
    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-create-update-user-role.js");

    m = MongoRunner.runMongodAuditLogger({});
    audit = m.auditSpooler();
    db = m.getDB("test");

    // CreateUser commands
    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "user", roles: []}));
    audit.assertEntry("createUser", {user: "user1", db: "test", roles: []});

    assert.commandWorked(db.runCommand(
        {createUser: "user2", pwd: "user", roles: [{role: "readWrite", db: "admin"}]}));
    audit.assertEntry("createUser",
                      {user: "user2", db: "test", roles: [{role: "readWrite", db: "admin"}]});

    assert.commandWorked(db.runCommand(
        {createUser: "user3", pwd: "user", roles: [], authenticationRestrictions: []}));
    audit.assertEntry("createUser", {user: "user3", db: "test", roles: []});

    assert.commandWorked(db.runCommand({
        createUser: "user4",
        pwd: "user",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));
    audit.assertEntry("createUser", {
        user: "user4",
        db: "test",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // UpdateUser commands
    assert.commandWorked(db.runCommand({updateUser: "user1", pwd: "secret"}));
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: true});

    assert.commandWorked(
        db.runCommand({updateUser: "user1", roles: [{role: "readWrite", db: "admin"}]}));
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        roles: [{role: "readWrite", db: "admin"}]
    });

    assert.commandWorked(db.runCommand(
        {updateUser: "user1", authenticationRestrictions: [{serverAddress: ["::1/128"]}]}));
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        authenticationRestrictions: [{serverAddress: ["::1/128"]}]
    });

    assert.commandWorked(db.runCommand({updateUser: "user1", authenticationRestrictions: []}));
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: false});

    // CreateRole commands
    assert.commandWorked(db.runCommand({createRole: "role1", roles: [], privileges: []}));
    audit.assertEntry("createRole", {role: "role1", db: "test", roles: [], privileges: []});

    assert.commandWorked(
        db.runCommand({createRole: "role2", roles: [{role: "role1", db: "test"}], privileges: []}));
    audit.assertEntry(
        "createRole",
        {role: "role2", db: "test", roles: [{role: "role1", db: "test"}], privileges: []});

    assert.commandWorked(db.runCommand({
        createRole: "role3",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    audit.assertEntry("createRole", {
        role: "role3",
        db: "test",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });

    assert.commandWorked(db.runCommand(
        {createRole: "role4", roles: [], privileges: [], authenticationRestrictions: []}));
    audit.assertEntry("createRole", {role: "role4", db: "test", roles: [], privileges: []});

    assert.commandWorked(db.runCommand({
        createRole: "role5",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));
    audit.assertEntry("createRole", {
        role: "role5",
        db: "test",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // UpdateRole command
    assert.commandWorked(db.runCommand({updateRole: "role1", roles: []}));
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(
        db.runCommand({updateRole: "role3", roles: [{role: "role1", db: "test"}]}));
    audit.assertEntry("updateRole",
                      {role: "role3", db: "test", roles: [{role: "role1", db: "test"}]});

    assert.commandWorked(db.runCommand({updateRole: "role1", privileges: []}));
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(db.runCommand({
        updateRole: "role1",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    audit.assertEntry("updateRole", {
        role: "role1",
        db: "test",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });

    assert.commandWorked(db.runCommand({updateRole: "role1", authenticationRestrictions: []}));
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(db.runCommand(
        {updateRole: "role1", authenticationRestrictions: [{clientSource: ["::1/128"]}]}));
    audit.assertEntry(
        "updateRole",
        {role: "role1", db: "test", authenticationRestrictions: [{clientSource: ["::1/128"]}]});

    print("SUCCESS audit-create-update-user-role.js");
})();
