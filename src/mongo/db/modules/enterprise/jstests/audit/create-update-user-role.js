// Verify {create,update}{User,Role} is sent to audit log

(function() {
    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-create-update-user-role.js");

    m = MongoRunner.runMongodAuditLogger({setParameter: {auditAuthorizationSuccess: true}});
    audit = m.auditSpooler();
    db = m.getDB("test");

    // CreateUser commands
    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "user", roles: []}));
    audit.assertEntryRelaxed("authCheck",
                             {args: {createUser: "user1", pwd: "xxx", $db: "test", roles: []}});
    audit.assertEntry("createUser", {user: "user1", db: "test", roles: []});

    assert.commandWorked(db.runCommand(
        {createUser: "user2", pwd: "user", roles: [{role: "readWrite", db: "admin"}]}));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createUser: "user2",
            pwd: "xxx",
            $db: "test",
            roles: [{role: "readWrite", db: "admin"}]
        }
    });
    audit.assertEntry("createUser",
                      {user: "user2", db: "test", roles: [{role: "readWrite", db: "admin"}]});

    assert.commandWorked(db.runCommand(
        {createUser: "user3", pwd: "user", roles: [], authenticationRestrictions: []}));
    audit.assertEntryRelaxed("authCheck",
                             {args: {createUser: "user3", pwd: "xxx", $db: "test", roles: []}});
    audit.assertEntry("createUser", {user: "user3", db: "test", roles: []});

    assert.commandWorked(db.runCommand({
        createUser: "user4",
        pwd: "user",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createUser: "user4",
            pwd: "xxx",
            $db: "test",
            roles: [],
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });
    audit.assertEntry("createUser", {
        user: "user4",
        db: "test",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // UpdateUser commands
    assert.commandWorked(db.runCommand({updateUser: "user1", pwd: "secret"}));
    audit.assertEntryRelaxed("authCheck", {args: {updateUser: "user1", pwd: "xxx", $db: "test"}});
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: true});

    assert.commandWorked(
        db.runCommand({updateUser: "user1", roles: [{role: "readWrite", db: "admin"}]}));
    audit.assertEntryRelaxed(
        "authCheck",
        {args: {updateUser: "user1", roles: [{role: "readWrite", db: "admin"}], $db: "test"}});
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        roles: [{role: "readWrite", db: "admin"}]
    });

    assert.commandWorked(db.runCommand(
        {updateUser: "user1", authenticationRestrictions: [{serverAddress: ["::1/128"]}]}));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            updateUser: "user1",
            $db: "test",
            authenticationRestrictions: [{serverAddress: ["::1/128"]}]
        }
    });
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        authenticationRestrictions: [{serverAddress: ["::1/128"]}]
    });

    assert.commandWorked(db.runCommand({updateUser: "user1", authenticationRestrictions: []}));
    audit.assertEntryRelaxed(
        "authCheck", {args: {updateUser: "user1", $db: "test", authenticationRestrictions: []}});
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: false});

    // CreateRole commands
    assert.commandWorked(db.runCommand({createRole: "role1", roles: [], privileges: []}));
    audit.assertEntryRelaxed("authCheck",
                             {args: {createRole: "role1", $db: "test", privileges: [], roles: []}});
    audit.assertEntry("createRole", {role: "role1", db: "test", roles: [], privileges: []});

    assert.commandWorked(
        db.runCommand({createRole: "role2", roles: [{role: "role1", db: "test"}], privileges: []}));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createRole: "role2",
            $db: "test",
            privileges: [],
            roles: [{role: "role1", db: "test"}]
        }
    });
    audit.assertEntry(
        "createRole",
        {role: "role2", db: "test", roles: [{role: "role1", db: "test"}], privileges: []});

    assert.commandWorked(db.runCommand({
        createRole: "role3",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createRole: "role3",
            $db: "test",
            privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}],
            roles: []
        }
    });
    audit.assertEntry("createRole", {
        role: "role3",
        db: "test",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });

    assert.commandWorked(db.runCommand(
        {createRole: "role4", roles: [], privileges: [], authenticationRestrictions: []}));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createRole: "role4",
            $db: "test",
            privileges: [],
            roles: [],
            authenticationRestrictions: []
        }
    });
    audit.assertEntry("createRole", {role: "role4", db: "test", roles: [], privileges: []});

    assert.commandWorked(db.runCommand({
        createRole: "role5",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            createRole: "role5",
            $db: "test",
            privileges: [],
            roles: [],
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });
    audit.assertEntry("createRole", {
        role: "role5",
        db: "test",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // UpdateRole command
    assert.commandWorked(db.runCommand({updateRole: "role1", roles: []}));
    audit.assertEntryRelaxed("authCheck", {args: {updateRole: "role1", $db: "test", roles: []}});
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(
        db.runCommand({updateRole: "role3", roles: [{role: "role1", db: "test"}]}));
    audit.assertEntryRelaxed(
        "authCheck",
        {args: {updateRole: "role3", $db: "test", roles: [{role: "role1", db: "test"}]}});
    audit.assertEntry("updateRole",
                      {role: "role3", db: "test", roles: [{role: "role1", db: "test"}]});

    assert.commandWorked(db.runCommand({updateRole: "role1", privileges: []}));
    audit.assertEntryRelaxed("authCheck",
                             {args: {updateRole: "role1", $db: "test", privileges: []}});
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(db.runCommand({
        updateRole: "role1",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            updateRole: "role1",
            $db: "test",
            privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
        }
    });
    audit.assertEntry("updateRole", {
        role: "role1",
        db: "test",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });

    assert.commandWorked(db.runCommand({updateRole: "role1", authenticationRestrictions: []}));
    audit.assertEntryRelaxed(
        "authCheck", {args: {updateRole: "role1", $db: "test", authenticationRestrictions: []}});
    audit.assertEntry("updateRole", {role: "role1", db: "test"});

    assert.commandWorked(db.runCommand(
        {updateRole: "role1", authenticationRestrictions: [{clientSource: ["::1/128"]}]}));
    audit.assertEntryRelaxed("authCheck", {
        args: {
            updateRole: "role1",
            $db: "test",
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });
    audit.assertEntry(
        "updateRole",
        {role: "role1", db: "test", authenticationRestrictions: [{clientSource: ["::1/128"]}]});

    MongoRunner.stopMongod(m);
    print("SUCCESS audit-create-update-user-role.js");
})();
