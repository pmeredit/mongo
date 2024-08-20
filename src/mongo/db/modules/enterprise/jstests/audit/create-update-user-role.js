// Verify {create,update}{User,Role} is sent to audit log

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

let runCommands = function(db) {
    // CreateUser commands
    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "user", roles: []}));
    assert.commandWorked(db.runCommand(
        {createUser: "user2", pwd: "user", roles: [{role: "readWrite", db: "admin"}]}));
    assert.commandWorked(db.runCommand(
        {createUser: "user3", pwd: "user", roles: [], authenticationRestrictions: []}));
    assert.commandWorked(db.runCommand({
        createUser: "user4",
        pwd: "user",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));

    // UpdateUser commands
    assert.commandWorked(db.runCommand({updateUser: "user1", pwd: "secret"}));
    assert.commandWorked(
        db.runCommand({updateUser: "user1", roles: [{role: "readWrite", db: "admin"}]}));
    assert.commandWorked(db.runCommand(
        {updateUser: "user1", authenticationRestrictions: [{serverAddress: ["::1/128"]}]}));
    assert.commandWorked(db.runCommand({updateUser: "user1", authenticationRestrictions: []}));

    // CreateRole commands
    assert.commandWorked(db.runCommand({createRole: "role1", roles: [], privileges: []}));
    assert.commandWorked(
        db.runCommand({createRole: "role2", roles: [{role: "role1", db: "test"}], privileges: []}));
    assert.commandWorked(db.runCommand({
        createRole: "role3",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    assert.commandWorked(db.runCommand(
        {createRole: "role4", roles: [], privileges: [], authenticationRestrictions: []}));
    assert.commandWorked(db.runCommand({
        createRole: "role5",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    }));

    // UpdateRole command
    assert.commandWorked(db.runCommand({updateRole: "role1", roles: []}));
    assert.commandWorked(
        db.runCommand({updateRole: "role3", roles: [{role: "role1", db: "test"}]}));
    assert.commandWorked(db.runCommand({updateRole: "role1", privileges: []}));
    assert.commandWorked(db.runCommand({
        updateRole: "role1",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    }));
    assert.commandWorked(db.runCommand({updateRole: "role1", authenticationRestrictions: []}));
    assert.commandWorked(db.runCommand(
        {updateRole: "role1", authenticationRestrictions: [{clientSource: ["::1/128"]}]}));

    // GrantPrivilegesToRole command
    const grantRevokePrivilege = {resource: {db: "test", collection: "bar"}, actions: ["find"]};
    assert.commandWorked(
        db.runCommand({grantPrivilegesToRole: "role1", privileges: [grantRevokePrivilege]}));

    // RevokePrivilegesFromRole command
    assert.commandWorked(
        db.runCommand({revokePrivilegesFromRole: "role1", privileges: [grantRevokePrivilege]}));
};

let checkAuditAuth = function(audit) {
    // Create User auth audit entries
    audit.assertEntryRelaxed(
        "authCheck",
        {ns: "test.user1", args: {createUser: "user1", pwd: "xxx", $db: "test", roles: []}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.user2",
        args: {
            createUser: "user2",
            pwd: "xxx",
            $db: "test",
            roles: [{role: "readWrite", db: "admin"}]
        }
    });
    audit.assertEntryRelaxed(
        "authCheck",
        {ns: "test.user3", args: {createUser: "user3", pwd: "xxx", $db: "test", roles: []}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.user4",
        args: {
            createUser: "user4",
            pwd: "xxx",
            $db: "test",
            roles: [],
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });

    // Update User auth audit entries
    audit.assertEntryRelaxed("authCheck", {args: {updateUser: "user1", pwd: "xxx", $db: "test"}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.user1",
        args: {updateUser: "user1", roles: [{role: "readWrite", db: "admin"}], $db: "test"}
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.user1",
        args: {
            updateUser: "user1",
            $db: "test",
            authenticationRestrictions: [{serverAddress: ["::1/128"]}]
        }
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.user1",
        args: {updateUser: "user1", $db: "test", authenticationRestrictions: []}
    });

    // Create Role auth audit entries
    audit.assertEntryRelaxed(
        "authCheck",
        {ns: "test.role1", args: {createRole: "role1", $db: "test", privileges: [], roles: []}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role2",
        args:
            {createRole: "role2", $db: "test", privileges: [], roles: [{role: "role1", db: "test"}]}
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role3",
        args: {
            createRole: "role3",
            $db: "test",
            privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}],
            roles: []
        }
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role4",
        args: {
            createRole: "role4",
            $db: "test",
            privileges: [],
            roles: [],
            authenticationRestrictions: []
        }
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role5",
        args: {
            createRole: "role5",
            $db: "test",
            privileges: [],
            roles: [],
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });

    // Update Role auth audit entries
    audit.assertEntryRelaxed(
        "authCheck", {ns: "test.role1", args: {updateRole: "role1", $db: "test", roles: []}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role3",
        args: {updateRole: "role3", $db: "test", roles: [{role: "role1", db: "test"}]}
    });
    audit.assertEntryRelaxed(
        "authCheck", {ns: "test.role1", args: {updateRole: "role1", $db: "test", privileges: []}});
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role1",
        args: {
            updateRole: "role1",
            $db: "test",
            privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
        }
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role1",
        args: {updateRole: "role1", $db: "test", authenticationRestrictions: []}
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role1",
        args: {
            updateRole: "role1",
            $db: "test",
            authenticationRestrictions: [{clientSource: ["::1/128"]}]
        }
    });

    // Grant and revoke privileges to/from role
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role1",
        args: {
            grantPrivilegesToRole: "role1",
            privileges: [{resource: {db: "test", collection: "bar"}, actions: ["find"]}]
        }
    });
    audit.assertEntryRelaxed("authCheck", {
        ns: "test.role1",
        args: {
            revokePrivilegesFromRole: "role1",
            privileges: [{resource: {db: "test", collection: "bar"}, actions: ["find"]}]
        }
    });
};

let checkAuditDB = function(audit) {
    // Create User db audit entries
    audit.assertEntry("createUser", {user: "user1", db: "test", roles: []});
    audit.assertEntry("createUser",
                      {user: "user2", db: "test", roles: [{role: "readWrite", db: "admin"}]});
    audit.assertEntry("createUser", {user: "user3", db: "test", roles: []});
    audit.assertEntry("createUser", {
        user: "user4",
        db: "test",
        roles: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // Update User db audit entries
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: true});
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        roles: [{role: "readWrite", db: "admin"}]
    });
    audit.assertEntry("updateUser", {
        user: "user1",
        db: "test",
        passwordChanged: false,
        authenticationRestrictions: [{serverAddress: ["::1/128"]}]
    });
    audit.assertEntry("updateUser", {user: "user1", db: "test", passwordChanged: false});

    // Create Role db audit entries
    audit.assertEntry("createRole", {role: "role1", db: "test", roles: [], privileges: []});
    audit.assertEntry(
        "createRole",
        {role: "role2", db: "test", roles: [{role: "role1", db: "test"}], privileges: []});
    audit.assertEntry("createRole", {
        role: "role3",
        db: "test",
        roles: [],
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });
    audit.assertEntry("createRole", {role: "role4", db: "test", roles: [], privileges: []});
    audit.assertEntry("createRole", {
        role: "role5",
        db: "test",
        roles: [],
        privileges: [],
        authenticationRestrictions: [{clientSource: ["::1/128"]}]
    });

    // Update Role db audit entries
    audit.assertEntry("updateRole", {role: "role1", db: "test"});
    audit.assertEntry("updateRole",
                      {role: "role3", db: "test", roles: [{role: "role1", db: "test"}]});
    audit.assertEntry("updateRole", {role: "role1", db: "test"});
    audit.assertEntry("updateRole", {
        role: "role1",
        db: "test",
        privileges: [{resource: {db: "test", collection: "foo"}, actions: ["find"]}]
    });
    audit.assertEntry("updateRole", {role: "role1", db: "test"});
    audit.assertEntry(
        "updateRole",
        {role: "role1", db: "test", authenticationRestrictions: [{clientSource: ["::1/128"]}]});

    // Grant and privileges to/from role
    audit.assertEntry("grantPrivilegesToRole", {
        role: "role1",
        db: "test",
        privileges: [{resource: {db: "test", collection: "bar"}, actions: ["find"]}]
    });
    audit.assertEntry("revokePrivilegesFromRole", {
        role: "role1",
        db: "test",
        privileges: [{resource: {db: "test", collection: "bar"}, actions: ["find"]}]
    });
};

{
    print("START audit-create-update-user-role.js on standalone");
    const m = MongoRunner.runMongodAuditLogger({setParameter: {auditAuthorizationSuccess: true}});
    const audit = m.auditSpooler();
    const db = m.getDB("test");

    runCommands(db);
    checkAuditAuth(audit);
    audit.resetAuditLine();
    checkAuditDB(audit);

    MongoRunner.stopMongod(m);
    print("SUCCESS audit-create-update-user-role.js on standalone");
}

{
    jsTest.log("START audit-create-update-user-role.js for mongos on Sharded Cluster");
    const st = MongoRunner.runShardedClusterAuditLogger(
        {}, {setParameter: {auditAuthorizationSuccess: true}});
    const auditMongos = st.s0.auditSpooler();
    const auditConfig = st.configRS.nodes[0].auditSpooler();
    const db = st.s0.getDB("test");

    runCommands(db);
    checkAuditAuth(auditMongos);
    checkAuditDB(auditConfig);

    st.stop();
    print("SUCCESS audit-create-update-user-role.js for mongos on Sharded Cluster");
}
