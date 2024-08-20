// Verify {create,update,remove}{User,Role} via direct table manipulation is sent to audit log

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {AuditSpooler} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

print("START audit-crud-user-role-direct.js");

function runTests(db, auditPrimary, auditSecondary) {
    print("Create user by inserting directly into admin.system.users");
    assert.commandWorked(db.system.users.insert({
        "_id": "test.user4",
        "userId": {"$binary": "oK3OqNTYRAiwIEf0vg6taA==", "$type": "04"},
        "user": "user4",
        "db": "test",
        "credentials": {
            "SCRAM-SHA-256": {
                "iterationCount": 15000,
                "salt": "6U1oXpwvRgxwhSV6ezwiuaqcKvmgEosaqCRnyg==",
                "storedKey": "Ugz4JTaetYIO6l5ppWGM8Tgb66yNoX7bp7Io+fAsDtA=",
                "serverKey": "YrdccDFtsaNx3PL7JjDWiSip32rUSi66aSR8s1wa030="
            }
        },
        "roles": [{"role": "readWrite", "db": "test"}]
    }));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.user4",
            "userId": {"$binary": "oK3OqNTYRAiwIEf0vg6taA==", "$type": "04"},
            "user": "user4",
            "db": "test",
            "credentials": ["SCRAM-SHA-256"],
            "roles": [{"role": "readWrite", "db": "test"}]
        },
        "ns": "admin.system.users",
        "operation": "insert"
    });

    print("Update user by directly changing admin.system.users");
    assert.commandWorked(db.system.users.update({_id: "test.user4"},
                                                {"$set": {roles: [{role: "root", db: "test"}]}}));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.user4",
            "userId": {"$binary": "oK3OqNTYRAiwIEf0vg6taA==", "$type": "04"},
            "user": "user4",
            "db": "test",
            "credentials": ["SCRAM-SHA-256"],
            "roles": [{"role": "root", "db": "test"}]
        },
        "ns": "admin.system.users",
        "operation": "update"
    });

    print("Remove user by directly changing admin.system.users");
    assert.commandWorked(db.system.users.remove({_id: "test.user4"}));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.user4",
            "userId": {"$binary": "oK3OqNTYRAiwIEf0vg6taA==", "$type": "04"},
            "user": "user4",
            "db": "test",
            "credentials": ["SCRAM-SHA-256"],
            "roles": [{"role": "root", "db": "test"}]
        },
        "ns": "admin.system.users",
        "operation": "remove"
    });

    print("create role by inserting directly into admin.system.roles");
    assert.commandWorked(db.system.roles.insert({
        "_id": "test.role6",
        "role": "role6",
        "db": "test",
        "privileges": [],
        "roles": [{"role": "readOnly", "db": "test"}]
    }));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.role6",
            "role": "role6",
            "db": "test",
            "privileges": [],
            "roles": [{"role": "readOnly", "db": "test"}]
        },
        "ns": "admin.system.roles",
        "operation": "insert"
    });

    print("update role by directly changing admin.system.roles");
    assert.commandWorked(db.system.roles.update(
        {_id: "test.role6"}, {"$set": {roles: [{role: "readWrite", db: "test"}]}}));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.role6",
            "role": "role6",
            "db": "test",
            "privileges": [],
            "roles": [{"role": "readWrite", "db": "test"}]
        },
        "ns": "admin.system.roles",
        "operation": "update"
    });

    print("Remove role by directly changing admin.system.users");
    assert.commandWorked(db.system.roles.remove({_id: "test.role6"}));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {
            "_id": "test.role6",
            "role": "role6",
            "db": "test",
            "privileges": [],
            "roles": [{"role": "readWrite", "db": "test"}]
        },
        "ns": "admin.system.roles",
        "operation": "remove"
    });

    print("Remove all roles by renaming system.roles table");
    assert.commandWorked(db.system.roles.renameCollection("rrr"));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {"renameCollection": "admin.system.roles", "to": "admin.rrr"},
        "ns": "admin.system.roles",
        "operation": "command"
    });

    print("Create roles by renaming other table to system.roles");
    assert.commandWorked(db.rrr.renameCollection("system.roles"));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {"renameCollection": "admin.rrr", "to": "admin.system.roles"},
        "ns": "admin.system.roles",
        "operation": "command"
    });

    print("Delete system.roles via applyOps");
    let uuidSystemRoles = db.getCollectionInfos({name: "system.roles"}).map(x => x.info.uuid)[0];
    assert.commandWorked(db.runCommand({
        applyOps: [{
            "op": "c",
            "ns": "admin.$cmd",
            "ui": uuidSystemRoles,
            "o": {"drop": "system.roles"},
            "o2": {"numRecords": 1},
            "ts": Timestamp(1612481521, 1),
            "t": NumberLong(1),
            "wall": ISODate("2021-02-04T23:32:01.016Z"),
            "v": NumberLong(2)
        }]
    }));
    auditPrimary.assertEntry("directAuthMutation", {
        "document": {"dropCollection": "admin.system.roles"},
        "ns": "admin.system.roles",
        "operation": "command"
    });

    if (auditSecondary !== undefined) {
        print("Secondaries should not log any directAuthMutation events");
        auditSecondary.assertNoEntry("directAuthMutation");
    }
}  // function runTests()

jsTest.log('Testing on a standalone server...');
const m = MongoRunner.runMongodAuditLogger({setParameter: {auditAuthorizationSuccess: true}});

runTests(m.getDB("admin"), m.auditSpooler());
MongoRunner.stopMongod(m);
jsTest.log('Testing on a standalone server complete');

jsTest.log('Testing on a replica set...');
const options = {
    name: "auditCrudUserRoleDirect",
    nodes: [
        {
            auditDestination: "file",
            auditFormat: "JSON",
            auditPath: MongoRunner.dataPath + '/auditCrudUserRoleDirect_node0.log',
        },
        {
            auditDestination: "file",
            auditFormat: "JSON",
            auditPath: MongoRunner.dataPath + '/auditCrudUserRoleDirect_node1.log',
            rsConfig: {
                priority: 0,
                votes: 0,
            },
        }
    ]
};

let rst = new ReplSetTest(options);
rst.startSet();
rst.initiate();
rst.awaitSecondaryNodes();

const db = rst.getPrimary().getDB("admin");
const auditPrimary = new AuditSpooler(options.nodes[0].auditPath, false);
const auditSecondary = new AuditSpooler(options.nodes[1].auditPath, false);
runTests(db, auditPrimary, auditSecondary);
rst.stopSet();
jsTest.log('Testing on a replica set complete');

print("SUCCESS audit-crud-user-role-direct.js");
