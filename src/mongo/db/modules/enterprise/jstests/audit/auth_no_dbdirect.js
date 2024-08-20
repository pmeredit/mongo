// Check to make sure that there are no entries from DB Direct Client in the audit log
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

let setupTest = function(auditSpoolers, admin) {
    assert.commandWorked(
        admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

    assert(admin.auth({user: "user1", pwd: "pwd"}));

    auditSpoolers.forEach(audit => audit.fastForward());

    assert.commandWorked(
        admin.runCommand({createUser: "user2", pwd: "pwd2", roles: [{role: "root", db: "admin"}]}));
};

let checkAuditLog = function(audit) {
    // The DB direct client runs two commands, calling update on admin.system.version and calling
    // insert on admin.system.users. We want to make sure these commands are not logged.
    audit.assertNoNewEntries("authCheck", {"command": "update"});
    audit.assertNoNewEntries("authCheck", {"command": "insert"});
};

const st = MongoRunner.runShardedClusterAuditLogger(
    {}, {setParameter: {auditAuthorizationSuccess: true}, auth: null});
const auditMongos = st.s0.auditSpooler();

const auditShard = st.rs0.nodes[0].auditSpooler();
const admin = st.s0.getDB("admin");

setupTest([auditMongos, auditShard], admin);
checkAuditLog(auditMongos);
checkAuditLog(auditShard);

st.stop();
