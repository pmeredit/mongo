// Verify an auth failure is NOT audited when an unauthed session is terminated.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
print("START orphan-session.js");

const m = MongoRunner.runMongodAuditLogger({auth: ''});
const audit = m.auditSpooler();
const admin = m.getDB("admin");

// Create a "dummy" user to turn off the localhost auth bypass.
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));

// Sanity check that localhost auth bypass is off.
assert.commandFailed(admin.runCommand({createUser: "nobody", pwd: "fail", roles: []}));

// Explicitly close a session without auth.
// This will fail as unauthorized, but we don't need to see a audit for it.
audit.fastForward();
const session = m.startSession();
assert.commandFailedWithCode(admin.runCommand({endSessions: [session.getSessionId()]}),
                             ErrorCodes.Unauthorized);
audit.assertNoNewEntries("authCheck");

MongoRunner.stopMongod(m);
print("SUCCESS orphan-session.js");
})();
