// Test auditing of the killCursors command.
(function() {
"use strict";

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const m =
    MongoRunner.runMongodAuditLogger({auth: "", setParameter: {"auditAuthorizationSuccess": true}});

const audit = m.auditSpooler();
const db = m.getDB("admin");

audit.verifyAuditEntriesForCursors = function(cursorIds, expectKillCursorsEntry) {
    assert(Array.isArray(cursorIds));
    cursorIds.forEach(cursorId => {
        assert(cursorId);
    });

    const startLine = audit._auditLine;
    if (expectKillCursorsEntry) {
        audit.assertEntryRelaxed("authCheck", {
            command: "killCursors",
            ns: "admin.audit",
            args: {killCursors: "audit", cursors: cursorIds}
        });
    }

    cursorIds.forEach(cursorId => {
        audit._auditLine = startLine;
        audit.assertEntry("authCheck", {
            command: "killCursors",
            ns: "admin.audit",
            args: {killCursors: "audit", cursorId: cursorId}
        });
    });
};

assert.commandWorked(
    db.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
db.auth({user: "user1", pwd: "pwd"});
audit.fastForward();

for (let i = 0; i < 100; i++) {
    assert.writeOK(db.audit.insert({_id: i}));
}

let query, query2;
let cursorId, cursorId2;
let cmdRes;

jsTest.log('Testing cursor via running find(), then kill');

query = db.audit.find().batchSize(1);
query.next();
cursorId = query["_cursor"]["_cursorid"];
assert.neq(cursorId, NumberLong(0));
cmdRes =
    assert.commandWorked(db.runCommand({killCursors: db.audit.getName(), cursors: [cursorId]}));
audit.verifyAuditEntriesForCursors([cursorId], true);

jsTest.log('Testing cursor via running aggregate(), then kill');

query = db.audit.aggregate([{$limit: 500}], {cursor: {batchSize: 1}});
cursorId = query._cursorid;
assert.neq(cursorId, NumberLong(0));
cmdRes =
    assert.commandWorked(db.runCommand({killCursors: db.audit.getName(), cursors: [cursorId]}));
audit.verifyAuditEntriesForCursors([cursorId], true);

jsTest.log('Testing cursor via running listCollections, then kill');

db.createCollection("tempColA");
db.createCollection("tempColB");
cmdRes = db.runCommand({listCollections: 1, cursor: {batchSize: 1}});
cursorId = cmdRes.cursor.id;
cmdRes = db.runCommand({killCursors: db.audit.getName(), cursors: [cursorId]});
audit.verifyAuditEntriesForCursors([cursorId], true);

jsTest.log('Testing cursor via find(), then drop query before it completes');

query = db.audit.find().batchSize(1);
query.next();
cursorId = query["_cursor"]["_cursorid"];
assert.neq(cursorId, NumberLong(0));
query.close();
audit.verifyAuditEntriesForCursors([cursorId], false);

jsTest.log('Testing cursor via aggregate(), then drop query before it completes');

query = db.audit.aggregate([{$limit: 500}], {cursor: {batchSize: 1}});
query.next();
cursorId = query["_cursorid"];
assert.neq(cursorId, NumberLong(0));
query.close();
audit.verifyAuditEntriesForCursors([cursorId], false);

jsTest.log('Testing multiple cursors killed in one command');

query = db.audit.find().batchSize(1);
query2 = db.audit.find().batchSize(2);
query.next();
query2.next();
cursorId = query["_cursor"]["_cursorid"];
assert.neq(cursorId, NumberLong(0));
cursorId2 = query2["_cursor"]["_cursorid"];
assert.neq(cursorId2, NumberLong(0));
cmdRes = assert.commandWorked(
    db.runCommand({killCursors: db.audit.getName(), cursors: [cursorId, cursorId2]}));
audit.verifyAuditEntriesForCursors([cursorId, cursorId2], true);

MongoRunner.stopMongod(m);
}());
