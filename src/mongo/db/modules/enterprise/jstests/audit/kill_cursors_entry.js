// Test auditing of the killCursors command.
(function() {
    "use strict";

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');

    const standalone = MongoRunner.runMongodAuditLogger({});
    const audit = standalone.auditSpooler();

    const testDB = standalone.getDB("test");

    // Issue a killCursors that the server expects to be owned by a collection, and verify the audit
    // entry. Cursors owned by a collection have 00 as their most significant bits.
    const collectionOwnedCursorId = NumberLong("1");
    let cmdRes = assert.commandWorked(
        testDB.runCommand({killCursors: "kill_cursors_entry", cursors: [collectionOwnedCursorId]}));
    assert.eq(1, cmdRes.cursorsNotFound.length);
    audit.assertEntry("authCheck", {
        command: "killCursors",
        ns: "test.kill_cursors_entry",
        args: {killCursors: "kill_cursors_entry", cursorId: collectionOwnedCursorId}
    });

    // Issue a killCursors that the server expects to be owned by the global cursor manager, and
    // verify the audit entry. Cursors owned by the global cursor manager have 01 as their most
    // significant bits. We use 0x7000000000000000.
    const globallyOwnedCursorId = NumberLong("8070450532247928832");
    cmdRes = assert.commandWorked(
        testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [globallyOwnedCursorId]}));
    assert.eq(1, cmdRes.cursorsNotFound.length);
    audit.assertEntry("authCheck", {
        command: "killCursors",
        ns: "test.$cmd.aggregate",
        args: {killCursors: "$cmd.aggregate", cursorId: globallyOwnedCursorId}
    });
    MongoRunner.stopMongod(standalone);
}());
