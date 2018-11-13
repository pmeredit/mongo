/**
 * Test the basic operation of a `$backupCursorExtend` aggregation stage.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
(function() {
    "use strict";

    let conn = MongoRunner.runMongod();
    let db = conn.getDB("test");

    function assertFailedToParse(parameters) {
        assert.commandFailedWithCode(
            db.runCommand(
                {aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}),
            ErrorCodes.FailedToParse);
    }

    function assertBackupIdNotFound(parameters) {
        assert.commandFailedWithCode(
            db.runCommand(
                {aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}),
            51011);
    }

    function assertSuccessfulExtend(parameters) {
        assert.commandWorked(db.runCommand(
            {aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}));
    }

    const backupIdNotExist = UUID();
    const binData = BinData(0, "AAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    const extendTo = Timestamp(100, 1);
    const nullTimestamp = Timestamp();

    // 1. Extend without a running backup cursor.
    assertBackupIdNotFound({backupId: backupIdNotExist, timestamp: extendTo});

    // 2. Extend with invalid parameters.
    const aggBackupCursor = db.aggregate([{$backupCursor: {}}]);
    const backupId = aggBackupCursor.next().metadata.backupId;

    assertFailedToParse({timestamp: extendTo});                     // Without backupId.
    assertFailedToParse({backupId: 1, timestamp: extendTo});        // Wrong type backupId.
    assertFailedToParse({backupId: binData, timestamp: extendTo});  // Wrong type backupId.
    assertBackupIdNotFound({backupId: backupIdNotExist, timestamp: extendTo});  // Invalid backupId.

    assertFailedToParse({backupId: backupId});                            // Without timestamp.
    assertFailedToParse({backupId: backupId, timestamp: 1});              // Wrong type timestamp.
    assertFailedToParse({backupId: backupId, timestamp: nullTimestamp});  // Invalid timestamp.

    // With extraneous unknown parameter.
    assertFailedToParse({bakcupId: backupId, timestamp: extendTo, extraParam: 1});

    // 3. Successful extend.
    assertSuccessfulExtend({backupId: backupId, timestamp: extendTo});
    assertSuccessfulExtend({backupId: backupId, timestamp: extendTo});  // Extend again.

    // Expected usage is for the tailable $backupCursor to be explicitly killed by the client.
    aggBackupCursor.close();
    MongoRunner.stopMongod(conn);
})();
