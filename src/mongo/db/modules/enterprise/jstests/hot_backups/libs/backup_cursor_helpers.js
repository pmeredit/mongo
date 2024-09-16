// Helper library for backup cursor.
import {openBackupCursor} from "jstests/libs/backup_utils.js";

/**
 * Validates that backupCursor can be opened/killed successfully on a replica set node and validates
 * the metadata document returned.
 */
export const validateReplicaSetBackupCursor = function(db) {
    let backupCursor = openBackupCursor(db);
    // The metadata document should be returned first.
    let metadataDocEnvelope = backupCursor.next();
    assert(metadataDocEnvelope.hasOwnProperty("metadata"));

    let metadataDoc = metadataDocEnvelope["metadata"];
    let backupId = metadataDoc["backupId"];
    let oplogStart = metadataDoc["oplogStart"];
    let checkpointTimestamp = metadataDoc["checkpointTimestamp"];

    assert(backupId);
    // When replication is run, there will always be an oplog with a start.
    assert(oplogStart);
    // The first opTime will likely have term -1 (repl initiation).
    assert.gte(oplogStart["t"], -1);

    // The checkpoint timestamp may or may not exist. If it exists, it must be between the start
    // and end.
    if (checkpointTimestamp != null) {
        assert.gte(checkpointTimestamp, oplogStart["ts"]);
    }

    // Kill the backup cursor.
    let cursorId = backupCursor.getId();
    let response =
        assert.commandWorked(db.runCommand({killCursors: "$cmd.aggregate", cursors: [cursorId]}));
    assert.eq(1, response.cursorsKilled.length);
    assert.eq(cursorId, response.cursorsKilled[0]);
};