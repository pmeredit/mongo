/**
 * Tests end-to-end WiredTiger selective backups and restores.
 *
 * While concurrent FSM workloads are running in the background on the primary node, several
 * full and selective backups are taken. The backups are restored and verified by running dbHash on
 * the collections restored in the selective backup against the collections restored in the full
 * backup.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_replication,
 *     requires_wiredtiger
 * ]
 */
import {copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {extractUUIDFromObject} from "jstests/libs/uuid_util.js";
import {
    kSeparator,
    startFSMClient,
    stopFSMClient
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/incremental_backup_helpers.js";

// Windows doesn't guarantee synchronous file operations.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
        }
    ],
    // Frequent checkpoints.
    nodeOptions:
        {binVersion: 'last-lts', syncdelay: 5, oplogSize: 1024, setParameter: {logLevel: 0}}
});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

// Run the FSM workload on the primary throughout the duration of this test.
const fsmPid = startFSMClient(primary.host);

const fullBackupPath = primary.dbpath + kSeparator + "fullBackup";
const selectiveBackupPath = primary.dbpath + kSeparator + "selectiveBackup";

function resetBackupPaths() {
    resetDbpath(fullBackupPath);
    mkdir(fullBackupPath + kSeparator + "journal");

    resetDbpath(selectiveBackupPath);
    mkdir(selectiveBackupPath + kSeparator + "journal");
}

const kNumIterations = 5;
for (let iteration = 1; iteration <= kNumIterations; iteration++) {
    jsTestLog("Running iteration " + iteration);

    resetBackupPaths();

    const backupCursor = openBackupCursor(primary.getDB("admin"));

    // Print the metadata document.
    assert(backupCursor.hasNext());
    jsTestLog(backupCursor.next());

    const optionalFilesCopied = {};
    const optionalFilesSkipped = {};

    while (backupCursor.hasNext()) {
        const doc = backupCursor.next();

        // Copy all the files for the full backup.
        jsTestLog("Copying for full backup: " + tojson(doc));
        copyFileHelper(
            {filename: doc.filename, fileSize: doc.fileSize}, primary.dbpath, fullBackupPath);

        if (doc.required) {
            jsTestLog("Copying required file for selective backup: " + tojson(doc));
            copyFileHelper({filename: doc.filename, fileSize: doc.fileSize},
                           primary.dbpath,
                           selectiveBackupPath);
            continue;
        }

        const filename = doc.filename.substring(doc.filename.lastIndexOf(kSeparator) + 1);
        const identStem = filename.split(".")[0];

        if (doc.ns.length == 0) {
            // The file is known to the storage engine but not the durable catalog at the backups
            // checkpoint timestamp. WiredTiger will not reconstruct the metadata for this file
            // during startup recovery as it isn't copied.
            jsTestLog("Skipping orphaned file for selective backup: " + tojson(doc));
            assert.eq("", doc.uuid);
            continue;
        }

        assert.neq("", doc.ns);
        assert.neq("", doc.uuid);

        if (optionalFilesCopied.hasOwnProperty(doc.uuid)) {
            // At least one file for this ident was already copied, so this needs to be copied too.
            if (identStem.startsWith("collection-")) {
                optionalFilesCopied[doc.uuid].collUri = identStem;
            } else if (identStem.startsWith("index-")) {
                optionalFilesCopied[doc.uuid].indexUris.push(identStem);
            }

            jsTestLog("Copying for selective backup: " + tojson(doc));
            copyFileHelper({filename: doc.filename, fileSize: doc.fileSize},
                           primary.dbpath,
                           selectiveBackupPath);
            continue;
        }

        if (optionalFilesSkipped.hasOwnProperty(doc.uuid)) {
            // At least one file for this ident was already skipped, so this needs to be skipped
            // too.
            if (identStem.startsWith("collection-")) {
                optionalFilesSkipped[doc.uuid].collUri = identStem;
            } else if (identStem.startsWith("index-")) {
                optionalFilesSkipped[doc.uuid].indexUris.push(identStem);
            }

            jsTestLog("Skipping for selective backup: " + tojson(doc));
            continue;
        }

        const entry = {collUri: undefined, indexUris: [], ns: doc.ns, uuid: doc.uuid};
        if (identStem.startsWith("collection-")) {
            entry.collUri = identStem;
        } else if (identStem.startsWith("index-")) {
            entry.indexUris.push(identStem);
        }

        if (Math.random() < 0.5) {
            // 50% chance to either copy or skip this ident.
            optionalFilesSkipped[doc.uuid] = entry;

            jsTestLog("Skipping for selective backup: " + tojson(doc));
            continue;
        }

        optionalFilesCopied[doc.uuid] = entry;

        jsTestLog("Copying for selective backup: " + tojson(doc));
        copyFileHelper(
            {filename: doc.filename, fileSize: doc.fileSize}, primary.dbpath, selectiveBackupPath);
    }

    backupCursor.close();

    jsTestLog("Optional files copied for selective backup: " + tojson(optionalFilesCopied));
    jsTestLog("Optional files skipped for selective backup: " + tojson(optionalFilesSkipped));

    const numCollectionsSkipped = Object.keys(optionalFilesSkipped).length;

    // Restore the full backup.
    let fullBackupConn = MongoRunner.runMongod(
        {dbpath: fullBackupPath, noCleanData: true, restore: "", setParameter: {logLevel: 0}});
    assert(fullBackupConn);

    // Clear the RamLog to avoid reading the log from the restore above.
    clearRawMongoProgramOutput();

    // Restore the selective backup.
    let selectiveBackupConn = MongoRunner.runMongod({
        dbpath: selectiveBackupPath,
        noCleanData: true,
        restore: "",
        setParameter: {
            logLevel: 0,
            // For log 22251
            logComponentVerbosity: tojson({storage: 1}),
        }
    });
    assert(selectiveBackupConn);

    // Files known to the storage engine but not the durable catalog at the recovery timestamp will
    // be dropped and need to be removed from optionalFilesCopied.
    rawMongoProgramOutput("ident").split('\n').forEach((line) => {
        if (!line.includes("Dropping unknown ident") && !line.includes("Dropping internal ident")) {
            return;
        }

        // JSONify the log line.
        const firstBraceIdx = line.search("{");
        const lineObj = JSON.parse(line.substring(firstBraceIdx));
        const droppedIdent = lineObj.attr.ident;

        for (const optionalFileCopied of Object.keys(optionalFilesCopied)) {
            const entry = optionalFilesCopied[optionalFileCopied];
            if (entry.collUri == droppedIdent || entry.indexUris.includes(droppedIdent)) {
                jsTestLog("Removing optional file copied during startup recovery: " +
                          tojson(entry));
                delete optionalFilesCopied[optionalFileCopied];
                break;
            }
        }
    });

    let selectiveAdminDb = selectiveBackupConn.getDB("admin");
    let dbList = selectiveAdminDb.runCommand({listDatabases: 1, nameOnly: true}).databases;

    dbList.forEach(function(dbInfo) {
        const dbName = dbInfo.name;
        const collList = selectiveBackupConn.getDB(dbName)
                             .runCommand({listCollections: 1, filter: {type: "collection"}})
                             .cursor.firstBatch;

        collList.forEach(function(collInfo) {
            const uuidString = extractUUIDFromObject(collInfo.info.uuid);

            if (optionalFilesCopied.hasOwnProperty(uuidString)) {
                let collName;
                if (collInfo.options.hasOwnProperty("timeseries")) {
                    collName = dbName + "." + collInfo.name.replace("system.buckets.", "");
                } else {
                    collName = dbName + "." + collInfo.name;
                }

                // If this collection was optional and copied, check that the namespace is the same
                // as it was during the backup.
                assert.eq(optionalFilesCopied[uuidString].ns, collName);
            }

            // Remove the entry in optionalFilesCopied, if it exists. optionalFilesCopied needs to
            // be empty at the end of this procedure, otherwise we missed restoring a collection we
            // copied, which would be a bug.
            delete optionalFilesCopied[uuidString];

            // Remove the entry in optionalFilesSkipped, if it exists. If this happens, then we
            // restored a collection we didn't backup, which would be a bug.
            delete optionalFilesSkipped[uuidString];
        });

        const fullDbHash =
            assert.commandWorked(fullBackupConn.getDB(dbName).runCommand({dbHash: 1}));
        const selectiveDbHash =
            assert.commandWorked(selectiveBackupConn.getDB(dbName).runCommand({dbHash: 1}));

        for (const collection of Object.keys(selectiveDbHash.collections)) {
            if (collection == "system.views") {
                // Skip comparing dbHash on 'system.views' collections as views for collections not
                // restored are removed.
                continue;
            }

            const selectiveCollHash = selectiveDbHash.collections[collection];
            const fullCollHash = fullDbHash.collections[collection];

            assert.eq(selectiveCollHash, fullCollHash);
        }
    });

    MongoRunner.stopMongod(fullBackupConn);
    MongoRunner.stopMongod(selectiveBackupConn);

    // Verify that we restored all non-required backed up collections.
    assert.eq(0, Object.keys(optionalFilesCopied).length, optionalFilesCopied);

    // Verify that we didn't restore collections not backed up.
    assert.eq(
        numCollectionsSkipped, Object.keys(optionalFilesSkipped).length, optionalFilesSkipped);

    jsTestLog("Testing point-in-time restore functionality");
    fullBackupConn = MongoRunner.runMongod({
        dbpath: fullBackupPath,
        noCleanData: true,
        restore: "",
        setParameter: {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
    });
    assert(fullBackupConn);

    selectiveBackupConn = MongoRunner.runMongod({
        dbpath: selectiveBackupPath,
        noCleanData: true,
        restore: "",
        setParameter: {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
    });
    assert(selectiveBackupConn);

    // Compare dbHash's now that the oplog was replayed.
    selectiveAdminDb = selectiveBackupConn.getDB("admin");
    dbList = selectiveAdminDb.runCommand({listDatabases: 1, nameOnly: true}).databases;
    dbList.forEach(function(dbInfo) {
        const dbName = dbInfo.name;
        const fullDbHash =
            assert.commandWorked(fullBackupConn.getDB(dbName).runCommand({dbHash: 1}));
        const selectiveDbHash =
            assert.commandWorked(selectiveBackupConn.getDB(dbName).runCommand({dbHash: 1}));

        for (const collection of Object.keys(selectiveDbHash.collections)) {
            if (collection == "system.views") {
                // Skip comparing dbHash on 'system.views' collections as views for collections not
                // restored are removed.
                continue;
            }

            const selectiveCollHash = selectiveDbHash.collections[collection];
            const fullCollHash = fullDbHash.collections[collection];

            assert.eq(selectiveCollHash, fullCollHash);
        }
    });

    MongoRunner.stopMongod(fullBackupConn);
    MongoRunner.stopMongod(selectiveBackupConn);
}

stopFSMClient(fsmPid);
rst.stopSet();
