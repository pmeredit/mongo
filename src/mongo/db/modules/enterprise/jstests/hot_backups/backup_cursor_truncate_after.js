/**
 * This tests runs multiple (two) concurrent writers and after some time, opens a $backupCursor
 * and copies the required data files. The test then asserts the following properties:
 *
 * 1) The copied data is at the checkpointTimestamp returned by the command.
 * 2) The copied data can be manipulated in standalone mode to control exactly how much
 *    replication recovery replays.
 * 3) Writes causally after opening the $backupCursor never show up in the copied data files.
 *
 * More specifically, the writers will record a sample of the returned operation times for their
 * writes. Those are then used that to assert exactly whether data should or should not exist in
 * the backed up data when restored to a specific time.
 *
 * @tags: [requires_persistence, requires_replication, requires_wiredtiger]
 */
(function() {
    "use strict";

    load("jstests/libs/backup_utils.js");
    load("jstests/libs/parallelTester.js");  // for ScopedThread.

    Random.setRandomSeed();
    // Returns true iff opTime <= threshold. In other words, when reading at `threshold`, is
    // `opTime` visible?
    function isVisible(opTime, threshold) {
        return timestampCmp(opTime, threshold) <= 0;
    }

    function isLessThan(opTime1, opTime2) {
        return timestampCmp(opTime1, opTime2) < 0;
    }

    // Gets the collection writer 1 (Alpha) writes to. Before Alpha observes the backup cursor, it
    // writes to `preBackupCursor=true`. After observing the backup cursor was opened, it writes
    // to `preBackupCursor=false`.
    function getAlphaColl(conn, preBackupCursor) {
        if (preBackupCursor) {
            return conn.getDB("test")["alphaPre"];
        } else {
            return conn.getDB("test")["alphaPost"];
        }
    }

    // Gets the collection writer 2 (Beta) writes to.
    function getBetaColl(conn, preBackupCursor) {
        if (preBackupCursor) {
            return conn.getDB("test")["betaPre"];
        } else {
            return conn.getDB("test")["betaPost"];
        }
    }

    // After the $backupCursor is opened, a document is inserted into this collection. The other
    // actors in this test query this collection after each write to observe when the
    // $backupCursor was opened.
    function getBackupColl(conn) {
        return conn.getDB("test")["backup"];
    }

    // This method brings up a standalone on `backupDir`, updates the truncation point document
    // and shuts down the process. Restarting in replica set mode, or as a standalone with
    // `recoverFromOplogAsStandalone` will observe this update and throw away the oplog at the
    // truncation point. Data deleted this way will not come back. Multiple calls on the same
    // `backupDir` should start with later truncation points and follow up with earlier truncation
    // points.
    function manipulateOplogTruncateAfterPoint(backupDir, truncatePoint) {
        let conn = MongoRunner.runMongod({dbpath: backupDir, noCleanData: true});
        assert.neq(null, conn);

        const upsert = true;
        let resp = assert.commandWorked(conn.getDB("local").replset.oplogTruncateAfterPoint.update(
            {_id: "oplogTruncateAfterPoint"},
            {$set: {"oplogTruncateAfterPoint": truncatePoint}},
            upsert));
        assert(resp["nUpserted"] === 1 || resp["nModified"] === 1);
        MongoRunner.stopMongod(conn, {noCleanData: true});
    }

    // Assign names for calling `get<WriterName>Coll`.
    const preBackupCursor = false;
    const postBackupCursor = true;

    // Use a smaller oplog to exercise backup cursors in the face of oplogs that roll over.
    let rst = new ReplSetTest({nodes: 2, nodeOptions: {syncdelay: 1, oplogSize: 1}});
    let nodes = rst.startSet();

    rst.initiate();
    rst.awaitNodesAgreeOnPrimary();

    let primary = rst.getPrimary();
    let secondary = rst.getSecondary();

    function writerFunc(host, collFn, backupCollFn) {
        // Redefine constants for ScopedThread use.
        const preBackupCursor = false;
        const postBackupCursor = true;
        const largePayload = 'a'.repeat(100 * 1024);

        let conn = new Mongo(host);
        let docNum = 0;
        let sampledOpTimes = [];
        while (docNum < 100 * 1000) {
            // Writers will write a document, record the operationTime the server returns and
            // check if a backup cursor was opened.
            let result = assert.commandWorked(collFn(conn, preBackupCursor).runCommand("insert", {
                documents: [{docNum: ++docNum, payload: largePayload}]
            }));
            if (docNum % 50 == 0) {
                sampledOpTimes.push({doc: docNum, opTime: result["operationTime"]});
            }

            if (backupCollFn(conn).find().itcount() == 1) {
                break;
            }
        }

        jsTestLog({
            msg: "Writer observed the backup cursor. Draining.",
            coll: collFn(conn, preBackupCursor),
            lastDoc: docNum
        });
        // After observing the $backupCursor, do some more writes. Because the backup process does
        // not copy the WT log files that are causally after opening the $backupCursor, these
        // writes will never show up on the restore side.
        for (let postCursor = 0; postCursor < 100; ++postCursor) {
            collFn(conn, postBackupCursor).insert({docNum: postCursor});
        }

        return sampledOpTimes;
    }

    const readyWriterOne = new ScopedThread(writerFunc, primary.host, getAlphaColl, getBackupColl);
    readyWriterOne.start();
    const readyWriterTwo = new ScopedThread(writerFunc, primary.host, getBetaColl, getBackupColl);
    readyWriterTwo.start();

    // Wait a little bit to perform writes before opening the $backupCursor.
    sleep(1000 + Random.randInt(1000));

    let backupCursor = openBackupCursor(primary);
    sleep(Random.randInt(1000));
    let primaryBackupMetadata =
        copyCursorFiles(primary, backupCursor, primary.dbpath + "/primary-backup");
    jsTestLog({"Primary $backupCursor metadata": primaryBackupMetadata});
    backupCursor.close();

    backupCursor = openBackupCursor(secondary);
    // Signal that the last $backupCursor was opened. Writers will change behavior once they
    // observe this document.
    getBackupColl(primary).insert({});
    // Give the writers some time to observe the $backupCursor and change behavior before
    // performing file copies. The data copied must not contain causally related writes.
    sleep(1000 + Random.randInt(1000));
    let secondaryBackupMetadata =
        copyCursorFiles(secondary, backupCursor, secondary.dbpath + "/secondary-backup");
    jsTestLog({"Secondary $backupCursor metadata": secondaryBackupMetadata});
    backupCursor.close();

    // Wait for the writers to complete and get a sample list of (docNum, op[eration]Time) pairs.
    readyWriterOne.join();
    readyWriterTwo.join();
    let writerOneOpTimes = readyWriterOne.returnData();
    let writerTwoOpTimes = readyWriterTwo.returnData();

    // Define the method that asserts the data files copied have the expected data. The inputs are
    // the dbpath the files were copied to, the metadata document returned by the $backupCursor and
    // the sample list of inserts by both writers.
    function assertData(backupDir, metadata, writerOneOpTimes, writerTwoOpTimes) {
        // Define the method that selects an additional "interesting" opTime to exercise a "point in
        // time restore" procedure.
        function getAdditionalScenario(metadata, opTimes1, opTimes2) {
            let ckptTime = metadata["checkpointTimestamp"];
            let oplogEnd = metadata["oplogEnd"]["ts"];

            // All opTimes by both the Alpha and Beta writer that are after the checkpoint
            // timestamp and in the oplog copied are equally likely candidates.
            let candidates = [];
            for (let docOpTime of opTimes1) {
                let ts = docOpTime["opTime"];
                if (isLessThan(ckptTime, ts) && isLessThan(ts, oplogEnd)) {
                    candidates.push(ts);
                }
            }
            for (let docOpTime of opTimes2) {
                let ts = docOpTime["opTime"];
                if (isLessThan(ckptTime, ts) && isLessThan(ts, oplogEnd)) {
                    candidates.push(ts);
                }
            }
            if (candidates.length == 0) {
                return null;
            }

            return candidates[Random.randInt(candidates.length)];
        }

        let scenarios = [
            // First startup on the backup data as a standalone without any replication
            // recovery. Only operations earlier than the checkpointTimestamp should be visible.
            {
              visibleTime: metadata["checkpointTimestamp"],
              recoverOplog: false,
            },
            // Then start up with oplog recovery. This will also set the truncate after point to
            // the visibleTime. Add tests in reverse visibleTime order such that later scenarios
            // only need a subset of the oplog of the previous scenario.
            {
              visibleTime: metadata["oplogEnd"]["ts"],
              // Note the `oplogEnd` is a minimum guarantee. It's likely the data contains more
              // oplog entries than this value. `recoverOplog: true` will call set a
              // truncateAfterPoint which will result in deleting those excess entries.
              recoverOplog: true,
            }
        ];

        // If we find a usable optime that splits the checkpoint timestamp and the end of oplog,
        // add it as a scenario.
        let scenario = getAdditionalScenario(metadata, writerOneOpTimes, writerTwoOpTimes);
        if (scenario) {
            jsTestLog({"Additional scenario. Visible time": scenario});
            // The scenario Timestamp object is owned by one of the writerOpTimes arrays. Create a
            // copy for the `visibleTime`.
            scenarios.push({visibleTime: Timestamp(scenario.t, scenario.i), recoverOplog: true});
        }

        // Loop through each scenario. There are two loop invariants:
        //
        // 1) MongoD is not running on the `backupDir`.
        // 2) The oplog in the `backupDir` is >= the scenario's visibleTime.
        for (let scenario of scenarios) {
            jsTestLog({
                msg: "Asserting scenario.",
                backupDir: backupDir,
                visibleTime: scenario["visibleTime"],
                oplogRecovery: scenario["recoverOplog"],
            });

            let conn;
            if (scenario["recoverOplog"]) {
                // If we're recovering oplog, temporarily startup mongod to manipulate the
                // `oplogTruncateAfterPoint`.
                manipulateOplogTruncateAfterPoint(backupDir, scenario["visibleTime"]);

                // The `oplogTruncateAfterPoint` is inclusive. Using the `visibleTime` as the
                // `oplogTruncateAfterPoint` will delete the update at the `visibleTime`. Peel
                // back visibility by one tick to compensate. The `inc` field should always be >=
                // 1. Decrementing to 0 will suffice for visibility calculations. Fail if some
                // future version of MongoDB can generate times with an inc of 0 and deal with
                // underflow more thoroughly then.
                assert(scenario["visibleTime"].i >= 1);
                scenario["visibleTime"].i -= 1;
                jsTestLog({"Adjusted visibility time": scenario["visibleTime"]});

                conn = MongoRunner.runMongod({
                    dbpath: backupDir,
                    noCleanData: true,
                    setParameter: {recoverFromOplogAsStandalone: true}
                });
                assert.neq(conn, null);
            } else {
                conn = MongoRunner.runMongod({dbpath: backupDir, noCleanData: true});
                assert.neq(conn, null);
            }

            const writerOpTimes = scenario["opTimes"];
            const collFn = scenario["collFn"];

            // The clients do not write to the `postBackupCursor` collection until after they
            // causally observe the (last) backup cursor being opened. The backup data must
            // never contain these writes.
            for (let collFn of[getAlphaColl, getBetaColl]) {
                assert.eq(0, collFn(conn, postBackupCursor).count());
                assert.eq(0, collFn(conn, postBackupCursor).find().itcount());
            }

            // Assert the fine-grained control of the `oplogTruncateAfterPoint`. First assert
            // Alpha's ledger of writes, then Beta's.
            for (let collTest
                     of[{name: "Alpha", collFn: getAlphaColl, writerOpTimes: writerOneOpTimes},
                        {name: "Beta", collFn: getBetaColl, writerOpTimes: writerTwoOpTimes}]) {
                jsTestLog({"Asserting collection": collTest["name"]});
                let collFn = collTest["collFn"];
                let writerOpTimes = collTest["writerOpTimes"];
                for (let docOpTime of writerOpTimes) {
                    let doc = docOpTime["doc"];
                    let opTime = docOpTime["opTime"];

                    // Compare each sample write/operation time pair with the
                    // checkpointTimestamp. We expect visibilityTime's to be exact, thus we expect
                    // every sample to precisely be in or not in the dataset.
                    if (isVisible(opTime, scenario["visibleTime"])) {
                        assert.eq(1,
                                  collFn(conn, preBackupCursor).find({docNum: doc}).itcount(),
                                  "Expected document not found. Doc: " + doc + " OperationTime: " +
                                      opTime);
                    } else {
                        assert.eq(
                            0,
                            collFn(conn, preBackupCursor).find({docNum: doc}).itcount(),
                            "Unexpected document found. Doc: " + doc + " OperationTime: " + opTime);
                    }
                }
            }

            MongoRunner.stopMongod(conn, {noCleanData: true});
        }
    }

    assertData(primary.dbpath + "/primary-backup",
               primaryBackupMetadata,
               writerOneOpTimes,
               writerTwoOpTimes);
    assertData(secondary.dbpath + "/secondary-backup",
               secondaryBackupMetadata,
               writerOneOpTimes,
               writerTwoOpTimes);

    rst.stopSet(undefined, undefined);
})();
