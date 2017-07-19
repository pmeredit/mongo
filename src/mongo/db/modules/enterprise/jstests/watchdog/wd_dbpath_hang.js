// Storage Node Watchdog test cases
//
load("src/mongo/db/modules/enterprise/jstests/watchdog/lib/charybdefs_lib.js");

(function() {
    'use strict';

    let control = new CharybdefsControl();

    const db_path = control.getMountPath() + "/db";

    function testMongoD() {
        // Now start MongoD with it enabled at startup
        //
        const conn = MongoRunner.runMongod({
            dbpath: db_path,
            setParameter: "watchdogPeriodSeconds=" + control.getWatchdogPeriodSeconds()
        });
        assert.neq(null, conn, 'mongod was unable to start up');

        // Wait for watchdog to get running
        const admin = conn.getDB("admin");

        // Wait for the watchdog to run some checks first
        control.waitForWatchdogToStart(admin);

        // Hang the file system
        control.addWriteDelayFaultAndWait("watchdog_probe.txt");

        // Check MongoD is dead by sending SIGTERM
        // This will trigger our "nice" shutdown, but since mongod is stuck in the kernel doing I/O,
        // the process will not terminate until charybdefs is done sleeping.
        print("Stopping MongoDB now, it will terminate once charybdefs is done sleeping.");
        MongoRunner.stopMongod(conn, undefined, {allowedExitCode: 61});
    }

    // Cleanup previous runs
    control.cleanup();

    try {
        // Start the file system
        control.start();

        // Create the data directory on fuse mount point.
        mkdir(db_path);

        testMongoD();
    } finally {
        control.cleanup();
    }

})();
