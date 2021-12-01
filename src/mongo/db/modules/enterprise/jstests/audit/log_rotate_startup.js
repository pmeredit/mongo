// Tests the functionality of rotate log on startup.

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

'use strict';

const kListeningOnID = 23015;

function testRotateLogsOnStartup(fixture) {
    {
        print("Testing functionality of rotating the audit log on startup.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user2", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        const options = conn.fullOptions;

        // We need to sleep to prevent race conditions where the audit spooler is still reading
        // the previous audit file and the name is not being moved to an existing audit archive
        // file from the previous rotate.
        sleep(2000);

        fixture.restartMainFixtureProcess(conn, options, false);

        // The rotate should happen on startup and we should not
        // see the previous auth user in the audit log
        audit.assertNoEntry("authenticate",
                            {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }

    // We need this sleep in case the server shutdown happens so quickly that the log tries to
    // be moved to the same name as an existing log archive file from the previous rotate.
    sleep(2000);
    {
        print(
            "Testing functionality of rotating the audit log on startup after an unclean shutdown.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user2", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        const options = conn.fullOptions;

        // We need to sleep to prevent race conditions where the audit spooler is still reading
        // the previous audit file and the name is not being moved to an existing audit archive
        // file from the previous rotate.
        sleep(2000);

        // Unclean shutdown is not being tested in ShardingFixture
        fixture.restartMainFixtureProcess(conn, options, true);

        // The rotate should happen on startup and we should not
        // see the previous auth user in the audit log
        audit.assertNoEntry("authenticate",
                            {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }
}

// /dir/blah.log -> /dir/blah.log.2021-12-03T00-29-32
function _generateRotatedFileNameFromLogPath(logPath, rotateDate) {
    return logPath + "." + rotateDate.getFullYear() + "-" +
        String(rotateDate.getMonth() + 1).padStart(2, '0') + "-" +
        String(rotateDate.getDate()).padStart(2, '0') + "T" +
        String(rotateDate.getHours()).padStart(2, '0') + "-" +
        String(rotateDate.getMinutes()).padStart(2, '0') + "-" +
        String(rotateDate.getSeconds()).padStart(2, '0');
}

function testRotateLogOnStartupFailureAbortsStartup(fixture, opts) {
    // The # of dummy files to generate for this test
    // This will give us a lead time of <numFiles> seconds for mongo[d,s] to start and
    // attempt to rotate the log file - hopefully plenty of time
    const numFiles = 30;

    const timestampStrings = new Array(numFiles);
    const now = new Date().getTime();
    const logPath = fixture.auditPath;

    // generate a bunch of dummy "rotated" log files to foil the on-start log rotation.
    for (let i = 0; i < numFiles; i++) {
        let nextSecond = new Date(now + (i * 1000));
        timestampStrings[i] = _generateRotatedFileNameFromLogPath(logPath, nextSecond);
    }

    // write the dummy files using shell cmd
    for (let f = 0; f < numFiles; f++) {
        writeFile(timestampStrings[f], "");
    }

    // the fixture should not startup due to filename collision with dummy files
    let err = {returnCode: -1};
    try {
        err = assert.throws(() => fixture.startProcess(opts),
                            [],
                            "Fixture failed when log rotation on startup failed");
        assert.eq(err.returnCode, 102);  // ExitCode::EXIT_AUDIT_ROTATE_ERROR
    } finally {
        // cleanup the dummy files regardless of outcome
        for (let f = 0; f < numFiles; f++) {
            removeFile(timestampStrings[f]);
        }
    }
}

// this fixture is not concerned with auditing, it is simply intended to test process
// startup or fail
class SimpleStandaloneMongosFixture {
    constructor() {
        this.logPathMongos = MongoRunner.dataPath + "mongos.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess() {
        let opts = {
            logpath: this.logPathMongod,
            auditDestination: "file",
            auditFormat: "JSON",
            auditPath: MongoRunner.dataPath + "audit.log",
            configdb: "something/127.0.01.1:20023"
        };

        const conn = MongoRunner.runMongos(opts);

        this.conn = conn;
        this.auditPath = opts.auditPath;
        this.logPath = this.conn.fullOptions.logpath;
        return {"conn": this.conn, "audit": this.audit};
    }

    stopProcess() {
        MongoRunner.stopMongos(this.conn);
    }
}

// this fixture is not concerned with auditing or clustering, it is simply intended
// to test process startup or fail
class SimpleStandaloneMongodFixture {
    constructor() {
        this.logPathMongos = MongoRunner.dataPath + "mongos.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess() {
        let opts = {
            logpath: this.logPathMongod,
            auditDestination: "file",
            auditFormat: "JSON",
            auditPath: MongoRunner.dataPath + "audit.log",
        };

        const conn = MongoRunner.runMongod(opts);

        this.conn = conn;
        this.auditPath = opts.auditPath;
        this.logPath = this.conn.fullOptions.logpath;
        return {"conn": this.conn, "audit": this.audit};
    }

    stopProcess() {
        MongoRunner.stopMongod(this.conn);
    }
}

print("Testing functionality of rotating both logs.");

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing rotate functionality on standalone");
    testRotateLogsOnStartup(standaloneFixture);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing rotate functionality on sharded cluster");

    testRotateLogsOnStartup(shardingFixture);
}

sleep(1000);

{
    jsTest.log("Testing that mongod audit file rotation failure on startup terminates startup");

    const fixture = new SimpleStandaloneMongodFixture();

    testRotateLogOnStartupFailureAbortsStartup(fixture);
}

sleep(1000);

{
    jsTest.log("Testing that mongos audit file rotation failure on startup terminates startup");

    const fixture = new SimpleStandaloneMongosFixture();

    testRotateLogOnStartupFailureAbortsStartup(fixture);
}
})();
