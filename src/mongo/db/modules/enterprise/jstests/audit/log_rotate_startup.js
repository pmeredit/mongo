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

function testRotateAuditLogOnStartupWithNoLogFilePresent(fixture) {
    print("Testing logging of log rotation event on startup without existing log file");
    const auditLogPath = fixture.auditPath;
    // Check if audit log file exists
    // If it does, remove it
    if (pathExists(auditLogPath)) {
        removeFile(auditLogPath);
    }
    // start mongod process
    const {conn, audit, admin} = fixture.startProcess();
    // check that audit log does not contain a log rotation event
    audit.assertNoEntry("rotateLog");

    fixture.stopProcess();
}

function testRotateAuditLogOnStartupWithLogFilePresent(fixture) {
    print("Testing logging of log rotation event on startup with existing log file");

    const logPath = fixture.auditPath;

    if (!pathExists(logPath)) {
        writeFile(logPath, "");
        print("Wrote file to " + logPath +
              " as it was not present and required for log rotation at startup");
    }

    const {conn, audit, admin} = fixture.startProcess();
    // Ensure that log contains a log rotation event
    audit.assertEntry("rotateLog");

    fixture.stopProcess();
}

function testRotateLogOnStartupFailureAbortsStartup(fixture) {
    const logPath = fixture.auditPath;

    if (!pathExists(logPath)) {
        writeFile(logPath, "");
        print("Wrote file to " + logPath +
              " as it was not present and required for rotation at startup.");
    }

    const opts = {setParameter: "failpoint.auditLogRotateFileExists={mode:'alwaysOn'}"};

    // the fixture should not startup due to filename collision with dummy files
    let err = {returnCode: -1};
    try {
        err = assert.throws(() => fixture.startProcess(opts),
                            [],
                            "Fixture failed when log rotation on startup failed");
    } catch (ex) {
        // if the test fails, the server is going to start, make sure to try and shut it down
        fixture.stopProcess();
    }
    assert.eq(err.returnCode,
              MongoRunner.EXIT_AUDIT_ROTATE_ERROR);  // See ExitCode::auditRotateError
}

// this fixture is not concerned with auditing, it is simply intended to test process
// startup or fail
class SimpleStandaloneMongosFixture {
    constructor() {
        this.logPathMongos = MongoRunner.dataPath + "mongos.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess(opts) {
        opts.logpath = this.logPathMongos;
        opts.auditDestination = "file";
        opts.auditFormat = "JSON";
        opts.auditPath = MongoRunner.dataPath + "audit.log";
        opts.configdb = "something/127.0.01.1:20023";

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
        this.logPathMongod = MongoRunner.dataPath + "mongod.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess(opts) {
        opts.logpath = this.logPathMongod;
        opts.auditDestination = "file";
        opts.auditFormat = "JSON";
        opts.auditPath = MongoRunner.dataPath + "audit.log";

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

sleep(1000);

{
    jsTest.log(
        "Testing that no rotateLog event is triggered on startup if audit log path does not exist");

    const fixture = new StandaloneFixture();
    testRotateAuditLogOnStartupWithNoLogFilePresent(fixture);
}

sleep(1000);

{
    jsTest.log("Testing that rotateLog event is triggered on startup if audit log path exists");

    const fixture = new StandaloneFixture();
    testRotateAuditLogOnStartupWithLogFilePresent(fixture);
}
})();
