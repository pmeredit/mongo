// Tests the expected number of log files are created on startup
import {
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function testNumberOfLogsOnStartup(fixture, logDir, auditLogName, serverLogName) {
    function assertFileCount(expected) {
        const files = listFiles(logDir);
        const auditFiles = files.filter(f => !f.isDirectory && f.baseName.startsWith(auditLogName));
        assert.eq(auditFiles.length,
                  expected,
                  'Expected ' + expected + ' audit log files, found: ' + tojson(auditFiles));
        const logFiles = files.filter(f => !f.isDirectory && f.baseName.startsWith(serverLogName));
        assert.eq(logFiles.length,
                  expected,
                  'Expected ' + expected + ' server log files, found: ' + tojson(logFiles));
    }

    print("Checking number of log files after initial startup.");

    assert(mkdir(logDir).created);

    const {conn, audit, admin} = fixture.startProcess();
    assertFileCount(1);

    // We need to sleep to prevent race conditions where the audit spooler is still reading
    // the previous audit file and the name is not being moved to an existing audit archive
    // file from the previous rotate.
    sleep(2000);

    print("Checking number of log files after restart.");

    const options = conn.fullOptions;
    fixture.restartMainFixtureProcess(conn, options, false);

    assertFileCount(2);

    fixture.stopProcess();
    sleep(2000);
}

{
    const fixture = new StandaloneFixture();
    const logDir = MongoRunner.dataPath + "standalone/";
    fixture.auditPath = logDir + "audit.log";
    fixture.logPathMongod = logDir + "mongod.log";

    jsTest.log("Testing standalone");
    testNumberOfLogsOnStartup(fixture, logDir, "audit.log", "mongod.log");
}

{
    const fixture = new ShardingFixture();
    const logDir = MongoRunner.dataPath + "sharded/";
    fixture.auditPath = logDir + "audit.log";
    fixture.logPathMongos = logDir + "mongos.log";

    jsTest.log("Testing sharded cluster");
    testNumberOfLogsOnStartup(fixture, logDir, "audit.log", "mongos.log");
}
