// Tests the errors returned when logRotate command fails

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function testSubsecondRotations() {
    jsTest.log("Test two logRotate commands invoked within one second causes file rename failure");
    const fixture = new StandaloneFixture();
    const {conn, audit, admin} = fixture.startProcess();
    fixture.createUserAndAuth();
    sleep(2000);

    assert.commandWorked(admin.adminCommand({logRotate: 1}));

    let result = undefined;

    // use assert.soon because we want to keep retrying the command
    // in case the succeeding command did not arrive in the same
    // second as the previous command
    assert.soon(() => {
        result = admin.adminCommand({logRotate: 1});
        return result.ok === 0;
    });
    assert.eq(result.code, ErrorCodes.FileRenameFailed);

    // verify the combined minor error message is logged
    assert(checkLog.checkContainsOnceJson(conn, 6221501));

    fixture.stopProcess();
    sleep(1000);
}

function testPermissionDeniedOnRotation() {
    function isRunningAsRoot() {
        clearRawMongoProgramOutput();
        const rc = runProgram("id", "-un");
        assert.eq(0, rc);
        const output = rawMongoProgramOutput(".*");
        return output.trim().search("root") !== -1;
    }

    jsTest.log("Test logRotate command fails with permission denied");
    if (_isWindows() || isRunningAsRoot()) {
        jsTest.log("Test skipped");
        return;
    }
    const fixture = new StandaloneFixture();
    const {conn, audit, admin} = fixture.startProcess();
    fixture.createUserAndAuth();
    sleep(2000);

    // remove write perms on the directory containing the log files
    const rc = runProgram("chmod", "ug-w", MongoRunner.dataPath);
    assert.eq(0, rc);

    const result = admin.adminCommand({logRotate: 1});
    // restore write perms
    runProgram("chmod", "ug+w", MongoRunner.dataPath);

    assert.eq(result.ok, 0);
    assert.eq(result.code, ErrorCodes.FileRenameFailed);

    // there should be 2 "Permission denied" messages logged,
    // one for mongod.log and the other for audit.log
    assert(checkLog.checkContainsWithCountJson(conn, 23168, undefined, 2));

    fixture.stopProcess();
    sleep(1000);
}

function testPermissionDeniedOnAuditRotation() {
    function isRunningAsRoot() {
        clearRawMongoProgramOutput();
        const rc = runProgram("id", "-un");
        assert.eq(0, rc);
        const output = rawMongoProgramOutput(".*");
        return output.trim().search("root") !== -1;
    }

    jsTest.log("Test audit logRotate command fails with permission denied");
    if (_isWindows() || isRunningAsRoot()) {
        jsTest.log("Test skipped");
        return;
    }
    const fixture = new StandaloneFixture();
    const {conn, audit, admin} = fixture.startProcess();
    fixture.createUserAndAuth();
    sleep(2000);
    audit.fastForward();

    // remove write perms on the directory containing the log files
    const rc = runProgram("chmod", "ug-w", MongoRunner.dataPath);
    assert.eq(0, rc);

    const result = admin.adminCommand({logRotate: "audit"});

    const auditRotateLogEntry = audit.assertEntry("rotateLog");

    assert.neq(auditRotateLogEntry.param.logRotationStatus.status, "OK");

    // restore write perms
    runProgram("chmod", "ug+w", MongoRunner.dataPath);

    assert.eq(result.ok, 0);
    assert.eq(result.code, ErrorCodes.FileRenameFailed);

    // there should be 2 "Permission denied" messages logged,
    // one for mongod.log and the other for audit.log
    // assert(checkLog.checkContainsWithCountJson(conn, 23168, undefined, 1));

    fixture.stopProcess();
    sleep(1000);
}

function testLogFileDeletedBeforeRotation() {
    jsTest.log("Test logRotate command with deleted source file");
    const fixture = new StandaloneFixture();
    const {conn, audit, admin} = fixture.startProcess();
    fixture.createUserAndAuth();
    sleep(2000);

    const logPath = MongoRunner.dataPath + "mongod.log";
    const auditPath = MongoRunner.dataPath + "audit.log";
    removeFile(logPath);
    removeFile(auditPath);

    const result = admin.adminCommand({logRotate: 1});

    assert.eq(result.ok, 0);
    assert.eq(result.code, ErrorCodes.FileRenameFailed);

    assert(checkLog.checkContainsOnceJson(conn, 6221501));

    fixture.stopProcess();
    sleep(1000);
}

function testBadLogTypeInCommand() {
    jsTest.log("Test logRotate command with invalid logType string");
    const fixture = new StandaloneFixture();
    const {conn, audit, admin} = fixture.startProcess();
    fixture.createUserAndAuth();
    sleep(2000);

    const result = admin.adminCommand({logRotate: "foo"});
    assert.eq(result.ok, 0);
    assert.eq(result.code, ErrorCodes.NoSuchKey);

    // verify the logs contain "Unknown log type..." error message
    assert(checkLog.checkContainsOnceJson(conn, 6221500));
    fixture.stopProcess();
    sleep(1000);
}

{
    runProgram("chmod", "ug+w", MongoRunner.dataPath);
    testSubsecondRotations();
    testPermissionDeniedOnRotation();
    testPermissionDeniedOnAuditRotation();
    testLogFileDeletedBeforeRotation();
    testBadLogTypeInCommand();
}
