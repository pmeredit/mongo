// Testing the output of OCSF process activity.
//
// Does not need to test all edge cases, that should be taken care
// of in the mongo audit event tests.

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kProcessActivityCategory = 1;
const kProcessActivityClass = 1007;
const kProcessActivityLaunch = 1;
const kProcessActivityTerminate = 2;
const kProcessActivityUnknown = 99;

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF process activity output on standalone");

    const {conn, audit, admin} =
        standaloneFixture.startProcess({"auditSchema": "OCSF"}, "JSON", "ocsf");

    let line =
        audit.assertEntry(kProcessActivityCategory, kProcessActivityClass, kProcessActivityLaunch);
    jsTest.log(line);

    assert.neq(line.unmapped.startup_options, null);
    assert.neq(line.unmapped.cluster_parameters, null);

    standaloneFixture.createUserAndAuth();

    assert.commandWorked(admin.runCommand({logApplicationMessage: "Hello World"}));

    line =
        audit.assertEntry(kProcessActivityCategory, kProcessActivityClass, kProcessActivityUnknown);
    jsTest.log(line);

    assert.eq(line.unmapped.msg, "Hello World");

    // We need this sleep in case the server startup happens so quickly that the log tries to
    // be rotated to the same name as an existing log archive file from the previous shutdown.
    sleep(2000);

    assert.commandWorked(admin.adminCommand({logRotate: 1}));
    audit.resetAuditLine();

    line =
        audit.assertEntry(kProcessActivityCategory, kProcessActivityClass, kProcessActivityUnknown);
    assert.eq(line.observables[0].name, "rotatedLogPath");
    jsTest.log(line);

    standaloneFixture.stopProcess();

    line = audit.assertEntry(
        kProcessActivityCategory, kProcessActivityClass, kProcessActivityTerminate);
    jsTest.log(line);
}