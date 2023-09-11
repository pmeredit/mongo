// Testing the output of OCSF authentication activity.
//
// Does not need to test all edge cases, that should be taken care
// of in the mongo audit event tests.

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kAuthenticateActivityCategory = 3;
const kAuthenticateActivityClass = 3002;
const kAuthenticateActivityLogon = 1;
const kAuthenticateActivityLogoff = 2;

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF authenticate activity output on standalone");

    const {conn, audit, admin} = standaloneFixture.startProcess(
        {"auditSchema": "OCSF", "setParameter": "featureFlagOCSF=true"}, "JSON", "ocsf");

    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
    assert(admin.auth("admin", "pwd"));

    {
        let line = audit.assertEntry(
            kAuthenticateActivityCategory, kAuthenticateActivityClass, kAuthenticateActivityLogon);
        jsTest.log(line);

        assert.neq(line.user, null);
        assert.neq(line.unmapped, null);
    }

    admin.logout();

    {
        let line = audit.assertEntry(
            kAuthenticateActivityCategory, kAuthenticateActivityClass, kAuthenticateActivityLogoff);
        jsTest.log(line);

        assert.neq(line.message, null);
    }

    standaloneFixture.stopProcess();
}