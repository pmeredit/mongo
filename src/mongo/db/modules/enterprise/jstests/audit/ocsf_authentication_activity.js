// Testing the output of OCSF authentication activity.
//
// Does not need to test all edge cases, that should be taken care
// of in the mongo audit event tests.

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kAuthenticateActivityCategory = 3;
const kAuthenticateActivityClass = 3002;
const kAuthenticateActivityLogon = 1;
const kAuthenticateActivityLogoff = 2;
const kTimeDiffBetweenAuthAndLogoutMS = 15 * 1000;
const kAcceptableRangeMS = 5 * 1000;
{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF authenticate activity output on standalone");

    const {conn, audit, admin} = standaloneFixture.startProcess(
        {"auditSchema": "OCSF", "setParameter": "featureFlagOCSF=true"}, "JSON", "ocsf");

    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
    assert(admin.auth("admin", "pwd"));

    let authLine = audit.assertEntry(
        kAuthenticateActivityCategory, kAuthenticateActivityClass, kAuthenticateActivityLogon);
    jsTest.log(authLine);

    assert.neq(authLine.user, null);
    assert.neq(authLine.unmapped, null);

    sleep(kTimeDiffBetweenAuthAndLogoutMS);
    admin.logout();
    let logoutLine = audit.assertEntry(
        kAuthenticateActivityCategory, kAuthenticateActivityClass, kAuthenticateActivityLogoff);
    jsTest.log(logoutLine);

    assert.neq(logoutLine.message, null);
    assert(authLine.hasOwnProperty("time") && logoutLine.hasOwnProperty("unmapped") &&
           logoutLine["unmapped"].hasOwnProperty("loginTime"));
    const authDate = new Date(authLine["time"]);
    const logoutDate = new Date(logoutLine["time"]);
    const loginDate = new Date(logoutLine["unmapped"]["loginTime"]);
    assert(Math.abs(loginDate - authDate) < kAcceptableRangeMS);
    assert(Math.abs(logoutDate - authDate - kTimeDiffBetweenAuthAndLogoutMS) < kAcceptableRangeMS);

    standaloneFixture.stopProcess();
}
