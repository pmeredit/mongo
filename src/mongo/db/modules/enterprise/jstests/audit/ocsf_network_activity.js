// Testing the output of OCSF network activity.
//
// Does not need to test all edge cases, that should be taken care
// of in the mongo audit event tests.

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kNetworkActivityCategory = 4;
const kNetworkActivityClass = 4001;
const kNetworkActivityId = 99;

{
    const standaloneFixture = new StandaloneFixture();
    jsTest.log("Testing OCSF authenticate activity output on standalone");

    const {conn, audit, admin} =
        standaloneFixture.startProcess({"auditSchema": "OCSF"}, "JSON", "ocsf");

    audit.fastForward();

    let newConn = new Mongo(`mongodb://${conn.host}/?appName=foo`);

    {
        let line =
            audit.assertEntry(kNetworkActivityCategory, kNetworkActivityClass, kNetworkActivityId);
        jsTest.log(line);

        assert.neq(line.unmapped, null);

        assert(line.unmapped.hasOwnProperty("application"));
        assert.eq(line.unmapped.application.name, "foo");
    }

    standaloneFixture.stopProcess();
}