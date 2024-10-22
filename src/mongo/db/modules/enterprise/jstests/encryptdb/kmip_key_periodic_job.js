// This test checks that when the KMIP key is in a state other than active,
// we shut down the node.
// It assumes that PyKMIP is installed
// @tags: [uses_pykmip, incompatible_with_s390x]

import {findMatchingLogLine} from "jstests/libs/log.js";
import {
    deactivatePyKMIPKey,
    killPyKMIPServer,
    startPyKMIPServer
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const kmipServerPort = 6570;

function setUpTest() {
    let kmipServerPid = startPyKMIPServer(kmipServerPort);
    clearRawMongoProgramOutput();
    let defaultOpts = {
        enableEncryption: "",
        kmipServerName: "127.0.0.1",
        kmipPort: kmipServerPort,
        kmipServerCAFile: "jstests/libs/trusted-ca.pem",
        kmipKeyStatePollingSeconds: 5,
        encryptionCipherMode: "AES256-CBC",
        // We need the trusted-client certificate file in order to avoid permission issues when
        // getting the state attribute from the newly created key.
        kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
    };

    const md = MongoRunner.runMongod(defaultOpts);
    const adminDB = md.getDB('admin');

    // Get the keyId of the newly created key from the logs.
    // The log reads: {..."msg":"Created KMIP key","attr":{"keyId":"1"}}
    let log = assert.commandWorked(adminDB.adminCommand({getLog: "global"})).log;
    let line = findMatchingLogLine(log, {id: 24199, msg: "Created KMIP key"});
    assert(line, "Failed to find a log line matching the message");

    let keyId = JSON.parse(line).attr.keyId;
    jsTest.log('Key ID found: ' + keyId);

    return {keyId, kmipServerPid, md};
}

print("Testing that server shuts down when key is deactivated.");
{
    const {keyId, kmipServerPid, md} = setUpTest();

    deactivatePyKMIPKey(kmipServerPort, keyId);
    sleep(10000);

    assert.soon(() => {
        return rawMongoProgramOutput(".*").search(
                   "KMIP Key used for ESE is not in active state. Shutting down server.") >= 0;
    });

    killPyKMIPServer(kmipServerPid);
}

print("Successfully verified that server shuts down when key is deactivated.");
print("Testing that server does not shut down when periodic job cannot reach KMIP Server.");
{
    const {keyId, kmipServerPid, md} = setUpTest();

    killPyKMIPServer(kmipServerPid);

    sleep(10000);

    checkLog.containsJson(md, 4250502);

    MongoRunner.stopMongod(md);
}
print(
    "Successfully verified that server does not shut down when periodic job cannot reach KMIP Server.");
