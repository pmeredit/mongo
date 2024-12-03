/**
 * Test that if the KMIP responder has a certificate with an
 * OCSP responder, and we are adding a new node to the set starting
 * with FCBIS, we can still proceed with FCBIS.
 *
 * @tags: [requires_fcv_60, requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {MockOCSPServer} from "jstests/ocsp/lib/mock_ocsp.js";
import {
    OCSP_CA_PEM,
    OCSP_CLIENT_CERT,
    OCSP_SERVER_CERT,
} from "jstests/ocsp/lib/ocsp_helpers.js";
import {
    killPyKMIPServer,
    startPyKMIPServer
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const kmipServerPort = 6590;
let kmipServerPid = startPyKMIPServer(kmipServerPort, false, OCSP_SERVER_CERT, OCSP_CA_PEM);

let mock_ocsp = new MockOCSPServer("", 1);
mock_ocsp.start();

const testName = TestData.testName;

const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        enableEncryption: "",
        kmipServerName: "127.0.0.1",
        kmipPort: kmipServerPort,
        kmipServerCAFile: OCSP_CA_PEM,
        tlsAllowInvalidHostnames: "",
        kmipActivateKeys: false,
        kmipClientCertificateFile: OCSP_CLIENT_CERT,
    }]
});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
rst.awaitReplication();

// Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    enableEncryption: "",
    kmipServerName: "127.0.0.1",
    kmipPort: kmipServerPort,
    kmipServerCAFile: OCSP_CA_PEM,
    tlsAllowInvalidHostnames: "",
    kmipActivateKeys: false,
    kmipClientCertificateFile: OCSP_CLIENT_CERT,
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({
            replication: {verbosity: 1, initialSync: 2},
            control: 1,
        }),
    }
});
rst.reInitiate();
// We use this assert instead of waitForSecondary because we want errors due to the node
// crashing to fail, not timeout.
assert.soon(() => initialSyncNode.adminCommand({hello: 1}).secondary);
rst.stopSet();

mock_ocsp.stop();

killPyKMIPServer(kmipServerPid);
