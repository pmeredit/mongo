// This contains some helper routines and variables that get used across several different
// ESE tests
import {getPython3Binary} from "jstests/libs/python.js";

export const hostInfo = function() {
    const md = MongoRunner.runMongod({});
    assert.neq(null, md, "Failed to start mongod to probe host type");
    const db = md.getDB("test");
    const hostInfo = db.hostInfo();
    MongoRunner.stopMongod(md);
    return hostInfo;
}();

export const isOSX = (hostInfo.os.type == "Darwin");

export const isWindowsSchannel =
    (hostInfo.os.type == "Windows" && /SChannel/.test(buildInfo().openssl.running));

export const platformSupportsGCM = !(isOSX || isWindowsSchannel);
export const kmipPyPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/";

// Starts a PyKMIP server on the given port and returns the UID.
export function startPyKMIPServer(port, useLegacyProtocol = false) {
    clearRawMongoProgramOutput();
    const kmipServerPid = _startMongoProgram(getPython3Binary(),
                                             kmipPyPath + "kmip_server.py",
                                             "--version",
                                             useLegacyProtocol ? "1.0" : "1.2",
                                             port);
    // Assert here that PyKMIP is compatible with the default Python version
    assert(checkProgram(kmipServerPid));
    // wait for the PyKMIP server to be ready
    assert.soon(() => rawMongoProgramOutput(".*").search("Starting connection service") !== -1);
    return kmipServerPid;
}

// Given the port of a KMIP server, runs a PyKMIP script which creates and activates a new symmetric
// key, and returns its UID.
export function createPyKMIPKey(kmipServerPort, useLegacyProtocol = false) {
    clearRawMongoProgramOutput();
    const pid = _startMongoProgram(getPython3Binary(),
                                   kmipPyPath + "kmip_manage_key.py",
                                   "--kmipPort",
                                   kmipServerPort,
                                   "--version",
                                   useLegacyProtocol ? "1.0" : "1.2",
                                   "create_key");
    let uid;
    assert.soon(() => {
        const output = rawMongoProgramOutput(".*");
        // Wait for the UID to be output
        let idx = output.search("UID=");
        if (idx === -1) {
            return false;
        }
        const baseidx = idx + 5;  // skip past UID=<_
        const uidlen = output.substring(baseidx).search(">");
        if (uidlen === -1) {
            return false;
        }
        uid = output.substring(baseidx, baseidx + uidlen);
        return true;
    });

    waitProgram(pid);

    return uid;
}

// Given the port of a KMIP server, runs a PyKMIP script which checks the key specified by the UID
// is in the activated state.
export function isPyKMIPKeyActive(kmipServerPort, uid) {
    clearRawMongoProgramOutput();
    const pid = _startMongoProgram(getPython3Binary(),
                                   kmipPyPath + "kmip_manage_key.py",
                                   "--kmipPort",
                                   kmipServerPort,
                                   "get_state_attribute",
                                   "--uid",
                                   uid);
    let isActive;
    assert.soon(() => {
        const output = rawMongoProgramOutput(".*");

        let isActiveOutput = output.match(/IS_ACTIVE=<(.*)>/);
        if (isActiveOutput !== null) {
            isActive = isActiveOutput[1];
            return true;
        }

        return false;
    });

    waitProgram(pid);

    return (isActive == "True");
}

export function activatePyKMIPKey(kmipServerPort, uid, useLegacyProtocol = false) {
    clearRawMongoProgramOutput();

    let pid = _startMongoProgram(getPython3Binary(),
                                 kmipPyPath + "kmip_manage_key.py",
                                 "--kmipPort",
                                 kmipServerPort,
                                 "--version",
                                 useLegacyProtocol ? "1.0" : "1.2",
                                 "activate_key",
                                 "--uid",
                                 uid);

    assert.soon(() => {
        return rawMongoProgramOutput(".*").search("Successfully activated KMIP Key") !== -1;
    });

    waitProgram(pid);
}

export function deactivatePyKMIPKey(kmipServerPort, uid) {
    clearRawMongoProgramOutput();

    let pid = _startMongoProgram(getPython3Binary(),
                                 kmipPyPath + "kmip_manage_key.py",
                                 "--kmipPort",
                                 kmipServerPort,
                                 "deactivate_kmip_key",
                                 "--uid",
                                 uid);

    assert.soon(() => {
        return rawMongoProgramOutput(".*").search("Successfully Deactivated KMIP Key") !== -1;
    });

    waitProgram(pid);
}

export function killPyKMIPServer(pid) {
    if (_isWindows()) {
        // we use taskkill because we need to kill children
        waitProgram(_startMongoProgram("taskkill", "/F", "/T", "/PID", pid));
        // waitProgram to ignore error code
        waitProgram(pid);
    } else {
        let kSIGINT = 2;
        stopMongoProgramByPid(pid, kSIGINT);
    }
}
