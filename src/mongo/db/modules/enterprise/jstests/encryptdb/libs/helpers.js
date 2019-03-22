// This contains some helper routines and variables that get used across several different
// ESE tests

const hostInfo = function() {
    const md = MongoRunner.runMongod({});
    assert.neq(null, md, "Failed to start mongod to probe host type");
    const db = md.getDB("test");
    const hostInfo = db.hostInfo();
    MongoRunner.stopMongod(md);
    return hostInfo;
}();
const isOSX = (hostInfo.os.type == "Darwin");
const isWindowsSchannel =
    (hostInfo.os.type == "Windows" && /SChannel/.test(buildInfo().openssl.running));

const platformSupportsGCM = !(isOSX || isWindowsSchannel);
