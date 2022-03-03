// Validate that apiVersion is correctly propagated by mongocryptd commands.
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

function runTestCmdVersion(db, cmd, apiVersion) {
    if (apiVersion !== undefined) {
        cmd = Object.assign({}, cmd, {apiVersion: apiVersion});
    }
    jsTest.log('Command: ' + tojson(cmd));
    const result = assert.commandWorked(db.runCommand(cmd)).result;
    jsTest.log('Result: ' + tojson(result));
    assert(result.apiVersion === cmd.apiVersion,
           tojson(result.apiVersion) + ' !== ' + tojson(cmd.apiVersion));
}

function runTestCmd(db, cmd) {
    runTestCmdVersion(db, cmd, undefined);
    runTestCmdVersion(db, cmd, '1');
}

function runTest(db) {
    const findBase = {
        "find": "default",
        "filter": {"encrypted_string": "string1"},
    };

    // find
    runTestCmd(db, Object.assign({}, findBase, generateSchema({}, "test.default")));

    // explain
    runTestCmd(db, Object.assign({explain: findBase}, generateSchema({}, "test.default")));
}

{
    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const test = conn.getDB("test");

    runTest(test);

    mongocryptd.stop();
}
})();
