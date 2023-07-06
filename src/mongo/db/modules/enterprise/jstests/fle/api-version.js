// Validate that apiVersion is correctly propagated by mongocryptd commands.
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {generateSchema} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

function runTestCmdVersion(db, cmd, apiVersion, apiDeprecationErrors, apiStrict) {
    if (apiVersion !== undefined) {
        cmd = Object.assign({}, cmd, {apiVersion: apiVersion});
    }

    if (apiDeprecationErrors !== undefined) {
        cmd = Object.assign({}, cmd, {apiDeprecationErrors: apiDeprecationErrors});
    }

    if (apiStrict !== undefined) {
        cmd = Object.assign({}, cmd, {apiStrict: apiStrict});
    }

    jsTest.log('Command: ' + tojson(cmd));
    const result = assert.commandWorked(db.runCommand(cmd)).result;
    jsTest.log('Result: ' + tojson(result));
    assert(result.apiVersion === cmd.apiVersion,
           tojson(result.apiVersion) + ' !== ' + tojson(cmd.apiVersion));
    assert(result.apiDeprecationErrors === cmd.apiDeprecationErrors,
           tojson(result.apiDeprecationErrors) + ' !== ' + tojson(cmd.apiDeprecationErrors));
    assert(result.apiStrict === cmd.apiStrict,
           tojson(result.apiStrict) + ' !== ' + tojson(cmd.apiStrict));
}

function runTestCmd(db, cmd) {
    runTestCmdVersion(db, cmd, undefined, undefined, undefined);
    runTestCmdVersion(db, cmd, '1', undefined, undefined);
    runTestCmdVersion(db, cmd, '1', true, false);
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
