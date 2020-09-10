/**
 * Validate that hello and its aliases, ismaster and isMaster, are all accepted
 * by mongocryptd and that the appropriate response fields are returned.
 */
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
'use strict';

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();

function checkResponseFields(commandString) {
    jsTestLog("Testing " + commandString + " for MongoCryptD");
    const commandResponse = assert.commandWorked(conn.adminCommand(commandString));
    assert.eq(commandResponse.iscryptd,
              true,
              "iscryptd is not true, command response: " + tojson(commandResponse));

    if (commandString === "hello") {
        assert.eq(commandResponse.ismaster,
                  undefined,
                  "ismaster is not undefined, command response: " + tojson(commandResponse));
        assert.eq(commandResponse.isWritablePrimary,
                  true,
                  "isWritablePrimary is not true, command response: " + tojson(commandResponse));
    } else {
        assert.eq(commandResponse.ismaster,
                  true,
                  "ismaster is not true, command response: " + tojson(commandResponse));
        assert.eq(
            commandResponse.isWritablePrimary,
            undefined,
            "isWritablePrimary is not undefined, command response: " + tojson(commandResponse));
    }
}

checkResponseFields("ismaster");
checkResponseFields("isMaster");
checkResponseFields("hello");

mongocryptd.stop();
})();
