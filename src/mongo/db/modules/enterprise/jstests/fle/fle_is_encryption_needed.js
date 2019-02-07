// Validate isEncryptionNeeded works
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const ret = assert.commandWorked(conn.adminCommand(
        {isEncryptionNeeded: 1, jsonSchema: {properties: {foo: {type: "string"}}}}));

    assert.eq(ret.requiresEncryption, false);

    mongocryptd.stop();
})();
