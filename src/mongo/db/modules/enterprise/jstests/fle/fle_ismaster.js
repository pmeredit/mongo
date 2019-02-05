// Validate fle is master says it is cryptd
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const isMaster = assert.commandWorked(conn.adminCommand({isMaster: 1}));

    assert.eq(isMaster.iscryptd, true);
    assert.eq(isMaster.ismaster, true);

    mongocryptd.stop();
})();
