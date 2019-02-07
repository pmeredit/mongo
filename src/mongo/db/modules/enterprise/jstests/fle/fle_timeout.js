// Validate fle shuts down after timeout
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    // Set idle timeout to 5 seconds
    mongocryptd.start(5);

    const conn = mongocryptd.getConnection();

    // Close the only connection
    conn.close();

    // Wait for it to shutdown
    sleep(10 * 1000);

    const code = mongocryptd.stop();
    print("Exit Code: " + code);
    assert.eq(code, 12);
})();
