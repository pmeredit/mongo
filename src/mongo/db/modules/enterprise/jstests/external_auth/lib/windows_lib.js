// Utility functions used by mongo shell to verify logon information
function verifyAuth(db, user) {
    'use strict';

    const externalDB = db.getSiblingDB("$external");

    const status = externalDB.runCommand({"connectionStatus": 1});

    // The default user and role used for most of the tests
    const authInfo = {
        "authenticatedUsers": [{"user": user, "db": "$external"}],
    };

    print(tojson(authInfo));

    // Check that the user we tried to authenticate as was, in fact, authenticated.
    assert.eq(status.authInfo.authenticatedUsers,
              authInfo.authenticatedUsers,
              "unexpected authenticated users");
}
