// Test setting special values for ldapTimeoutMS (e.g., NaN, out-of-bound doubles)

(function() {
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js");

const testOptions = {
    isPooled: false,
};

function checkRuntimeTimeoutValues({conn, testObj, options}) {
    const adminDB = conn.getDB("admin");
    assert(adminDB.auth("siteRootAdmin", "secret"));

    assert.commandFailed(conn.adminCommand({setParameter: 1, "ldapTimeoutMS": NaN}));
    assert.commandFailed(conn.adminCommand({setParameter: 1, "ldapTimeoutMS": "hi"}));
    assert.commandFailed(conn.adminCommand({setParameter: 1, "ldapTimeoutMS": (0 / 0)}));
    assert.commandFailed(
        conn.adminCommand({setParameter: 1, "ldapTimeoutMS": (Number.MAX_SAFE_INTEGER)}));
    assert.commandFailed(
        conn.adminCommand({setParameter: 1, "ldapTimeoutMS": (Number.MIN_SAFE_INTEGER)}));
    assert.commandFailed(conn.adminCommand({setParameter: 1, "ldapTimeoutMS": 0}));

    // Valid numbers should be accepted for new timeouts.
    assert.commandWorked(conn.adminCommand({setParameter: 1, "ldapTimeoutMS": 50000}));
}

runTimeoutTest(checkRuntimeTimeoutValues, testOptions);
}());
