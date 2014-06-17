// This test verifies that one can read from secondaries through mongos when auditing
// is enabled.  This is a regression test for SERVER-14170.

(function () {
'use strict';

var st = new ShardingTest({
    name: 'audit_read_from_sharded_secondaries',
    keyFile: 'jstests/libs/key2',
    mongos: 1,
    rs: { nodes: 2 },
    shards: 1,
    config: 1,
    other: {
        rsOptions: {
            auditDestination: 'console'
        },
        configOptions: {
            auditDestination: 'console'
        },
        mongosOptions: {
            auditDestination: 'console'
        },
        enableBalancer: false,
        useHostname: true
    }
});

// Use a localhost connection to the MongoS to leverage the localhost exception to create an initial
// user.
var localS = new Mongo('localhost:' + st.s.port);
localS.getDB('admin').createUser({user: 'root', pwd: 'root', roles: ['root']});
localS = null;
authutil.assertAuthenticate(st.s, 'admin', { user: 'root', pwd: 'root' });

var test = st.s.getDB('test');
assert.writeOK(test.docs.insert({}, { writeConcern: { w: 2 } }));
assert.eq(test.docs.count(), 1);
test.getMongo().setReadPref('secondary');
try {
    assert.eq(test.docs.count(), 1);
}
finally {
    test.getMongo().setReadPref('primary');
}

}());

