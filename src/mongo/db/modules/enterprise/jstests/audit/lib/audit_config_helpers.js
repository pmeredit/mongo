// Helper library for testing audit config

'use strict';

const kDefaultLogicalTime = {
    "$timestamp": {t: 0, i: 0}
};

const kDefaultParameterConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    clusterParameterTime: kDefaultLogicalTime
};

const kDefaultDirectConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    generation: ObjectId("000000000000000000000000"),
};

function assertSameTimestamp(a, b) {
    if (!(a instanceof Timestamp) && (a['$timestamp'] === undefined)) {
        assert(false, tojson(a) + ' is not a Timestamp');
    }

    if (!(b instanceof Timestamp) && (b['$timestamp'] === undefined)) {
        assert(false, tojson(b) + ' is not a Timestamp');
    }

    // Normalize {'$timestamp':...} to a Timestamp object.
    const ats = (a instanceof Timestamp) ? a : Timestamp(a['$timestamp'].t, a['$timestamp'].i);
    const bts = (b instanceof Timestamp) ? b : Timestamp(b['$timestamp'].t, b['$timestamp'].i);
    assert.eq(bsonWoCompare(ats, bts), 0, "Objects are inequal: " + tojson(a) + " != " + tojson(b));
}

function assertSameOID(a, b) {
    if (!(a instanceof ObjectId) && (a['$oid'] === undefined)) {
        assert(false, tojson(a) + ' is not an ObjectId');
    }

    if (!(b instanceof ObjectId) && (b['$oid'] === undefined)) {
        assert(false, tojson(b) + ' is not an ObjectId');
    }

    // Normalize ObjectId or {'$oid':...} to a hex string.
    const astr = (a instanceof ObjectId) ? a.valueOf() : a['$oid'];
    const bstr = (b instanceof ObjectId) ? b.valueOf() : b['$oid'];
    assert.eq(astr, bstr, "Objects are inequal: " + tojson(a) + " != " + tojson(b));
}
