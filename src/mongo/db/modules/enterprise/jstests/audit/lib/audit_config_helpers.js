// Helper library for testing audit config

export const kDefaultLogicalTime = {
    "$timestamp": {t: 0, i: 0}
};

export const kDefaultParameterConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    clusterParameterTime: kDefaultLogicalTime
};

export const kDefaultDirectConfig = {
    filter: {},
    auditAuthorizationSuccess: false,
    generation: ObjectId("000000000000000000000000"),
};

export function assertSameTimestamp(a, b) {
    if (a === undefined) {
        assert.eq(b, undefined, "Objects are inequal: undefined != " + tojson(b));
        return;
    }
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

export function assertSameOID(a, b) {
    if (a === undefined) {
        assert.eq(b, undefined, "Objects are inequal: undefined != " + tojson(b));
        return;
    }
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

export function findAllWithMajority(db, collName, filter) {
    return new DBCommandCursor(
               db,
               assert.commandWorked(db.runCommand(
                   {find: collName, filter: filter, readConcern: {level: "majority"}})),
               1 /* batchsize */)
        .toArray();
}
