// Verify internal operations which run large commands succeed with auditing

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');

    let m = MongoRunner.runMongodAuditLogger({setParameter: "auditAuthorizationSuccess=true"});
    let audit = m.auditSpooler();
    let db = m.getDB("test");
    let coll = db.server36070;

    assert.writeOK(coll.insert({a: "a".repeat(15500000)}));

    var bulk = coll.initializeUnorderedBulkOp();
    for (var i = 0; i < 50000; i++) {
        bulk.insert({a: "a"});
    }
    assert.writeOK(bulk.execute());

    coll.aggregate([{$match: {}}, {$out: "22server36070"}], {allowDiskUse: true});

    MongoRunner.stopMongod(m);
})();
