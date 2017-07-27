// Storage Node Watchdog - validate --dbpath
//
load("src/mongo/db/modules/enterprise/jstests/watchdog/lib/wd_test_common.js");

(function() {
    'use strict';

    let control = new CharybdefsControl("dbpath_hang");

    const dbPath = control.getMountPath() + "/db";

    testFuseAndMongoD(control, {dbpath: dbPath});

})();
