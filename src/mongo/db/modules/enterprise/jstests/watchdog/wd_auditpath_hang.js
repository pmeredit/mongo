// Storage Node Watchdog - validate watchdog monitors --auditpath
//
load("src/mongo/db/modules/enterprise/jstests/watchdog/lib/wd_test_common.js");

(function() {
    'use strict';

    let control = new CharybdefsControl("auditpath_hang");

    const auditPath = control.getMountPath();

    testFuseAndMongoD(control, {

        auditDestination: 'file',
        auditFormat: 'JSON',
        auditPath: auditPath + "/auditLog.json"
    });

})();
