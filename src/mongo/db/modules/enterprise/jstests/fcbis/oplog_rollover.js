/**
 * Test that oplog (on both primary and secondary) rolls over when its size exceeds the configured
 * maximum, when the secondary was initialized with FCBIS.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {oplogRolloverTest} from "jstests/replsets/libs/oplog_rollover_test.js";

TestData.skipEnforceFastCountOnValidate = true;
oplogRolloverTest("wiredTiger", "fileCopyBased");
