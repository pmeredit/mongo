/**
 * Test $lookup involving multiple encrypted collections with CSFLE end to end on a 6.0 mongodb
 * binary.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_standalone_mongod,
 *   fle2_no_mongos
 * ]
 */
import {isEnterpriseShell} from "jstests/fle2/libs/encrypted_client_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    execTest
} from
    "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/fle_lookup_multi_schema_end_to_end_utils.js";

if (!isEnterpriseShell()) {
    jsTestLog("Skipping test as it requires the enterprise module");
    quit();
}

const rst = new ReplSetTest({nodes: 1, nodeOptions: {binVersion: "6.0"}});
rst.startSet();

rst.initiate();
rst.awaitReplication();
// We only test CSFLE for multi-version scenarios.
execTest(rst.getPrimary(), false);
rst.stopSet();
