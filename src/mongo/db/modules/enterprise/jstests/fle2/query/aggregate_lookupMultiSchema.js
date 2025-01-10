/**
 * End to end test for $lookup involving multiple encrypted collections.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_81
 * ]
 */
import {isEnterpriseShell} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    execTest
} from
    "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/fle_lookup_multi_schema_end_to_end_utils.js";

if (!isEnterpriseShell()) {
    jsTestLog("Skipping test as it requires the enterprise module");
    quit();
}

// Test CSFLE
execTest(db.getMongo(), false);
// Test FLE2
execTest(db.getMongo(), true);
