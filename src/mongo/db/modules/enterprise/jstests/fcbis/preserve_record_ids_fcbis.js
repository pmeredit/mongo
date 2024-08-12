/**
 * Tests that when initial syncing using FCBIS for a collection with recordIdsReplicated:true, the
 * recordIds are preserved across the initial sync.
 * @tags: [requires_persistence, requires_wiredtiger, featureFlagRecordIdsReplicated]
 */
import {
    testPreservingRecordIdsDuringInitialSync,
} from "jstests/libs/replicated_record_ids_utils.js";

TestData.skipEnforceFastCountOnValidate = true;
testPreservingRecordIdsDuringInitialSync(/*initSyncMethod*/ "fileCopyBased",
                                         /*beforeCloningFP*/ "fCBISHangAfterOpeningBackupCursor",
                                         /*afterCloningFP*/ "fCBISHangAfterFileCloning");
