/* This tests that index builds on the ESE succeed, even when forced to resume.
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 * ]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest, ResumableIndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";
import {
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
const dbName = "test";
const key = assetsPath + "ekf";
run("chmod", "600", key);

function runEncryptedIndexBuildTests(cipherMode) {
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet({enableEncryption: "", encryptionKeyFile: key, encryptionCipherMode: cipherMode});
    rst.initiate();

    function initializeCollection(docs, collSuffix) {
        const coll = rst.getPrimary().getDB(dbName).getCollection(jsTestName() + collSuffix);
        assert.commandWorked(coll.insert(docs));
        return coll;
    }

    // Tests that an index build on ESE is successful when there are no process restarts.
    function runEncryptedNonResumableIndexBuildTest(docs, indexDocs, indexNames) {
        const coll =
            initializeCollection(docs, "_" + cipherMode + "_encrypted_nonresumable_index_build");

        indexDocs.forEach((indexDoc) => { assert.commandWorked(coll.createIndex(indexDoc)); });
        IndexBuildTest.assertIndexes(coll, 2, indexNames);
    }

    // Tests that an index build on ESE resumes successfully when there is a process restart
    // during the collection scan phase.
    function runEncryptedResumableIndexBuildTest(docs, indexDocs) {
        const coll =
            initializeCollection(docs, "_" + cipherMode + "_encrypted_resumable_index_build");

        const kFailpointDuringBuildUUID = 20386;
        ResumableIndexBuildTest.run(
            rst,
            dbName,
            coll.getName(),
            indexDocs,
            [{
                name: "hangIndexBuildDuringCollectionScanPhaseBeforeInsertion",
                logIdWithBuildUUID: kFailpointDuringBuildUUID
            }],
            1,
            ["collection scan"],
            [{numScannedAfterResume: 1}],
            [],
            [],
            {enableEncryption: "", encryptionKeyFile: key, encryptionCipherMode: cipherMode});
    }
    const docs = [{a: 1, b: 1}, {a: 2, b: 2}];
    const indexDocs = [{a: 1}];
    const indexNames = ["_id_", "a_1"];

    print("Starting EncryptedNonResumableTest");
    runEncryptedNonResumableIndexBuildTest(docs, indexDocs, indexNames);
    print("SUCCESS - EncryptedNonResumableTest");

    print("Starting EncryptedResumableTest");
    runEncryptedResumableIndexBuildTest(docs, [indexDocs]);
    print("SUCCESS - EncryptedResumableTest");

    rst.stopSet();
}

print("Starting EncryptedIndexBuildTests for cipher mode AES256-CBC");
runEncryptedIndexBuildTests("AES256-CBC");
print("SUCCESS - EncryptedIndexBuildTests for cipher mode AES256-CBC");

if (platformSupportsGCM) {
    print("Starting EncryptedIndexBuildTests for cipher mode AES256-GCM");
    runEncryptedIndexBuildTests("AES256-GCM");
    print("SUCCESS - EncryptedIndexBuildTests for cipher mode AES256-GCM");
} else {
    print("Skipping AES256-GCM test because the platform does not support it");
}

print("SUCCESS - encrypt_index_build.js");
