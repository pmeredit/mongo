import {
    documentEq,
} from "jstests/aggregation/extras/utils.js";
import {
    CheckPointTestHelper,
    removeProjections,
    uuidStr
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    group_sort_pipeline,
    listStreamProcessors,
    sort_limit_pipeline,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

/**
 * This test resumes from a stored checkpoint and ensures that
 * the resume was successful and the test results were also as expected
 * The saved checkpoint was obtained from the checkpointInTheMiddleTest
 * by setting the intermediateStateDumpDir param to true.
 * If the source pipeline changes in the future, these tests should fail
 * since they verify the source pipeline against the pipeline stored in the
 * checkpoint metadata
 */
function _setupAndRunTest(pipeline, srcDir, expectedResultsLen, expectedStartOffset) {
    // Read pipeline stored in manifest metadata. Serves as a test to ensure
    // some metadata sanity and also ensures that the pipeline is still the same
    // one that we expect
    let manifest = _readDumpFile(srcDir + "/manifest.bson");
    manifest = manifest[0];
    assert.eq(manifest["metadata"]["userPipeline"][1]["$hoppingWindow"]["pipeline"],
              pipeline[0]["$hoppingWindow"]["pipeline"]);

    let expectedResults = _readDumpFile(srcDir + "/expectedResults.bson");
    assert.eq(expectedResults.length, expectedResultsLen);

    // copy test data files to an absolute path that is accessible from mongod
    const destDir = "/tmp/checkpoint/" + uuidStr();

    let inputDocs = _readDumpFile(srcDir + "/inputDocs.bson");

    var test2 = new CheckPointTestHelper(
        inputDocs, pipeline, 10000000, "kafka", true, {}, destDir, destDir);

    // mkdir and copyDir are builtins available in mongodb js driver code.
    // But for some reason, eslint complains about copyDir being an undefined
    // function. So disabling eslint for the copyDir invocation
    // mkdir(test2.spRestoreDir);
    mkdir(test2.spRestoreDir);
    // eslint-disable-next-line
    copyDir(srcDir + "/jstests-tenant/resume_from_checkpoint_test_spid", test2.spRestoreDir);

    let ids = test2.getCheckpointIds();
    assert.eq(ids.length, 1, "expected one checkpoint");
    const id = ids[0];

    test2.run();
    assert.soon(() => { return test2.outputColl.find({}).count() == expectedResults.length; });

    const startingOffset = test2.getStartOffsetFromCheckpoint(id, true);
    assert.eq(startingOffset, expectedStartOffset);
    test2.stop();

    let results = test2.getResults();
    assert.eq(results.length, expectedResults.length);

    var r = new Set();
    for (let i = 0; i < results.length; i++) {
        results[i] = removeProjections(results[i]);
        for (let j = 0; j < expectedResults.length; j++) {
            if (documentEq(results[i], expectedResults[j])) {
                if (r.has(j)) {
                    continue;
                } else {
                    r.add(j);
                    break;
                }
            }
        }
    }
    assert.eq(r.size, results.length);
}

function testHoppingWindowGroupSort() {
    const windowInterval = {size: NumberInt(5), unit: "second"};
    const allowedLatenessInteval = {size: NumberInt(3), unit: "second"};
    const hopInterval = {size: NumberInt(1), unit: "second"};

    const pipeline = [{
        $hoppingWindow: {
            interval: windowInterval,
            allowedLateness: allowedLatenessInteval,
            hopSize: hopInterval,
            pipeline: group_sort_pipeline.pipeline
        }
    }];

    const srcDir =
        "src/mongo/db/modules/enterprise/jstests/streams/data/checkpoint_backwards_compat/hopping_window_group_sort";
    const expectedResultsLen = 50;
    const expectedStartOffset = 627;

    _setupAndRunTest(pipeline, srcDir, expectedResultsLen, expectedStartOffset);
}

function testHoppingWindowSortLimit() {
    const windowInterval = {size: NumberInt(5), unit: "second"};
    const allowedLatenessInteval = {size: NumberInt(3), unit: "second"};
    const hopInterval = {size: NumberInt(1), unit: "second"};

    const pipeline = [{
        $hoppingWindow: {
            interval: windowInterval,
            allowedLateness: allowedLatenessInteval,
            hopSize: hopInterval,
            pipeline: sort_limit_pipeline.pipeline
        }
    }];

    const srcDir =
        "src/mongo/db/modules/enterprise/jstests/streams/data/checkpoint_backwards_compat/hopping_window_sort_limit";
    const expectedResultsLen = 157;
    const expectedStartOffset = 627;

    _setupAndRunTest(pipeline, srcDir, expectedResultsLen, expectedStartOffset);
}

testHoppingWindowGroupSort();
testHoppingWindowSortLimit();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
