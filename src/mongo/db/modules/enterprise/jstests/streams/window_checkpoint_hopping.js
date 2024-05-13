import {
    checkpointInTheMiddleTest
} from "src/mongo//db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    generateDocsForHoppingWindow,
    listStreamProcessors,
    windowPipelines,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

Random.setRandomSeed(20230328);

/**
 * This test forces a checkpoint in the middle with the dataset.
 * @param {*} docs  is the input that contains the documents that will open multiple hopping windows
 * @param {*} pipeline the pipeline to use for the window
 */
function hoppingWindowCheckpointInTheMiddle(docs, pipeline, compareFunction) {
    const windowInterval = {size: NumberInt(5), unit: "second"};
    const allowedLatenessInteval = {size: NumberInt(3), unit: "second"};
    const hopInterval = {size: NumberInt(1), unit: "second"};
    checkpointInTheMiddleTest(docs,
                              [{
                                  $hoppingWindow: {
                                      interval: windowInterval,
                                      allowedLateness: allowedLatenessInteval,
                                      hopSize: hopInterval,
                                      pipeline: pipeline
                                  }
                              }],
                              compareFunction);
}

(function checkpointInTheMiddleHoppingWindowTests() {
    // Run the test for an artibtrary number of docs between 50 and 500.
    const numDocs = Math.floor(Math.random() * 450 + 50);
    jsTestLog("Running the test for " + numDocs + " docs.");
    // TODO SERVER-84707 improve test setup to collect each windows output and then compare each
    // windows output. without that a larger value of size for e.g.: 5000 would fail this test.
    const docs = generateDocsForHoppingWindow(numDocs);
    for (let pipelineDef of windowPipelines) {
        hoppingWindowCheckpointInTheMiddle(
            docs,
            pipelineDef.pipeline,
            pipelineDef.compareFunction ? pipelineDef.compareFunction : assert.eq);
    }
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);