import {
    checkpointInTheMiddleTest
} from "src/mongo//db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    generateDocs,
    listStreamProcessors,
    windowPipelines
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

Random.setRandomSeed(20230328);

/**
 * This test forces a checkpoint in the middle for the given window.
 * @param {*} docs input for the tumbling window.
 * @param {*} pipeline is the pipeline for window.
 */
function tumblingWindowCheckpointInTheMiddle(docs, pipeline, compareFunction) {
    const windowInterval = {size: NumberInt(1), unit: "second"};
    const allowedLatenessInteval = {size: NumberInt(3), unit: "second"};
    checkpointInTheMiddleTest(docs,
                              [{
                                  $tumblingWindow: {
                                      interval: windowInterval,
                                      allowedLateness: allowedLatenessInteval,
                                      pipeline: pipeline
                                  }
                              }],
                              compareFunction);
}

(function checkpointInTheMiddleTumblingWindowTests() {
    // Run the test for an artibtrary number of docs between 50 and 500.
    const numDocs = Math.floor(Math.random() * 450 + 50);
    jsTestLog("Running the test for " + numDocs + " docs.");
    const docs = generateDocs(numDocs);
    for (let pipelineDef of windowPipelines) {
        tumblingWindowCheckpointInTheMiddle(
            docs,
            pipelineDef.pipeline,
            pipelineDef.compareFunction ? pipelineDef.compareFunction : assert.eq);
    }
}());

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);