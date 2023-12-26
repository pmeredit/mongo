import {
    checkpointInTheMiddleTest
} from "src/mongo//db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    generateDocs,
    windowPipelines
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

Random.setRandomSeed(20230328);

/**
 * This test forces a checkpoint in the middle for the given window.
 * @param {*} docs input for the tumbling window.
 * @param {*} pipeline is the pipeline for window.
 */
function tumblingWindowCheckpointInTheMiddle(docs, pipeline) {
    const windowInterval = {size: NumberInt(1), unit: "second"};
    const allowedLatenessInteval = {size: NumberInt(3), unit: "second"};
    checkpointInTheMiddleTest(docs, [{
                                  $tumblingWindow: {
                                      interval: windowInterval,
                                      allowedLateness: allowedLatenessInteval,
                                      pipeline: pipeline
                                  }
                              }]);
}

(function checkpointInTheMiddleTumblingWindowTests() {
    // can increase the numbers kept them to make tests run under 10 minutes
    const sizes = [10, 500, 5000];

    for (let x = 0; x < sizes.length; x++) {
        const docs = generateDocs(sizes[x]);
        for (let pipelineDef of windowPipelines) {
            tumblingWindowCheckpointInTheMiddle(docs, pipelineDef.pipeline);
        }
    }
}());
