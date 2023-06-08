/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

// Start a stream processor.
let startCmd = {
    streams_startStreamProcessor: '',
    name: 'sampleTest',
    pipeline:
        [{$source: {'connectionName': '__testMemory'}}, {$emit: {'connectionName': '__testLog'}}],
    connections: []
};

let result = db.runCommand(startCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Get metrics and verify that the value of num_stream_processors gauge is 1.
let getMetricsCmd = {streams_getMetrics: ''};
result = db.runCommand(getMetricsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
const gaugeValue = result["gauges"].filter(gauge => gauge.name === "num_stream_processors");
assert.eq(gaugeValue.length, 1);
assert.eq(gaugeValue[0].value, 1);

// Stop the streamProcessor.
let stopCmd = {
    streams_stopStreamProcessor: '',
    name: 'sampleTest',
};
result = db.runCommand(stopCmd);
assert.eq(result["ok"], 1);
}());
