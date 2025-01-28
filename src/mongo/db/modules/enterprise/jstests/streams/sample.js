/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {
    getStats,
    listStreamProcessors,
    sanitizeDoc,
    startSample,
    stopStreamProcessor,
    TEST_PROJECT_ID,
    TEST_TENANT_ID,
    verifyInputEqualsOutput
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

// Get counters pre-test.
let initialMetrics = {streams_getMetrics: ''};
let result = db.runCommand(initialMetrics);
jsTestLog(result);
let metricValue =
    result["counters"].filter(metric => metric.name === "stream_processor_requests_total");
let counterValue =
    metricValue.filter(mv => mv.labels[0].value === "start" && mv.labels[1].value === "true");
let initialStartSpCounter = counterValue.length > 0 ? counterValue[0].value : 0;
metricValue =
    result["counters"].filter(metric => metric.name === "stream_processor_requests_total");
counterValue =
    metricValue.filter(mv => mv.labels[0].value === "stop" && mv.labels[1].value === "true");
let initialStopSpCounter = counterValue.length > 0 ? counterValue[0].value : 0;

let intelMetrics = {streams_getMetrics: '', externalMetrics: true};
result = db.runCommand(intelMetrics);
assert.eq(result["ok"], 1);
jsTestLog(result);
let value = result.gauges.filter(metric => metric.name == "system_cpu_percent");
assert.gt(value.length, 0);
// Start a stream processor.
let startCmd = {
    streams_startStreamProcessor: '',
    tenantId: TEST_TENANT_ID,
    projectId: TEST_PROJECT_ID,
    name: 'sampleTest',
    processorId: 'sampleTest1',
    pipeline:
        [{$source: {'connectionName': '__testMemory'}}, {$emit: {'connectionName': '__testLog'}}],
    connections: [{name: '__testMemory', type: 'in_memory', options: {}}],
    options: {featureFlags: {}}
};

result = db.runCommand(startCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// List stream processors.
result = listStreamProcessors();
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["streamProcessors"].length, 1);
assert.eq(result["streamProcessors"][0].name, "sampleTest");
assert.eq(result["streamProcessors"][0].status, "running");
assert.eq(result["streamProcessors"][0].pipeline, startCmd.pipeline);

// Start a sample on the stream processor.
result = startSample('sampleTest', 4);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Read the cursor id of the sample request.
let cursorId = result["id"];

// Insert 2 documents into the stream.
let insertCmd = {
    streams_testOnlyInsert: '',
    tenantId: TEST_TENANT_ID,
    name: 'sampleTest',
    documents: [{'xyz': 10}, {'xyz': 20}],
};

result = db.runCommand(insertCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Retrieve 2 documents from the sample using the cursor id.
let getMoreCmd = {
    streams_getMoreStreamSample: cursorId,
    tenantId: TEST_TENANT_ID,
    name: 'sampleTest',
    batchSize: 4
};

let sampledDocs = [];
while (sampledDocs.length < 2) {
    result = db.runCommand(getMoreCmd);
    jsTestLog(result);
    assert.eq(result["cursor"]["id"], cursorId);
    sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
}
assert.eq(sampledDocs.length, 2);
assert.eq(sanitizeDoc(sampledDocs[0]), {'xyz': 10});
assert.eq(sanitizeDoc(sampledDocs[1]), {'xyz': 20});

// Get stats for the stream processor.
result = getStats('sampleTest');
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["name"], "sampleTest");
assert.eq(result["status"], "running");
assert.eq(result["inputMessageCount"], 2);
assert.gt(result["inputMessageSize"], 200);
assert.eq(result["outputMessageCount"], 2);
assert.gt(result["outputMessageSize"], 200);

// Insert 3 more documents into the stream.
insertCmd = {
    streams_testOnlyInsert: '',
    tenantId: TEST_TENANT_ID,
    name: 'sampleTest',
    documents: [{'xyz': 30}, {'xyz': 40}, {'xyz': 50}],
};

result = db.runCommand(insertCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

getMoreCmd = {
    streams_getMoreStreamSample: cursorId,
    tenantId: TEST_TENANT_ID,
    name: 'sampleTest',
    batchSize: 1
};

// Retrieve 2 more documents from the sample using the cursor id.
sampledDocs = [];
while (sampledDocs.length < 2) {
    result = db.runCommand(getMoreCmd);
    jsTestLog(result);
    sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
    if (sampledDocs.length < 2) {
        assert.eq(result["cursor"]["id"], cursorId);
    } else {
        // Verify that cursor id in the response is set to 0 when the limit is reached.
        assert.eq(result["cursor"]["id"], 0);
    }
}
assert.eq(sampledDocs.length, 2);
assert.eq(sanitizeDoc(sampledDocs[0]), {'xyz': 30});

// Get stats for the stream processor one more time.
result = getStats('sampleTest');
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["name"], "sampleTest");
assert.eq(result["status"], "running");
assert.eq(result["inputMessageCount"], 5);
assert.gt(result["inputMessageSize"], 400);
assert.eq(result["outputMessageCount"], 5);
assert.gt(result["outputMessageSize"], 400);

// Get metrics.
let getMetricsCmd = {streams_getMetrics: ''};
result = db.runCommand(getMetricsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
let gaugeValue = result["gauges"].filter(
    metric => metric.name === "num_stream_processors_by_status" && metric.value === 1);
assert.eq(gaugeValue.length, 1);
let gaugeStatusLabel = gaugeValue[0].labels.filter(label => label.key === "status");
assert.eq(gaugeStatusLabel.length, 1);
assert.eq(gaugeStatusLabel[0].value, "running");
counterValue = result["counters"].filter(metric => metric.name === "num_input_documents");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value, 5);
counterValue = result["counters"].filter(metric => metric.name === "num_output_documents");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value, 5);
metricValue =
    result["counters"].filter(metric => metric.name === "stream_processor_requests_total");
counterValue =
    metricValue.filter(mv => mv.labels[0].value === "start" && mv.labels[1].value === "true");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value - initialStartSpCounter, 1);

// Stop the streamProcessor.
result = stopStreamProcessor('sampleTest');
assert.eq(result["ok"], 1);

getMetricsCmd = {
    streams_getMetrics: ''
};
result = db.runCommand(getMetricsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
counterValue =
    result["counters"].filter(metric => metric.name === "stream_processor_requests_total");
metricValue =
    result["counters"].filter(metric => metric.name === "stream_processor_requests_total");
counterValue =
    metricValue.filter(mv => mv.labels[0].value === "stop" && mv.labels[1].value === "true");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value - initialStopSpCounter, 1);

// streams_startStreamProcessor.shouldStartSample is set to true for processors
// created with sp.process([pipeline]). Since we immediately start a sample session,
// we'll always capture the first few messages.
function processImmediatelyStartsSample() {
    let sp = getDefaultSp();
    let input = [{a: 1}, {b: 1}];
    let results = sp.process([{$source: {documents: input}}]);
    verifyInputEqualsOutput(input, results);
}

processImmediatelyStartsSample();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);