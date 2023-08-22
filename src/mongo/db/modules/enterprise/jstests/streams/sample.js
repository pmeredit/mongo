/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
// Start a stream processor.
let startCmd = {
    streams_startStreamProcessor: '',
    tenantId: 'tenant1',
    name: 'sampleTest',
    processorId: 'sampleTest1',
    pipeline:
        [{$source: {'connectionName': '__testMemory'}}, {$emit: {'connectionName': '__testLog'}}],
    connections: []
};

let result = db.runCommand(startCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Start a stream processor.
let listCmd = {streams_listStreamProcessors: ''};

result = db.runCommand(listCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["streamProcessors"].length, 1);
assert.eq(result["streamProcessors"][0].name, "sampleTest");
assert.eq(result["streamProcessors"][0].status, "running");
assert.eq(result["streamProcessors"][0].pipeline, startCmd.pipeline);

// Start a sample on the stream processor.
let startSampleCmd = {streams_startStreamSample: '', name: 'sampleTest', limit: 4};

result = db.runCommand(startSampleCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Read the cursor id of the sample request.
let cursorId = result["id"];

// Insert 2 documents into the stream.
let insertCmd = {
    streams_testOnlyInsert: '',
    name: 'sampleTest',
    documents: [{'xyz': 10}, {'xyz': 20}],
};

result = db.runCommand(insertCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Retrieve 2 documents from the sample using the cursor id.
let getMoreCmd = {streams_getMoreStreamSample: cursorId, name: 'sampleTest', batchSize: 4};

let sampledDocs = [];
while (sampledDocs.length < 2) {
    result = db.runCommand(getMoreCmd);
    jsTestLog(result);
    assert.eq(result["cursor"]["id"], cursorId);
    sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
}
assert.eq(sampledDocs.length, 2);
assert.eq(sampledDocs[0], {'xyz': 10});
assert.eq(sampledDocs[1], {'xyz': 20});

// Get stats for the stream processor.
let statsCmd = {streams_getStats: '', name: 'sampleTest'};
result = db.runCommand(statsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["name"], "sampleTest");
assert.eq(result["status"], "running");
assert.eq(result["inputDocs"], 2);
assert.gt(result["inputBytes"], 200);
assert.eq(result["outputDocs"], 2);
assert.gt(result["outputBytes"], 200);

// Insert 3 more documents into the stream.
insertCmd = {
    streams_testOnlyInsert: '',
    name: 'sampleTest',
    documents: [{'xyz': 30}, {'xyz': 40}, {'xyz': 50}],
};

result = db.runCommand(insertCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

getMoreCmd = {
    streams_getMoreStreamSample: cursorId,
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
assert.eq(sampledDocs[0], {'xyz': 30});

// Get stats for the stream processor one more time.
result = db.runCommand(statsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
assert.eq(result["name"], "sampleTest");
assert.eq(result["status"], "running");
assert.eq(result["inputDocs"], 5);
assert.gt(result["inputBytes"], 400);
assert.eq(result["outputDocs"], 5);
assert.gt(result["outputBytes"], 400);

// Get metrics.
let getMetricsCmd = {streams_getMetrics: ''};
result = db.runCommand(getMetricsCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);
let gaugeValue = result["gauges"].filter(metric => metric.name === "num_stream_processors");
assert.eq(gaugeValue.length, 1);
assert.eq(gaugeValue[0].value, 1);
let counterValue = result["counters"].filter(metric => metric.name === "num_input_documents");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value, 5);
counterValue = result["counters"].filter(metric => metric.name === "num_output_documents");
assert.eq(counterValue.length, 1);
assert.eq(counterValue[0].value, 5);

// Stop the streamProcessor.
let stopCmd = {
    streams_stopStreamProcessor: '',
    name: 'sampleTest',
};
result = db.runCommand(stopCmd);
assert.eq(result["ok"], 1);
