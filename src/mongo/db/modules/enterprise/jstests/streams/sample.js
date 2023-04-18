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

// Start a sample on the stream processor.
let startSampleCmd = {
    streams_startStreamSample: '',
    name: 'sampleTest',
};

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

// Insert 3 more documents into the stream.
insertCmd = {
    streams_testOnlyInsert: '',
    name: 'sampleTest',
    documents: [{'xyz': 30}, {'xyz': 40}, {'xyz': 50}],
};

result = db.runCommand(insertCmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// Retrieve these 3 documents from the sample using the cursor id.
getMoreCmd = {
    streams_getMoreStreamSample: cursorId,
    name: 'sampleTest',
    batchSize: 4
};

sampledDocs = [];
while (sampledDocs.length < 3) {
    result = db.runCommand(getMoreCmd);
    jsTestLog(result);
    assert.eq(result["cursor"]["id"], cursorId);
    sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
}
assert.eq(sampledDocs.length, 3);
assert.eq(sampledDocs[0], {'xyz': 30});
assert.eq(sampledDocs[1], {'xyz': 40});
assert.eq(sampledDocs[2], {'xyz': 50});

// Stop the streamProcessor.
let stopCmd = {
    streams_stopStreamProcessor: '',
    name: 'sampleTest',
};
result = db.runCommand(stopCmd);
assert.eq(result["ok"], 1);
}());
