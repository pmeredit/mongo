/**
 * Start a sample on a streamProcessor.
 */
function startSample(name) {
    let startSampleCmd = {streams_startStreamSample: '', name: name};
    let result = db.runCommand(startSampleCmd);
    assert.commandWorked(result);
    return result["id"];
}

/**
 * Sample until count results are retrieved.
 */
function sampleUntil(cursorId, count, name, maxIterations = 100, sleepInterval = 50) {
    let getMoreCmd = {streams_getMoreStreamSample: cursorId, name: name};
    let sampledDocs = [];
    let i = 0;
    while (i < maxIterations && sampledDocs.length < count) {
        let result = db.runCommand(getMoreCmd);
        assert.commandWorked(result);
        assert.eq(result["cursor"]["id"], cursorId);
        sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
        sleep(sleepInterval);
        i += 1;
    }
    assert.gte(sampledDocs.length, count, "Failed to retrieve expected number of docs");
    return sampledDocs;
}