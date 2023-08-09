/**
 * Start a sample on a streamProcessor.
 */
export function startSample(name) {
    let startSampleCmd = {streams_startStreamSample: '', name: name};
    let result = db.runCommand(startSampleCmd);
    assert.commandWorked(result);
    return result["id"];
}

/**
 * Sample until count results are retrieved.
 */
export function sampleUntil(cursorId, count, name, maxIterations = 100, sleepInterval = 50) {
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

/**
 * Wait until at least the specified number of documents exists in coll.
 */
export function waitForCount(coll, count, maxWaitSeconds = 5) {
    const sleepInterval = 50;
    const maxTime = Date.now() + 1000 * maxWaitSeconds;
    let currentCount = coll.find({}).count();
    while (currentCount < count && Date.now() < maxTime) {
        currentCount = coll.find({}).count();
        sleep(sleepInterval);
    }
    if (currentCount < count) {
        assert(false, 'maximum time elapsed');
    }
}

export function waitForDoc(coll, predicate, maxWaitSeconds = 5) {
    const sleepInterval = 50;
    const maxTime = Date.now() + 1000 * maxWaitSeconds;
    while (Date.now() < maxTime) {
        let docs = coll.find({}).toArray();
        if (docs.some(predicate)) {
            return;
        }
        sleep(sleepInterval);
    }
    throw 'maximum time elapsed';
}
