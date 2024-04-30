/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {listStreamProcessors} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

// Start a streamProcessor.
const sp = getDefaultSp();
const spName = "sampleWithDlq";
const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];
const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
inputColl.drop();
outputColl.drop();
sp.createStreamProcessor(spName, [
    {$source: {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}},
    {$validate: {validator: {$expr: {$gt: ['$fullDocument._id', 9]}}, validationAction: 'dlq'}},
    {
        $merge: {
            into: {connectionName: test.atlasConnection, db: test.dbName, coll: test.outputCollName}
        }
    },
]);
sp[spName].start();

// Start a sample session.
const cursorId = sp[spName].startSample()["id"];

// Insert a doc that will be DLQ-ed in the $validate stage.
const doc = {
    _id: 1
};
inputColl.insertOne(doc);

// Verify the DLQ-ed message shows up in the sample stream.
assert.soon(() => {
    const results = sp[spName].getNextSample(cursorId);
    if (results.length == 1) {
        const result = results[0];
        assert(result.hasOwnProperty("_dlqMessage"));
        const dlqMessage = result["_dlqMessage"];
        assert.eq("Input document found to be invalid in $validate stage",
                  dlqMessage.errInfo.reason);
        return true;
    }

    return false;
});

// Stop the streamProcessor.
sp[spName].stop();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);