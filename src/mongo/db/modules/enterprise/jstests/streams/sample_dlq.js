/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {listStreamProcessors} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

// Start a streamProcessor.
const sp = getDefaultSp();
const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
outputColl.drop();

const results = sp.process([
    {$source: {documents: [{_id: 1}]}},
    {$validate: {validator: {$expr: {$gt: ['$fullDocument._id', 9]}}, validationAction: 'dlq'}},
    {
        $merge: {
            into: {connectionName: test.atlasConnection, db: test.dbName, coll: test.outputCollName}
        }
    },
]);

// Verify the DLQ-ed message shows up in the sample stream.
assert.soon(() => {
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

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
