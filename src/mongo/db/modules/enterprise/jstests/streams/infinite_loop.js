/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

// This tests creates a stream processor with a $source reading from the same DB the $merge is
// writing to. This infinite loop creates a document that gets larger and larger. This will
// eventually lead to an error from the _changestream server_, complaining 16MB limit is exceeded.
// In this case we fail the stream processor with a non-retryable error.

// Start a streamProcessor.
const sp = getDefaultSp();
const spName = "changeStreamDbSource";
const inputDb = db.getSiblingDB(test.dbName);
const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
inputDb.dropDatabase();
outputColl.drop();
sp.createStreamProcessor(spName, [
    {$source: {connectionName: test.atlasConnection, db: test.dbName}},
    {
        $merge: {
            into: {connectionName: test.atlasConnection, db: test.dbName, coll: test.outputCollName}
        }
    },
]);
sp[spName].start();

// Insert a doc that will start the infinite loop, verify the stream processor ends in an error
// state.
const doc = {
    a: 1
};
const inputColl = inputDb[test.inputCollName];
inputColl.insertOne(doc);
assert.soon(() => {
    const result = sp.listStreamProcessors();
    let thisSp = result.streamProcessors.find((sp) => sp.name == spName);
    assert.neq(thisSp, null);
    let stats = sp[spName].stats();
    return stats.dlqMessageCount > 0 ||
        (thisSp.status == "error" &&
         thisSp.error.code == ErrorCodes.StreamProcessorSourceDocTooLarge &&
         thisSp.error.userError == true && thisSp.error.retryable == false);
}, "waiting for error to appear", 5 * 60 * 1000);
sp[spName].stop();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);