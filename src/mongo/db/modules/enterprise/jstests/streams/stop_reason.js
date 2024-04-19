/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 *
 * Validates that we print a stop reason in the stream processor stop logs.
 */

import {findMatchingLogLines} from "jstests/libs/log.js";
import {getDefaultSp, test} from 'src/mongo/db/modules/enterprise/jstests/streams/fake_client.js';
import {listStreamProcessors} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

// Start a streamProcessor.
const sp = getDefaultSp();
const spName = "testStopReasonLog";
const inputColl = db.getSiblingDB(test.dbName)[test.inputCollName];
const outputColl = db.getSiblingDB(test.dbName)[test.outputCollName];
inputColl.drop();
outputColl.drop();
sp.createStreamProcessor(spName, [
    {$source: {connectionName: test.atlasConnection, db: test.dbName, coll: test.inputCollName}},
    {
        $merge: {
            into: {connectionName: test.atlasConnection, db: test.dbName, coll: test.outputCollName}
        }
    },
]);
sp[spName].start();
sp[spName].stop();
// Verify the stop logs shows up.
assert.soon(() => {
    const log = assert.commandWorked(db.adminCommand({getLog: "global"})).log;
    return log.find((line) => {
        let logLine = JSON.parse(line);
        return logLine.id == 8728300 && logLine.attr.stopReason == "ExternalStopRequest";
    }) != null;
});
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);