import {findMatchingLogLine} from "jstests/libs/log.js";
import {
    startStreamProcessor,
    stopStreamProcessor,
    TEST_PROJECT_ID,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const spName = "stackTraceTest";
const inputCollName = "testin";
const outputCollName = 'outputColl';
const outputColl = db[outputCollName];
const inputColl = db[inputCollName];

startStreamProcessor(spName, [
    {
        $source: {
            'connectionName': 'db1',
            'db': 'test',
            'coll': inputColl.getName(),
            'timeField': {$toDate: {$multiply: ['$fullDocument.ts', 1000]}}
        }
    },
    {
        $merge: {
            into: {connectionName: 'db1', db: 'test', coll: outputColl.getName()},
            whenMatched: 'replace',
            whenNotMatched: 'insert'
        }
    }
]);

assert.commandWorked(
    db.runCommand({streams_sendEvent: '', processorId: '', tenantId: '', dumpStackTrace: true}));

assert.soon(() => {
    const log = assert.commandWorked(db.adminCommand({getLog: "global"})).log;
    var line = findMatchingLogLine(log, {id: 9620111});
    if (line === null) {
        line = findMatchingLogLine(log, {id: 9620113});
    }
    return !(line === null);
});

stopStreamProcessor(spName);