import {
    getCallerName,
} from 'jstests/core/timeseries/libs/timeseries_writes_util.js';
import {
    TEST_PROJECT_ID,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

export const dbNamePrefix = jsTestName();  // for multithreading usage this is not the final dbname

export function getDbName(threadId) {
    return `${dbNamePrefix}${threadId}`;
}

const dlqCollName = 'dlq';
const connectionName = 'db1';

/**
 * Starts a stream processor named 'spName` appended with thread id with 'pipeline' specification.
 * Returns the start command
 * result.
 *
 * The connections are pre-configured with
 * connections: [
 *     {name: "db1", type: 'atlas', options: {uri: "mongodb://127.0.0.1:_port_"}},
 *     {name: '__testMemory', type: 'in_memory', options: {}},
 * ]
 *
 * The DLQ is also pre-configured with
 * {dlq: {connectionName: "db1", db: "test" appended with thread id, coll: "dlq"}}
 */
function startStreamProcessor(pipeline, spName, threadId) {
    const uri = 'mongodb://' + db.getMongo().host;
    let startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        projectId: TEST_PROJECT_ID,
        name: `${spName}${threadId}`,
        processorId: spName,
        pipeline: pipeline,
        connections: [
            {name: connectionName, type: 'atlas', options: {uri: uri}},
            {name: '__testMemory', type: 'in_memory', options: {}},
            {
                name: `kafka${threadId}`,
                type: 'kafka',
                options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
            },
        ],
        options: {
            dlq: {connectionName: connectionName, db: getDbName(threadId), coll: dlqCollName},
            featureFlags: {}
        }
    };

    jsTestLog(`Starting ${spName}${threadId} - \n${tojson(startCmd)}`);
    return assert.commandWorked(db.runCommand(startCmd));
}

/*
** startStreamProcessorForThread
** cleans up database tables, starts stream processor
*/
export function startStreamProcessorForThread({pipeline, spName, threadId}) {
    jsTestLog(`Starting ${getCallerName(3)}() test`);
    const uri = 'mongodb://' + db.getMongo().host;

    db.getSiblingDB(getDbName(threadId)).outColl.drop();
    db.getSiblingDB(getDbName(threadId))[dlqCollName].drop();
    const source = {
        $source: {
            connectionName: `kafka${threadId}`,
            topic: `test1`,
            timeField: {$dateFromString: {"dateString": "$ts"}},
            testOnlyPartitionCount: NumberInt(1)
        }
    };
    jsTestLog(`createStreamProcessor dbcollName: ${
        db.getSiblingDB(getDbName(threadId)).outColl.getName()}`);

    // Starts a stream processor 'spName'.
    startStreamProcessor(
        [
            source,
            ...pipeline,
            {
                $merge: {
                    into: {
                        connectionName: connectionName,
                        db: getDbName(threadId),
                        coll: db.getSiblingDB(getDbName(threadId)).outColl.getName()
                    },
                    whenNotMatched: 'insert'
                }
            }
        ],
        spName,
        threadId);
}