/**
 * Test which verifies that $source can be configured to draw input from a change stream.
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    makeRandomString,
    TEST_TENANT_ID,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const connectionName = "conn1";

const uri = 'mongodb://' + db.getMongo().host;
let connectionRegistry = [
    {name: connectionName, type: 'atlas', options: {uri: uri}},
    {name: '__testMemory', type: 'in_memory', options: {}},
];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

(function honorSourceBufferSizeLimit() {
    // Test that ChangeStreamConsumerOperator stays within the configured memory usage limits.

    const dbName = "test";
    const collName = "input_coll";
    db.getSiblingDB(dbName)[collName].drop();
    const clusterTime = db.getSiblingDB(dbName).hello().$clusterTime.clusterTime;

    // Add 1000 input docs each of size ~1KB to the input collection.
    Random.setRandomSeed(42);
    const str = makeRandomString(1000);
    let writeColl = db.getSiblingDB(dbName)[collName];
    Array.from({length: 1000}, (_, i) => i)
        .forEach(idx => { assert.commandWorked(writeColl.insert({_id: idx, str: str})); });

    // For different values of number of stream processors and source buffer sizes, test that
    // maxMemoryUsage reported by ChangeStreamConsumerOperator stays within the configured limit.
    const totalNumPages = 10;
    for (let numProcessors = 1; numProcessors <= 3; ++numProcessors) {
        for (let maxPages = 1; maxPages <= 5; ++maxPages) {
            let featureFlags = {
                sourceBufferTotalSize: NumberLong(totalNumPages * 1500),
                sourceBufferMaxSize: NumberLong(maxPages * 1500),
                sourceBufferMinPageSize: NumberLong(1500),
                sourceBufferMaxPageSize: NumberLong(1500)
            };

            // Start all the stream processors.
            for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
                const processorName = "sp" + spIdx;
                sp.createStreamProcessor(processorName, [
                    {
                        $source: {
                            connectionName: connectionName,
                            db: dbName,
                            coll: collName,
                            config: {
                                startAtOperationTime: clusterTime,
                            }
                        }
                    },
                    {$emit: {connectionName: '__testMemory'}}
                ]);

                const processor = sp[processorName];
                processor.start({featureFlags: featureFlags});
            }

            // Wait until all stream processors are done processing all the input docs.
            assert.soon(() => {
                let allDone = true;
                for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
                    const processorName = "sp" + spIdx;
                    let statsResult = getStats(processorName);
                    if (statsResult.outputMessageCount < 1000) {
                        allDone = false;
                    }
                }
                return allDone;
            });

            // Verify stats and stop all stream processors.
            for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
                const processorName = "sp" + spIdx;
                let statsResult = getStats(processorName);
                jsTestLog(statsResult);
                const sourceStats = statsResult.operatorStats[0];
                assert.eq('ChangeStreamConsumerOperator', sourceStats.name);
                assert.eq(sourceStats.stateSize, 0, statsResult);
                assert.gt(sourceStats.maxMemoryUsage, 1000, statsResult);
                assert.lt(
                    sourceStats.maxMemoryUsage, featureFlags.sourceBufferMaxSize, statsResult);
                sp[processorName].stop();
            }
        }
    }
    db.getSiblingDB(dbName)[collName].drop();
}());
