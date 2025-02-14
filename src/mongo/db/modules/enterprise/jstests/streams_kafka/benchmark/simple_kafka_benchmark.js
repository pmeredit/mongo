/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 * A simple jstest based benchmark for Kafka $emit.
 */
import {uuidStr} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    TEST_TENANT_ID,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    LocalKafkaCluster
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/kafka_utils.js";

const kafkaPlaintextName = "kafka1";
const dbConnName = "db1";
const uri = 'mongodb://' + db.getMongo().host;
const kafkaUri = 'localhost:9092';
const eventHubName = "eventhub";
let connectionRegistry = [
    {name: dbConnName, type: 'atlas', options: {uri: uri}},
    {
        name: kafkaPlaintextName,
        type: 'kafka',
        options: {bootstrapServers: kafkaUri},
    },
    {name: '__testMemory', type: 'in_memory', options: {}},
];
let eventHubSetup = false;
// You can set this environment variable locally and the benchmark
// will target an EventHub.
let eventHubConnectionString = _getEnv("EVENT_HUB_CONNECTION_STRING");
if (eventHubConnectionString !== null && eventHubConnectionString != "") {
    eventHubSetup = true;
    let eventHubBootstrapServers = _getEnv("EVENT_HUB_BOOTSTRAP_SERVERS");
    assert.neq(eventHubBootstrapServers, "");
    assert.neq(eventHubBootstrapServers, null);
    connectionRegistry.push(
        {
            name: eventHubName,
            type: 'kafka',
            options: {
                bootstrapServers: eventHubBootstrapServers,
                auth: {
                    saslMechanism: "PLAIN",
                    saslUsername: "$ConnectionString",
                    saslPassword: eventHubConnectionString,
                    securityProtocol: "SASL_SSL",
                }
            },
        },
    );
    jsTestLog(`Added EventHub to connection registry ${
        connectionRegistry[connectionRegistry.length - 1].options.bootstrapServers}`);
}

const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

// Runs a test function with a fresh state including a fresh Kafka cluster.
function runKafkaTest(kafka, testFn, partitionCount = 1) {
    // Clear any previous persistent state so that the test starts with a clean slate.
    kafka.start(partitionCount);
    const funcStr = testFn.toString();
    try {
        jsTestLog(`Running: ${funcStr}`);
        testFn();
        jsTestLog(`Passed: ${funcStr}`);
    } finally {
        kafka.stop();
    }
}

let kafka = new LocalKafkaCluster();

// A benchmark that writes 1.5 million events from an in-memory test source to a Kafka $emit sink.
function benchmarkMemorySourceToKafkaEmit() {
    // 1.5 million docs
    const totalDocs = 1500000;
    const spName = "throughputMemory";
    const eventHubOnePartition = "eventhubonepartition";
    const eventHubEightPartition = "eventhubeightpartition";

    let resultsMap = new Map();
    const innerTest = ({useEventHub, useKafkaSink, eventHubTopic}) => {
        runKafkaTest(kafka, () => {
            let outputTopic = uuidStr();
            if (eventHubTopic) {
                outputTopic = eventHubTopic;
            }
            const kafkaConn = useEventHub ? eventHubName : kafkaPlaintextName;

            let options = {};
            options.featureFlags = {enableInMemoryConstantMessage: true};
            let pipeline = [
                // The source implementation will generate 1MB batches of docs.
                {$source: {'connectionName': '__testMemory'}},
            ];
            let config = {dateFormat: 'ISO8601'};
            if (useKafkaSink) {
                var stage = {
                    $emit: {connectionName: kafkaConn, topic: outputTopic, config: config}
                };
                pipeline.push(stage);
            }
            sp.createStreamProcessor(spName, pipeline);
            sp[spName].start(
                options,
                true /* assertWorked */,
                undefined /* pipelineVersion */,
                !useKafkaSink /* ephemeral */
            );

            // Wait for all the updates to be processed.
            const start = new Date();
            let lastStats = null;
            assert.soon(() => {
                lastStats = sp[spName].stats();
                return lastStats.outputMessageCount >= totalDocs;
            }, "waiting for output", 3 * 60 * 1000);
            const stopUnderTimer = useKafkaSink;
            if (stopUnderTimer) {
                // When using kafka producer, we need to stop to flush all outstanding events.
                sp[spName].stop();
            }
            const end = new Date();
            const time = end - start;
            const params = tojson({
                useKafkaSink: useKafkaSink,
                useEventHub: useEventHub,
                topic: outputTopic,
            });
            jsTestLog(`Finished: time: ${time}, ${params}, ${tojson(lastStats)}`);
            if (!stopUnderTimer) {
                // Stop the processor if we haven't already.
                sp[spName].stop();
            }
            if (!resultsMap.has(params)) {
                resultsMap.set(params, {sum: 0, count: 0});
            }
            const current = resultsMap.get(params);
            resultsMap.set(params, {sum: current.sum + time, count: current.count + 1});
        });
    };

    const reps = 2;
    for (let rep = 0; rep < reps; rep += 1) {
        // write the in-memory source to a no-op sink
        innerTest({useKafkaSink: false});
        innerTest({useKafkaSink: false, skipNoopJson: true});
        innerTest({useKafkaSink: false, addProject: true, skipNoopJson: true});
        // write to the local kafka
        innerTest({useEventHub: false, useKafkaSink: true});
        if (eventHubSetup) {
            // write to remote EventHub
            for (const topic of [eventHubEightPartition, eventHubOnePartition]) {
                innerTest({useEventHub: true, useKafkaSink: true, eventHubTopic: topic});
            }
        }

        jsTestLog("Finished iteration");
        jsTestLog(Array.from(resultsMap.entries()));
    }

    // print the averages.
    let avgMsMap = new Map();
    for (const [params, samples] of resultsMap) {
        avgMsMap.set(params, samples.sum / samples.count);
    }
    jsTestLog(Array.from(avgMsMap.entries()));
}
benchmarkMemorySourceToKafkaEmit();