/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

function badDBSourceStartError() {
    // Chose a valid network address and port that does not have a running server on it.
    const badUri = "mongodb://127.0.0.1:9123";
    const dbConnectionName = "db1";
    const dbName = "test";
    const inputCollName = "testin";
    const spName = "sp1";
    const connectionRegistry = [{
        name: dbConnectionName,
        type: 'atlas',
        options: {
            uri: badUri,
        }
    }];
    // Calls streams_startStreamProcessor with a bad $source.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: dbConnectionName,
                    db: dbName,
                    coll: inputCollName,
                }
            },
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq(75385, result.code);
    assert.eq(
        "No suitable servers found (`serverSelectionTryOnce` set): [connection refused calling hello on '127.0.0.1:9123']: generic server error",
        result.errmsg);
}

function badKafkaSourceStartError() {
    const spName = "sp2";
    const badUri = 'foohost:9092';
    const connectionName = "kafka1";
    const inputTopicName = "intopic";
    const connectionRegistry = [{
        name: connectionName,
        type: 'kafka',
        options: {bootstrapServers: badUri},
    }];
    // Calls streams_startStreamProcessor with a bad $source.
    let result = db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {$source: {connectionName: connectionName, topic: inputTopicName}},
            {$emit: {connectionName: "__testMemory"}}
        ],
        connections: connectionRegistry,
    });
    assert.commandFailed(result);
    assert.eq(77175, result.code);
    assert.eq(
        "Could not connect to the Kafka topic with librdkafka error code: -195 and message: Local: Broker transport failure.",
        result.errmsg);
}

badDBSourceStartError();
badKafkaSourceStartError();
}());

// TODO(SERVER-80742): Write tests for the below.
// dbSourceDiesAfterSuccesfulStart()
// kafkaSourceDiesAfterSuccesfulStart()
// kafkaSourceStartBeforeTopicExists()
// dbSourceInvalidUri()
// kafkaSourceCheckpointFails()
// dbSourceCheckpointFails()
// kafkaEmitFails()
// dbMergeFails()
// dlqFails()