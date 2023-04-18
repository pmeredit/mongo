/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');

let connectionName = 'kafka1';
let inputTopic = 'inputTopic';
let connections = [{
    name: connectionName,
    type: 'kafka',
    options: {bootstrapServers: 'localhost:9092', isTestKafka: true},
}];

let streams = new Streams(connections);
streams.createStreamProcessor("sp1", [
    {
        $source: {
            connectionName: connectionName,
            topic: inputTopic,
            partitionCount: NumberInt(1),
        }
    },
    {
        $tumblingWindow: {
            interval: {size: NumberInt(1), unit: "second"},
            pipeline: [
                {$sort: {value: 1}},
                {
                    $group: {
                        _id: "$id",
                        sum: {$sum: "$value"},
                        max: {$max: "$value"},
                        min: {$min: "$value"},
                        count: {$count: {}},
                        first: {$first: "$value"}
                    }
                },
                {$sort: {sum: 1}},
                {$limit: 1}
            ]
        }
    },
    {$match: {_id: NumberInt(0)}},
    {$emit: {connectionName: "__testLog"}}
]);

// Start the stream.
let result = streams.sp1.start();
assert.eq(result["ok"], 1, result);

// This call should fail, the stream name already exists.
let result2 = streams.sp1.start();
assert.eq(result2["ok"], 0);

// Stop the stream.
let result3 = streams.sp1.stop();
assert.eq(result3["ok"], 1);

// Start the stream.
let result4 = streams.sp1.start();
assert.eq(result4["ok"], 1);

// Stop the stream.
let result5 = streams.sp1.stop();
assert.eq(result5["ok"], 1);
}());
