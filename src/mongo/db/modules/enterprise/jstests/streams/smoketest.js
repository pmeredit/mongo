/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

let connectionName = 'kafka1';

let cmd = {
    _startStreamProcessor: '',
    name: 'helloWorld',
    pipeline: [
        {
            $source: {
                'connectionName': connectionName,
                topic: 'myTopic',
                partitionCount: NumberInt(1),
                timeField: {$toDate: {$multiply: ['$event_time_seconds', 1000]}},
                tsFieldOverride: '_myts'
            }
        },
        {$match: {a: 1}},
        {$emit: {'connectionName': '__testLog'}}
    ],
    connections: [{
        name: connectionName,
        type: 'kafka',
        options: {bootstrapServers: 'localhost:9092', isTestKafka: true}
    }]
};

let result = db.runCommand(cmd);
jsTestLog(result);
assert.eq(result["ok"], 1);

// This call should fail, the stream name already exists.
let result2 = db.runCommand(cmd);
jsTestLog(result2);
assert.eq(result2["ok"], 0);
}());
