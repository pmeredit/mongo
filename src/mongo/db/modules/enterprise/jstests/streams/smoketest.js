/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');

kafkaExample("kafka1", "inputTopic", true);
}());
