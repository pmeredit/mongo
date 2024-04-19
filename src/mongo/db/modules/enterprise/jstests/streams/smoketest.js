/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {kafkaExample} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {listStreamProcessors} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

kafkaExample("kafka1", "inputTopic", true);
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);