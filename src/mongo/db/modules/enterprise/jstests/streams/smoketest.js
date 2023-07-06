/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {kafkaExample} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

kafkaExample("kafka1", "inputTopic", true);