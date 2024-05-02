/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    listStreamProcessors,
    TEST_TENANT_ID
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

assert.commandFailedWithCode(db.runCommand({
    "streams_startStreamProcessor": "",
    name: "foo",
    processorId: "foo",
    tenantId: TEST_TENANT_ID,
    pipeline: [
        {$source: {connectionName: "__testMemory"}, config: {"fullDocumentOnly": true}},
        {$emit: {connectionName: "__testLog"}}
    ],
    connections: [{name: "__testMemory", type: "in_memory", options: {}}],
    options: {featureFlags: {}},
}),
                             8661200);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);