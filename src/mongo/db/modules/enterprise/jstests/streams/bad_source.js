/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

assert.commandFailedWithCode(db.runCommand({
    "streams_startStreamProcessor": "",
    name: "foo",
    processorId: "foo",
    tenantId: "testTenant",
    pipeline: [
        {$source: {connectionName: "__testMemory"}, config: {"fullDocumentOnly": true}},
        {$emit: {connectionName: "__testLog"}}
    ],
    connections: [{name: "__testMemory", type: "in_memory", options: {}}],
    options: {}
}),
                             8661200);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);