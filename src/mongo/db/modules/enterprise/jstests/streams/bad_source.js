/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
assert.commandFailedWithCode(db.runCommand({
    "streams_startStreamProcessor": "",
    name: "foo",
    pipeline: [
        {$source: {connectionName: "__testMemory"}, config: {"fullDocumentOnly": true}},
        {$emit: {connectionName: "__testLog"}}
    ],
    connections: [{name: "__testMemory", type: "in_memory", options: {}}],
    options: {}
}),
                             8661200);