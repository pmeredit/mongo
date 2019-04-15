/**
 * Basic set of tests to verify the response from mongocryptd for the explain command.
 */
(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const basicEncryptSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID()],
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ==")
                }
            }
        }
    };

    let cmds = [
        {find: "test", filter: {foo: 1}},
        {distinct: "test", query: {foo: 1}, key: "foo"},
        {count: "test", filter: {foo: 1}},
        {findAndModify: "test", query: {foo: 1.0}, update: {$inc: {score: 1.0}}},
        // old name
        {findandmodify: "test", query: {foo: 1.0}, update: {$inc: {score: 1.0}}},
        {aggregate: "test", pipeline: [{$match: {foo: 1}}]},
        {insert: "test", documents: [{foo: 1}]},
        {update: "test", updates: [{q: {foo: 1}, u: {"$set": {a: 2}}}]},
        {delete: "test", deletes: [{q: {foo: 1}, limit: 1}]},
    ];

    cmds.forEach(element => {
        // Make sure no json schema fails when explaining
        const explain_bad = {explain: 1, explain: element};  // eslint-disable-line no-dupe-keys
        assert.commandFailed(testDB.runCommand(explain_bad));

        const explain_cmd = {
            explain: element,
            jsonSchema: basicEncryptSchema,
            verbosity: "executionStats"
        };

        const explain_ret = assert.commandWorked(testDB.runCommand(explain_cmd));

        // NOTE: This mutates element so it now has jsonSchema
        Object.extend(element, {jsonSchema: basicEncryptSchema});
        const normal_ret = assert.commandWorked(testDB.runCommand(element));
        // Added by the shell.
        delete normal_ret["result"]["lsid"];
        assert.eq(explain_ret.hasEncryptionPlaceholders, normal_ret.hasEncryptionPlaceholders);
        assert.eq(explain_ret.schemaRequiresEncryption, normal_ret.schemaRequiresEncryption);
        assert.eq(explain_ret["result"]["explain"], normal_ret["result"]);
        assert.eq(explain_ret["result"]["verbosity"], "executionStats");

        // Test that an explain with the schema in the explained object fails.
        const misplaced_schema = {explain: element, jsonSchema: basicEncryptSchema};
        assert.commandFailedWithCode(testDB.runCommand(misplaced_schema), 30050);
    });

    mongocryptd.stop();
})();
