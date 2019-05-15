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
                    bsonType: "long"
                }
            }
        }
    };

    let cmds = [
        {find: "test", filter: {foo: NumberLong(1)}},
        {distinct: "test", query: {foo: NumberLong(1)}, key: "foo"},
        {count: "test", query: {foo: NumberLong(1)}},
        {findAndModify: "test", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}},
        // old name
        {findandmodify: "test", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}},
        {aggregate: "test", pipeline: [{$match: {foo: NumberLong(1)}}], cursor: {}},
        {insert: "test", documents: [{foo: NumberLong(1)}]},
        {update: "test", updates: [{q: {foo: NumberLong(1)}, u: {"$set": {a: 2}}}]},
        {delete: "test", deletes: [{q: {foo: NumberLong(1)}, limit: 1}]},
    ];

    cmds.forEach(element => {
        // Make sure no json schema fails when explaining
        const explainBad = {explain: 1, explain: element};  // eslint-disable-line no-dupe-keys
        assert.commandFailed(testDB.runCommand(explainBad));

        const explainCmd = {
            explain: element,
            jsonSchema: basicEncryptSchema,
            isRemoteSchema: false,
            verbosity: "executionStats"
        };

        const explainRes = assert.commandWorked(testDB.runCommand(explainCmd));

        Object.extend(element, {jsonSchema: basicEncryptSchema, isRemoteSchema: false});
        const normalRes = assert.commandWorked(testDB.runCommand(element));
        // Added by the shell.
        delete normalRes["result"]["lsid"];
        assert.eq(explainRes.hasEncryptionPlaceholders, normalRes.hasEncryptionPlaceholders);
        assert.eq(explainRes.schemaRequiresEncryption, normalRes.schemaRequiresEncryption);
        assert.eq(explainRes["result"]["explain"], normalRes["result"]);
        assert.eq(explainRes["result"]["verbosity"], "executionStats");

        // Test that an explain with the schema in the explained object fails.
        const misplacedSchema = {
            explain: element,
            jsonSchema: basicEncryptSchema,
            isRemoteSchema: false
        };
        assert.commandFailedWithCode(testDB.runCommand(misplacedSchema), 30050);
    });

    mongocryptd.stop();
})();
