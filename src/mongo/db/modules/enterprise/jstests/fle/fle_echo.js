// Validate fle accepts commands with and without json schema for non-encrypted commands.
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const basicJSONSchema = {properties: {foo: {type: "string"}}};
    const basicEncryptSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")],
                    bsonType: "double"
                }
            }
        }
    };

    let cmds = [
        {cmdName: "find", cmd: {find: "foo", filter: {_id: 1}}},
        {cmdName: "distinct", cmd: {distinct: "foo", query: {_id: 1}, key: "_id"}},
        {cmdName: "count", cmd: {count: "foo", query: {_id: 1}}},
        {
          cmdName: "findAndModify",
          cmd: {findAndModify: "foo", query: {foo: 1}, update: {$inc: {score: 1.0}}}
        },
        // old name
        {
          cmdName: "findAndModify",
          cmd: {findandmodify: "foo", query: {foo: 1}, update: {$inc: {score: 1.0}}}
        },
        {cmdName: "aggregate", cmd: {aggregate: "foo", pipeline: [{$match: {foo: 1}}], cursor: {}}},
        {cmdName: "insert", cmd: {insert: "foo", documents: [{foo: 1}]}},
        {cmdName: "update", cmd: {update: "foo", updates: [{q: {foo: 1}, u: {"$set": {a: 2}}}]}},
        {cmdName: "delete", cmd: {delete: "foo", deletes: [{q: {foo: 1}, limit: 1}]}}
    ];

    cmds.forEach(element => {
        // Make sure no json schema fails
        assert.commandFailed(testDB.runCommand(element.cmd));

        // NOTE: This mutates element so it now has jsonSchema
        Object.extend(element.cmd, {jsonSchema: basicJSONSchema});

        // Make sure json schema works
        const ret1 = assert.commandWorked(testDB.runCommand(element.cmd));
        assert.eq(ret1.hasEncryptionPlaceholders, false, ret1);
        assert.eq(ret1.schemaRequiresEncryption, false, ret1);

        // Test that generic "passthrough" command arguments are correctly echoed back from
        // mongocryptd.
        const passthroughFields = {
            "writeConcern": {w: "majority", wtimeout: 5000},
            "$audit": "auditString",
            "$client": "clientString",
            "$configServerState": 2,
            "allowImplicitCollectionCreation": true,
            "$oplogQueryData": false,
            "$readPreference": {"mode": "primary"},
            "$replData": {data: "here"},
            "$clusterTime": "now",
            "maxTimeMS": 500,
            "readConcern": {level: "majority"},
            "databaseVersion": 2.2,
            "shardVersion": 4.6,
            "tracking_info": {"found": "it"},
            "txnNumber": 7,
            "autocommit": false,
            "coordinator": false,
            "startTransaction": "now",
            "stmtId": NumberInt(1),
            "lsid": 1,
        };

        // Switch to the schema containing encrypted fields.
        Object.assign(element.cmd, {jsonSchema: basicEncryptSchema});

        // Merge the passthrough fields with the current command object.
        Object.assign(element.cmd, passthroughFields);

        const passthroughResult = assert.commandWorked(testDB.runCommand(element.cmd));

        // Verify that each of the passthrough fields is included in the result.
        for (let field in passthroughFields) {
            assert.eq(passthroughResult.result[field], passthroughFields[field], passthroughResult);
        }

        // Verify that the 'schemaRequiresEncryption' bit is correctly set.
        assert.eq(passthroughResult.schemaRequiresEncryption, true, passthroughResult);

        // The '$db' field is always removed by cryptd.
        assert(!passthroughResult.hasOwnProperty("$db"));

        // Commands name should hold the collection name
        assert.eq(passthroughResult.result[element.cmdName], "foo", passthroughResult);
    });

    mongocryptd.stop();
})();
