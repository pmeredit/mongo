// Validate fle accepts commands with and without json schema for non-encrypted commands.
// Also validates explain works
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const basicJSONSchema = {properties: {foo: {type: "string"}}};

    let cmds = [
        {find: "foo", filter: {_id: 1}},
        {distinct: "foo", query: {_id: 1}, key: "_id"},
        {count: "foo", filter: {_id: 1}},
        {findAndModify: "foo", query: {_id: 1.0}, update: {$inc: {score: 1.0}}},
        // old name
        {findandmodify: "foo", query: {_id: 1.0}, update: {$inc: {score: 1.0}}},
        {aggregate: "foo", pipeline: [{filter: {$eq: 1.0}}]},
        {insert: "foo", documents: [{a: 1}]},
        {update: "foo", updates: [{q: {a: 1}, u: {a: 2}}]},
        {delete: "foo", deletes: [{q: {a: 1}, limit: 1}]},
    ];

    const supportedCommands = ["find", "insert", "distinct"];

    cmds.forEach(element => {
        // Make sure no json schema fails
        assert.commandFailed(testDB.runCommand(element));

        // Make sure no json schema fails when explaining
        const explain_bad = {explain: 1, explain: element};  // eslint-disable-line no-dupe-keys
        assert.commandFailed(testDB.runCommand(explain_bad));

        // NOTE: This mutates element so it now has jsonSchema
        Object.extend(element, {jsonSchema: basicJSONSchema});

        // Make sure json schema works
        const ret1 = assert.commandWorked(testDB.runCommand(element));
        assert.eq(ret1.hasEncryptionPlaceholders, false);

        const explain_good = {
            explain: 1,
            explain: element,  // eslint-disable-line no-dupe-keys
        };

        // Make sure json schema works when explaining
        const ret2 = assert.commandWorked(testDB.runCommand(explain_good));
        assert.eq(ret2.hasEncryptionPlaceholders, false);

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

        // Merge the passthrough fields with the current command object.
        Object.assign(element, passthroughFields);

        const passthroughResult = assert.commandWorked(testDB.runCommand(element));

        if (Object.keys(element).some(field => supportedCommands.includes(field))) {
            // Command is supported, verify that each of the passthrough fields is included in the
            // result.
            for (let field in passthroughFields) {
                assert.eq(passthroughResult.result[field],
                          passthroughFields[field],
                          tojson(passthroughResult));
            }

            // The '$db' field is special as it's automatically added by the shell.
            assert.eq(passthroughResult.result.$db, "test", tojson(passthroughResult));
        } else {
            // Command is not supported yet, verify an empty 'result' in the response.
            assert.eq(passthroughResult.result, {}, tojson(passthroughResult));
        }
    });

    mongocryptd.stop();
})();
