// Validate fle accepts commands with and without json schema for non-encrypted commands.
// Also validates explain works
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const basicJSONSchema = {properties: {foo: {type: "string"}}};

    let cmds = [
        {find: "db.foo", filter: {_id: 1}},
        {distinct: "db.foo", filter: {_id: 1}},
        {count: "db.foo", filter: {_id: 1}},
        {findAndModify: "foo", query: {_id: 1.0}, update: {$inc: {score: 1.0}}},
        // old name
        {findandmodify: "foo", query: {_id: 1.0}, update: {$inc: {score: 1.0}}},
        {aggregate: "test.foo", pipeline: [{filter: {$eq: 1.0}}]},

        {insert: "test.foo", documents: [{a: 1}]},
        {update: "test.foo", updates: [{q: {a: 1}, u: {a: 2}}]},
        {delete: "test.foo", deletes: [{q: {a: 1}, limit: 1}]},
    ];

    cmds.forEach(element => {
        // Make sure no json schema fails
        assert.commandFailed(conn.adminCommand(element));

        // Make sure no json schema fails when explaining
        const explain_bad = {explain: 1, explain: element};  // eslint-disable-line no-dupe-keys
        assert.commandFailed(conn.adminCommand(explain_bad));

        // NOTE: This mutates element so it now has jsonSchema
        Object.extend(element, {jsonSchema: basicJSONSchema});

        // Make sure json schema works
        const ret1 = assert.commandWorked(conn.adminCommand(element));
        assert.eq(ret1.hasEncryptionPlaceholders, false);

        const explain_good = {
            explain: 1,
            explain: element,  // eslint-disable-line no-dupe-keys
        };

        // Make sure json schema works when explaining
        const ret2 = assert.commandWorked(conn.adminCommand(explain_good));
        assert.eq(ret2.hasEncryptionPlaceholders, false);
    });

    mongocryptd.stop();
})();
