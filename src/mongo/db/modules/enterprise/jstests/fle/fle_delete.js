/**
 * Test that mongocrypt can correctly mark the delete command with intent-to-encrypt placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.fle_delete;

    const schema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID(), UUID()],
                }
            }
        },
        patternProperties: {
            bar: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID(), UUID()],
                }
            }
        }
    };

    // Verify that this delete command is correctly marked for encryption.
    let deleteCmd = {
        delete: coll.getName(),
        deletes: [
            {q: {foo: 1, barx: 2, baz: 3}, limit: 1},
            {q: {x: 1, foobar: 1}, limit: 0},
        ],
        jsonSchema: schema
    };

    let cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.delete, cmdRes);
    assert.eq(false, cmdRes.result.bypassDocumentValidation, cmdRes);
    assert.eq(true, cmdRes.result.ordered, cmdRes);
    assert(cmdRes.result.hasOwnProperty("deletes"), cmdRes);
    assert.eq(2, cmdRes.result.deletes.length, cmdRes);

    let firstDelete = cmdRes.result.deletes[0];
    assert.eq(1, firstDelete.limit, cmdRes);
    assert(firstDelete.hasOwnProperty("q"), cmdRes);
    assert(firstDelete.q.hasOwnProperty("$and"), cmdRes);
    assert.eq(3, firstDelete.q.$and.length, cmdRes);
    assert(firstDelete.q.$and[0].hasOwnProperty("foo"), cmdRes);
    assert(firstDelete.q.$and[0].foo.$eq instanceof BinData, cmdRes);
    assert(firstDelete.q.$and[1].hasOwnProperty("barx"), cmdRes);
    assert(firstDelete.q.$and[1].barx.$eq instanceof BinData, cmdRes);
    assert(firstDelete.q.$and[2].hasOwnProperty("baz"), cmdRes);
    assert.eq(3, firstDelete.q.$and[2].baz.$eq, cmdRes);

    let secondDelete = cmdRes.result.deletes[1];
    assert.eq(0, secondDelete.limit, cmdRes);
    assert(secondDelete.hasOwnProperty("q"), cmdRes);
    assert(secondDelete.q.hasOwnProperty("$and"), cmdRes);
    assert.eq(2, secondDelete.q.$and.length, cmdRes);
    assert(secondDelete.q.$and[0].hasOwnProperty("x"), cmdRes);
    assert.eq(1, secondDelete.q.$and[0].x.$eq, cmdRes);
    assert(secondDelete.q.$and[1].hasOwnProperty("foobar"), cmdRes);
    assert(secondDelete.q.$and[1].foobar.$eq instanceof BinData, cmdRes);

    // Negative test to make sure that 'hasEncryptionPlaceholders' is set to false when no fields
    // are marked for encryption.
    deleteCmd = {
        delete: coll.getName(),
        deletes: [
            {q: {w: 1, x: {$in: [1, 2]}}, limit: 1},
            {q: {y: 1, z: {foo: 1}}, limit: 0},
        ],
        jsonSchema: schema
    };

    cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);

    // Test a delete with an empty 'deletes' array.
    deleteCmd = {delete: coll.getName(), deletes: [], jsonSchema: schema};
    cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq([], cmdRes.result.deletes, cmdRes);

    mongocryptd.stop();
}());
