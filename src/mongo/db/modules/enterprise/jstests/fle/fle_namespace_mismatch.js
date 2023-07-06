/**
 * Test that mongocryptd produces an error when the namespace given in
 * encryptionInfo does not match the one given in the command.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {kRandomAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

if (fle2Enabled()) {
    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const notTestDb = conn.getDB("test2");
    const coll = testDb.fle_namespace_mismatch;
    const notColl = testDb.fle_namespace_mismatch2;

    const fooRandomEncryptionSchema = generateSchema(
        {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}}},
        coll.getFullName());

    // Writes against the wrong namespace fail.
    assert.commandFailedWithCode(testDb.runCommand(Object.assign({
        insert: notColl.getName(),
        documents: [{"foo": "bar"}],
    },
                                                                 fooRandomEncryptionSchema)),
                                 6411900);
    assert.commandFailedWithCode(notTestDb.runCommand(Object.assign({
        insert: coll.getName(),
        documents: [{"foo": "bar"}],
    },
                                                                    fooRandomEncryptionSchema)),
                                 6411900);

    // Reads against the wrong namespace fail.
    assert.commandFailedWithCode(testDb.runCommand(Object.assign({
        find: notColl.getName(),
        filter: {foo: {$eq: NumberInt(3)}},
    },
                                                                 fooRandomEncryptionSchema)),
                                 6411900);
    assert.commandFailedWithCode(notTestDb.runCommand(Object.assign({
        find: coll.getName(),
        filter: {foo: {$eq: NumberInt(3)}},
    },
                                                                    fooRandomEncryptionSchema)),
                                 6411900);

    mongocryptd.stop();
}
