/**
 * Test encrypted insert works
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'modify_safe_content_errors';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

function runTest(bypassValidation, connection) {
    let fn;

    if (!bypassValidation) {
        fn = function(commandInvocation) {
            return assert.commandFailedWithCode(
                connection.runCommand(commandInvocation),
                [ErrorCodes.BadValue, ErrorCodes.DocumentValidationFailure, 6666200]);
        };
    } else {
        fn = function(commandInvocation) {
            return assert.commandWorked(connection.runCommand(commandInvocation));
        };
    }

    assert.commandWorked(edb.erunCommand({
        insert: "basic",
        documents: [{"first": "first1", "last": "last1"}],
        ordered: false,
    }));

    assert.commandWorked(edb.erunCommand({
        insert: "basic",
        documents: [{"first": "first2", "last": "last2"}],
        ordered: false,
    }));

    assert.commandWorked(edb.erunCommand({
        insert: "basic",
        documents: [{"first": "first3", "last": "last3"}],
        ordered: false,
    }));

    fn({
        insert: "basic",
        documents: [{"first": "shreyas", "item": "insert1", "__safeContent__": ["safeContent"]}],
        ordered: false,
        bypassDocumentValidation: bypassValidation,
    });

    fn({
        insert: "basic",
        documents: [{"first": "erwin", "item": "insert2", "__safeContent__": ["safeContent"]}],
        ordered: false,
        bypassDocumentValidation: bypassValidation,
    });

    fn({
        findAndModify: "basic",
        query: {"last": "last1"},
        update: {$set: {"item": "update1", "__safeContent__": ["field1"]}},
        bypassDocumentValidation: bypassValidation,
    });

    fn({
        findAndModify: "basic",
        query: {"last": "last3"},
        update: {"item": "update3", "__safeContent__": ["field3"]},
        bypassDocumentValidation: bypassValidation,
    });

    fn({
        update: "basic",
        updates: [{
            q: {"last": "last2"},
            u: {$set: {"item": "update2", "__safeContent__": ["field2"]}},
        }],
        bypassDocumentValidation: bypassValidation,
    });
}

// Have an unencrypted connection to test with.
let edb = client.getDB(dbName);
let testdb = db.getSiblingDB(dbName);

const bypassValidationOptions = [false, true];
const connectionOptions = [testdb, edb];

for (const validationOption of bypassValidationOptions) {
    let count = 0;
    for (const connectionOption of connectionOptions) {
        assert.commandWorked(client.createEncryptionCollection("basic", {
            encryptedFields: {
                "fields": [
                    {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                ]
            }
        }));

        let connType = "encrypted";
        if (count == 0) {
            connType = "regular";
        }
        count++;

        jsTest.log(`ValidationOption: ${validationOption}, ConnectionOption: ${connType}`);

        runTest(validationOption, connectionOption);

        // Clean up to reset for the next iteration.
        testdb.basic.drop();
        testdb.enxcol_.basic.esc.drop();
        testdb.enxcol_.basic.ecoc.drop();
    }
}
