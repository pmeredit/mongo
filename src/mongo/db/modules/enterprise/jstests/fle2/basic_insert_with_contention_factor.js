/**
 * Test encrypted insert contention factor works
 *
 * @tags: [
 * requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_insert_with_contention_factor';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "first",
                "bsonType": "string",
                "queries": {
                    "queryType": "equality",
                    "contention": NumberLong(2),
                }
            },
            {
                "path": "last",
                "bsonType": "string",
                "queries": {
                    "queryType": "equality",
                }
            },
        ]
    }
}));

const edb = client.getDB();

assert.commandWorked(
    edb.erunCommand({"insert": "basic", documents: [{"_id": 1, "first": "Bob", "last": "A"}]}));
assert.commandWorked(
    edb.erunCommand({"insert": "basic", documents: [{"_id": 2, "first": "Bob", "last": "B"}]}));
assert.commandWorked(
    edb.erunCommand({"insert": "basic", documents: [{"_id": 3, "first": "Tim", "last": "C"}]}));
assert.commandWorked(
    edb.erunCommand({"insert": "basic", documents: [{"_id": 4, "first": "Tim", "last": "D"}]}));

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "Bob", "last": "A"},
    {"_id": 2, "first": "Bob", "last": "B"},
    {"_id": 3, "first": "Tim", "last": "C"},
    {"_id": 4, "first": "Tim", "last": "D"},
]);

client.runEncryptionOperation(() => {
    {
        const docs = edb.basic.find({"first": "Bob"}).toArray().sort(function(a, b) {
            return a.last < b.last ? -1 : a.last > b.last ? 1 : 0;
        });
        assert.eq(docs.length, 2);
        assert.eq(docs[0]["last"], "A");
        assert.eq(docs[1]["last"], "B");
    } {const docs = edb.basic.find({"first": "Tim"}).toArray().sort(function(a, b) {
        return a.last < b.last ? -1 : a.last > b.last ? 1 : 0;
    });
       assert.eq(docs.length, 2);
       assert.eq(docs[0]["last"], "C");
       assert.eq(docs[1]["last"], "D");}

});