/**
 * Tests for trim factor.
 *
 * @tags: [
 * assumes_read_preference_unchanged,
 * featureFlagQERangeV2,
 * ]
 */
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

Random.setRandomSeed();
const collName = "coll";

// Test that trim factor doesn't work for equality queries.
{
    const dbName = "edb_equality";
    const client = new EncryptedClient(db.getMongo(), dbName);
    const err = assert.throws(() => client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [{
                path: "field1",
                bsonType: "int",
                queries: [{queryType: "equality", contention: NumberInt(0), trimFactor: 1}]
            }]
        }
    }));
    assert.commandFailed(err);
    assert.eq(
        err.reason,
        "Client Side Field Level Encryption Error:The field 'trimFactor' is not allowed for equality index but is present");
}

let counter = 0;
function createClientWithEncryptedCollectionOrFail(type, tf, min, max, precision) {
    const dbName = "edb" + UUID().toString().split('"')[1];
    counter = counter + 1;
    const client = new EncryptedClient(db.getMongo(), dbName);
    const query = {queryType: "range", sparsity: 1, trimFactor: tf};
    if (precision !== undefined) {
        query.precision = precision;
    }
    if (min !== undefined) {
        query.min = min;
    }
    if (max !== undefined) {
        query.max = max;
    }
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [
                {path: "field1", bsonType: type, queries: query},
            ]
        }
    }));
    return client;
}

function runCreateCollectionTest(shouldSucceed, type, tf, min, max, precision) {
    if (shouldSucceed) {
        const client = createClientWithEncryptedCollectionOrFail(type, tf, min, max, precision);
        client.getDB().dropDatabase();
    } else {
        assert.throws(() =>
                          createClientWithEncryptedCollectionOrFail(type, tf, min, max, precision));
    }
}

function runCRUDTest(BSONType, type, tf, insertsPerDoc, min, max, precision) {
    const client = createClientWithEncryptedCollectionOrFail(type, tf, min, max, precision);
    const edb = client.getDB();
    const coll = edb[collName];

    for (let i = 0; i < 32; i++) {
        assert.commandWorked(
            edb.runCommand({insert: collName, documents: [{_id: i, field1: BSONType(i % 16)}]}));
        client.assertEncryptedCollectionCounts(
            collName, i + 1, (i + 1) * insertsPerDoc, (i + 1) * insertsPerDoc);
    }

    for (let _ = 0; _ < 50; _++) {
        const min = Random.randInt(15);
        const max = Random.randInt(15 - min) + min;
        const res = coll.find({field1: {$gte: BSONType(min), $lte: BSONType(max)}}).toArray();

        assert.eq(res.length, (max - min + 1) * 2);
        const values = res.map((r) => BSONType(r.field1));
        assertArrayEq({
            actual: values,
            expected: Array.from(new Array((max - min + 1) * 2),
                                 (x, i) => BSONType(Math.floor(i / 2) + min))
        });
    }

    assert.commandWorked(coll.deleteOne({field1: BSONType(1)}));
    // Delete doesn't remove ESC/ECOC entries
    client.assertEncryptedCollectionCounts(collName, 31, 32 * insertsPerDoc, 32 * insertsPerDoc);

    assert.commandWorked(edb.runCommand({
        update: collName,
        updates: [{q: {"field1": BSONType(1)}, u: {"$set": {"field1": BSONType(8)}}}]
    }));
    assert.eq(0, coll.find({field1: BSONType(1)}).toArray().length);
    assert.eq(3, coll.find({field1: BSONType(8)}).toArray().length);
}

runCreateCollectionTest(false, "int", -1, NumberInt(0), NumberInt(1));
runCreateCollectionTest(true, "int", 0, NumberInt(0), NumberInt(1));
runCreateCollectionTest(false, "int", 1, NumberInt(0), NumberInt(1));
runCreateCollectionTest(true, "int", 1, NumberInt(0), NumberInt(2));
runCreateCollectionTest(false, "int", 2, NumberInt(0), NumberInt(2));

const INTMIN = NumberInt("-2147483648");
const INTMAX = NumberInt("2147483647");
const LONGMIN = NumberLong("-9223372036854775808");
const LONGMAX = NumberLong("9223372036854775807");
// Note -- This is not the actual min/max of the date field, however our JS Date class is annoying
// with bounds so we can't test the technical bounds (LONGMIN, LONGMAX). However, this should
// suffice for showing that trim factor works for the date field correctly, even for values not
// representable by int32.
const DATEMIN = new Date(INTMIN * 2);
const DATEMAX = new Date(INTMAX * 2);

runCreateCollectionTest(true, "int", 31, INTMIN, INTMAX);
runCreateCollectionTest(false, "int", 32, INTMIN, INTMAX);

runCreateCollectionTest(true, "long", 63, LONGMIN, LONGMAX);
runCreateCollectionTest(false, "long", 64, LONGMIN, LONGMAX);

runCreateCollectionTest(true, "date", 32, DATEMIN, DATEMAX);
runCreateCollectionTest(false, "date", 33, DATEMIN, DATEMAX);

runCreateCollectionTest(true, "double", 63);
runCreateCollectionTest(false, "double", 64);

runCreateCollectionTest(true, "decimal", 127);
runCreateCollectionTest(false, "decimal", 128);

// Inserts per doc = (# bits in domain + 1) - TF
runCRUDTest(NumberInt, "int", 2, 3, NumberInt(0), NumberInt(15));
runCRUDTest(NumberInt, "int", 3, 2, NumberInt(0), NumberInt(15));
runCRUDTest(NumberInt, "int", 2, 4, NumberInt(-5), NumberInt(16));
runCRUDTest(NumberInt, "int", 4, 2, NumberInt(-5), NumberInt(16));
runCRUDTest(NumberLong, "long", 5, 60, LONGMIN, LONGMAX);
runCRUDTest((x) => new Date(NumberLong(x)), "date", 6, 28, DATEMIN, DATEMAX);
runCRUDTest((x) => x, "double", 7, 58);
runCRUDTest(NumberDecimal, "decimal", 8, 121);
