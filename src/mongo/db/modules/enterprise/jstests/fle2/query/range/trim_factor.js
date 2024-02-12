/**
 * Tests for trim factor.
 *
 * @tags: [
 * featureFlagQERangeV2
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = jsTestName();
let dbTest = db.getSiblingDB(dbName);
let client = new EncryptedClient(db.getMongo(), dbName);
const collName = "coll";

function runTest(shouldSucceed, type, tf, min, max, precision) {
    const query = {queryType: "rangePreview", sparsity: 1, trimFactor: tf};
    if (precision !== undefined) {
        query.precision = precision;
    }
    if (min !== undefined) {
        query.min = min;
    }
    if (max !== undefined) {
        query.max = max;
    }
    dbTest.dropDatabase();
    const f = () => client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [
                {path: "field1", bsonType: type, queries: query},
            ]
        }
    });
    if (shouldSucceed) {
        assert.commandWorked(f());
    } else {
        let res;
        try {
            res = f();
        } catch {
            return;
        }
        // Didn't catch, fail
        assert(
            false,
            "Expected an error when creating encrypted collection, did not catch one. Got result:\n" +
                tojson(res));
    }
}

runTest(false, "int", -1, NumberInt(0), NumberInt(1));
runTest(true, "int", 0, NumberInt(0), NumberInt(1));
runTest(false, "int", 1, NumberInt(0), NumberInt(1));
runTest(true, "int", 1, NumberInt(0), NumberInt(2));
runTest(false, "int", 2, NumberInt(0), NumberInt(2));

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

jsTest.log(DATEMIN);
jsTest.log(DATEMAX);
runTest(true, "int", 31, INTMIN, INTMAX);
runTest(false, "int", 32, INTMIN, INTMAX);

runTest(true, "long", 63, LONGMIN, LONGMAX);
runTest(false, "long", 64, LONGMIN, LONGMAX);

runTest(true, "date", 32, DATEMIN, DATEMAX);
runTest(false, "date", 33, DATEMIN, DATEMAX);

runTest(true, "double", 63);
runTest(false, "double", 64);
