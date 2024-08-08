/**
 * Test encrypted range CRUD works with various domain sizes & tuning
 *
 * @tags: [
 *   requires_fcv_81,
 *   assumes_read_concern_unchanged,
 *   directly_against_shardsvrs_incompatible,
 *   assumes_read_preference_unchanged,
 * ]
 */
import {
    assertIsEqualityIndexedEncryptedField,
    assertIsRangeIndexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";

const DECIMAL128_MAX_SAFE_INTEGER = NumberDecimal("9999999999999999999999999999999999");  // 10^34-1
const DECIMAL128_MIN_SAFE_INTEGER = NumberDecimal("-9999999999999999999999999999999999");

const toNumberInt = (x) => { return (x === undefined) ? undefined : NumberInt(x); };
const toNumberLong = (x) => { return (x === undefined) ? undefined : NumberLong(x); };
const toNumberDouble = (x) => { return (x === undefined) ? undefined : x; };
const toNumberDecimal = (x) => { return (x === undefined) ? undefined : NumberDecimal(x); };

const nextDoubleEpsilon = (x) => {
    // Returns the next smallest double value greater than x,
    // calculated as (x + scaled_epsilon) where scaled_epsilon
    // is the result of (x * Number.EPSILON) with the mantissa
    // bits zeroed out.
    const buf = new ArrayBuffer(10);
    const view = new DataView(buf);
    view.setFloat64(0, x * Number.EPSILON);
    view.setUint8(1, view.getUint8(1) & 0xF0);
    view.setFloat64(2, 0);
    let next = x + Math.abs(view.getFloat64(0));
    assert.gt(next, x);
    return next;
};
const nextDoublePrc0 = (x) => { return x + 1; };
const nextDoublePrc1 = (x) => { return x + 0.1; };
const nextDoublePrc5 = (x) => { return x + 0.00001; };
const nextDoublePrc10 = (x) => { return x + 0.0000000001; };

function expectedEdges(bitlen, sparsity, trimFactor) {
    let sp = (sparsity === undefined) ? 2 : sparsity;
    let tf = (trimFactor === undefined) ? (Math.min(bitlen - 1, 6)) : trimFactor;
    assert.gt(sp, 0);
    assert.gt(bitlen, 0);
    assert.lt(tf, bitlen);
    assert.gte(tf, 0);

    let filteredEdges = Math.floor(bitlen / sp) + ((bitlen % sp) != 0 ? 1 : 0);  // includes leaf
    let trimmedEdges = Math.max(0, Math.floor((tf - 1) / sp));
    let root = (tf > 0 ? 0 : 1);
    return filteredEdges - trimmedEdges + root;
}

const tiny_domain_tests = {
    bitlen: 4,        // default tf = 3
    startValue: 0x4,  // insert values [4..11]
    stepCount: 8,
    tests: [
        {min: 0, max: 0xF, sparsity: 1, edges: 2},
        {min: 0, max: 0xF, sparsity: 1, trimFactor: 2, edges: 3},
        {min: 0, max: 0xF, edges: 1},
        {min: 0, max: 0xF, sparsity: 3, edges: 2},
        {min: 0, max: 0xF, sparsity: 4, edges: 1},
    ]
};

const small_domain_tests = {
    bitlen: 6,       // default tf = 5
    startValue: 16,  // insert values [16..47]
    stepCount: 32,
    tests: [
        {min: 0, max: 0x3F, sparsity: 1, edges: 2},
        {min: 0, max: 0x3F, sparsity: 1, trimFactor: 3, edges: 4},
        {min: 0, max: 0x3F, edges: 1},
    ]
};

const medium_domain_tests = {
    bitlen: 17,
    startValue: -0xF,  // insert values [-0xF..0xF]
    stepCount: 31,
    tests: [
        {min: -0xFFFF, max: 0xFFFF, sparsity: 1, edges: 12},
        {min: -0xFFFF, max: 0xFFFF, sparsity: 1, trimFactor: 8, edges: 10},
        {min: -0xFFFF, max: 0xFFFF, edges: 7},
        {min: -0xFFFF, max: 0xFFFF, sparsity: 4, edges: 4},
    ]
};

const int_large_domain_tests = {
    bitlen: 31,
    startValue: 16,  // insert values [16..47]
    stepCount: 32,
    tests: [
        {min: 0, sparsity: 1, edges: 26},
        {min: 0, sparsity: 1, trimFactor: 15, edges: 17},
        {min: 0, edges: 14},
    ]
};

const long_large_domain_tests = {
    bitlen: 63,
    startValue: 16,  // insert values [16..47]
    stepCount: 32,
    tests: [
        {min: 0, sparsity: 1, edges: 58},
        // note: if tf > 17, this runs into the BSON limit for minCover
        {min: 0, sparsity: 1, trimFactor: 17, edges: 47},
        {min: 0, edges: 30},
    ]
};

const int_unbounded_domain_tests = {
    bitlen: 32,
    startValue: 16,  // insert values [16..47]
    stepCount: 32,
    tests: [
        {sparsity: 1, edges: 27},
        {sparsity: 1, trimFactor: 16, edges: 17},
        {edges: 14},
    ]
};

const long_unbounded_domain_tests = {
    bitlen: 64,
    startValue: 16,  // insert values [16..47]
    stepCount: 32,
    tests: [
        {sparsity: 1, edges: 59},
        {sparsity: 1, trimFactor: 17, edges: 48},
        {edges: 30},
        {sparsity: 5, edges: 12},
        {sparsity: 6, edges: 11},
        {sparsity: 7, edges: 10},
        {sparsity: 8, edges: 8},
    ]
};

const double_unbounded_domain_tests = {
    bitlen: 64,
    startValue: 1,
    stepCount: 20,
    stepFunc: nextDoubleEpsilon,
    tests: [
        {sparsity: 1, edges: 59},
        {sparsity: 1, trimFactor: 17, edges: 48},
        {edges: 30},
    ]
};

// precision = 0 (integers only)
// domain size = 53 bits (all integers b/w -2^52 and 2^52-1
const double_integer_domain_tests = {
    bitlen: 53,
    startValue: 1,
    stepCount: 11,
    stepFunc: nextDoublePrc0,
    tests: [
        {min: -4503599627370496, max: 4503599627370495, precision: 0, sparsity: 1, edges: 48},
        {
            min: -4503599627370496,
            max: 4503599627370495,
            precision: 0,
            sparsity: 1,
            trimFactor: 17,
            edges: 37
        },
        {min: -4503599627370496, max: 4503599627370495, precision: 0, edges: 25},
    ]
};

// precision = 1
// domain size = 7 bits (0 thru 10x10^1 => 100 - 0 + 10^prc = 110 values),
const double_small_domain_tests = {
    bitlen: 7,
    startValue: 1,
    stepCount: 11,
    stepFunc: nextDoublePrc1,
    tests: [
        {min: 0, max: 10, precision: 1, sparsity: 1, edges: 2},
        {min: 0, max: 10, precision: 1, sparsity: 1, trimFactor: 3, edges: 5},
        {min: 0, max: 10, precision: 1, edges: 2},
    ]
};

// precision = 5
// domain size = 21 bits (0 thru 10x10^5 = 1000000 - 0 + 10^prc = 1100000 values),
const double_medium_domain_tests = {
    bitlen: 21,
    startValue: 3,
    stepCount: 100,
    stepFunc: nextDoublePrc5,
    tests: [
        {min: 0, max: 10, precision: 5, sparsity: 1, edges: 16},
        {min: 0, max: 10, precision: 5, sparsity: 1, trimFactor: 10, edges: 12},
        {min: 0, max: 10, precision: 5, edges: 9},
    ]
};

// precision = 10
// domain size = 37 bits (0 thru 10x10^10 = 100,000,000,000 - 0 + 10^prc = 1.1x10^11
const double_large_domain_tests = {
    bitlen: 37,
    startValue: 3,
    stepCount: 100,
    stepFunc: nextDoublePrc10,
    tests: [
        {min: 0, max: 10, precision: 10, sparsity: 1, edges: 32},
        {min: 0, max: 10, precision: 10, sparsity: 1, trimFactor: 17, edges: 21},
        {min: 0, max: 10, precision: 10, edges: 17},
    ]
};

const decimal_unbounded_domain_tests = {
    bitlen: 128,
    startValue: 1,
    stepCount: 20,
    stepFunc: nextDoublePrc0,
    tests: [
        {sparsity: 1, edges: 123},
        {sparsity: 1, trimFactor: 17, edges: 112},
        {edges: 62},
    ]
};

// precision = 1
// domain size = 7 bits (0 thru 10x10^1 => 100 - 0 + 10^prc = 110 values),
const decimal_small_domain_tests = {
    bitlen: 7,
    startValue: 1,
    stepCount: 11,
    stepFunc: nextDoublePrc1,
    tests: [
        {min: 0, max: 10, precision: 1, sparsity: 1, edges: 2},
        {min: 0, max: 10, precision: 1, sparsity: 1, trimFactor: 3, edges: 5},
        {min: 0, max: 10, precision: 1, edges: 2},
    ]
};

// precision = 5
// domain size = 21 bits (0 thru 10x10^5 = 1000000 - 0 + 10^prc = 1100000 values),
const decimal_medium_domain_tests = {
    bitlen: 21,
    startValue: 3,
    stepCount: 100,
    stepFunc: nextDoublePrc5,
    tests: [
        {min: 0, max: 10, precision: 5, sparsity: 1, edges: 16},
        {min: 0, max: 10, precision: 5, sparsity: 1, trimFactor: 10, edges: 12},
        {min: 0, max: 10, precision: 5, edges: 9},
    ]
};

// precision = 10
// domain size = 37 bits (0 thru 10x10^10 = 100,000,000,000 - 0 + 10^prc = 1.1x10^11
const decimal_large_domain_tests = {
    bitlen: 37,
    startValue: 3,
    stepCount: 100,
    stepFunc: nextDoublePrc10,
    tests: [
        {min: 0, max: 10, precision: 10, sparsity: 1, edges: 32},
        {min: 0, max: 10, precision: 10, sparsity: 1, trimFactor: 17, edges: 21},
        {min: 0, max: 10, precision: 10, edges: 17},
    ]
};

function runTestSuite(testSuite, bsonType, typeConvertFn) {
    const dbName = "basic_crud_range";
    jsTestLog(`Running test suite for type ${bsonType}: ${tojson(testSuite)}`);

    for (let test of testSuite.tests) {
        db.getSiblingDB(dbName).dropDatabase();
        const client = new EncryptedClient(db.getMongo(), dbName);
        const edb = client.getDB();

        assert.commandWorked(client.createEncryptionCollection("basic", {
            encryptedFields: {
                fields: [{
                    path: "field1",
                    bsonType: bsonType,
                    queries: {
                        queryType: "range",
                        min: typeConvertFn(test.min),
                        max: typeConvertFn(test.max),
                        precision: test.precision,
                        sparsity: test.sparsity,
                        trimFactor: test.trimFactor
                    }
                }]
            }
        }));

        assert.eq(test.edges, expectedEdges(testSuite.bitlen, test.sparsity, test.trimFactor));

        let edcCount = 0;
        let escCount = 0;
        let ecocCount = 0;
        let steps = (test.stepCount === undefined) ? testSuite.stepCount : test.stepCount;

        let stepFunc = (test.stepFunc !== undefined)
            ? test.stepFunc
            : (testSuite.stepFunc !== undefined ? testSuite.stepFunc : ((x) => { return x + 1; }));

        let startValue = (test.startValue === undefined) ? testSuite.startValue : test.startValue;
        let endValue = startValue;
        let currentValue = startValue;

        for (let i = 0; i < steps; i++) {
            let res = assert.commandWorked(edb.erunCommand({
                insert: "basic",
                documents: [{_id: NumberInt(i), field1: typeConvertFn(currentValue)}]
            }));

            assert.eq(res.n, 1);
            edcCount++;
            escCount += test.edges;
            ecocCount += test.edges;
            client.assertEncryptedCollectionCounts("basic", edcCount, escCount, ecocCount);
            endValue = currentValue;
            currentValue = stepFunc(currentValue);
            res = edb.basic.efindOne({_id: NumberInt(i)});
            assert.eq(res.field1, typeConvertFn(endValue));
        }

        // validate find works
        client.runEncryptionOperation(() => {
            const res =
                edb.basic
                    .find(
                        {field1: {$gte: typeConvertFn(startValue), $lte: typeConvertFn(endValue)}},
                        {__safeContent__: 0})
                    .sort({_id: 1})
                    .toArray();
            assert.eq(res.length, edcCount);
        });

        // validate update works (shift values after startValue down one step)
        let previousValue = startValue;
        currentValue = stepFunc(startValue);
        for (let i = 1; i < steps; i++) {
            const cmd = {
                update: "basic",
                updates: [{
                    q: {
                        field1: {
                            $gte: typeConvertFn(currentValue),
                            $lt: typeConvertFn(stepFunc(currentValue))
                        }
                    },
                    u: {$set: {field1: typeConvertFn(previousValue)}}
                }]
            };
            previousValue = currentValue;
            currentValue = stepFunc(currentValue);
            let res = assert.commandWorked(edb.erunCommand(cmd));
            assert.eq(res.nModified, 1);
            escCount += test.edges;
            ecocCount += test.edges;
            client.assertEncryptedCollectionCounts("basic", edcCount, escCount, ecocCount);
        }

        // validate find works
        client.runEncryptionOperation(() => {
            const res = edb.basic
                            .find({
                                field1: {
                                    $gte: typeConvertFn(stepFunc(startValue)),
                                    $lte: typeConvertFn(endValue)
                                }
                            },
                                  {__safeContent__: 0})
                            .sort({_id: 1})
                            .toArray();
            assert.eq(res.length, edcCount - 2);
        });

        // validate compact works
        client.runEncryptionOperation(() => { assert.commandWorked(edb.basic.compact()); });
        client.assertESCNonAnchorCount("basic", 0);
        client.assertStateCollectionsAfterCompact("basic", true);
        const escNs = client.getStateCollectionNamespaces("basic").esc;
        escCount = edb.getCollection(escNs).countDocuments({});
        ecocCount = 0;
        client.assertEncryptedCollectionCounts("basic", edcCount, escCount, ecocCount);

        // validate findAndModify works (shift values back +1)
        currentValue = startValue;
        for (let i = 1; i < steps; i++) {
            let res = assert.commandWorked(edb.erunCommand({
                findAndModify: "basic",
                query: {
                    field1: {
                        $gte: typeConvertFn(currentValue),
                        $lt: typeConvertFn(stepFunc(currentValue))
                    }
                },
                update: {$set: {field1: typeConvertFn(stepFunc(currentValue))}}
            }));
            currentValue = stepFunc(currentValue);
            escCount += test.edges;
            ecocCount += test.edges;
            client.assertEncryptedCollectionCounts("basic", edcCount, escCount, ecocCount);
        }

        // validate find works
        client.runEncryptionOperation(() => {
            const res2 = edb.basic.find({}, {__safeContent__: 0}).sort({_id: 1}).toArray();
            const res =
                edb.basic
                    .find(
                        {field1: {$gte: typeConvertFn(startValue), $lte: typeConvertFn(endValue)}},
                        {__safeContent__: 0})
                    .sort({_id: 1})
                    .toArray();
            assert.eq(res.length, edcCount);
        });

        // validate cleanup works
        client.runEncryptionOperation(() => { assert.commandWorked(edb.basic.cleanup()); });
        client.assertESCNonAnchorCount("basic", 0);
        client.assertStateCollectionsAfterCompact("basic", true);
        escCount = edb.getCollection(escNs).countDocuments({});
        ecocCount = 0;
        client.assertEncryptedCollectionCounts("basic", edcCount, escCount, ecocCount);
    }
}

runTestSuite(tiny_domain_tests, "int", toNumberInt);
runTestSuite(small_domain_tests, "int", toNumberInt);
runTestSuite(medium_domain_tests, "int", toNumberInt);
runTestSuite(int_large_domain_tests, "int", toNumberInt);
runTestSuite(int_unbounded_domain_tests, "int", toNumberInt);

runTestSuite(tiny_domain_tests, "long", toNumberLong);
runTestSuite(small_domain_tests, "long", toNumberLong);
runTestSuite(medium_domain_tests, "long", toNumberLong);
runTestSuite(long_large_domain_tests, "long", toNumberLong);
runTestSuite(long_unbounded_domain_tests, "long", toNumberLong);

runTestSuite(double_unbounded_domain_tests, "double", toNumberDouble);
runTestSuite(double_integer_domain_tests, "double", toNumberDouble);
runTestSuite(double_small_domain_tests, "double", toNumberDouble);
runTestSuite(double_medium_domain_tests, "double", toNumberDouble);
runTestSuite(double_large_domain_tests, "double", toNumberDouble);

runTestSuite(decimal_unbounded_domain_tests, "decimal", toNumberDecimal);
runTestSuite(decimal_small_domain_tests, "decimal", toNumberDecimal);
runTestSuite(decimal_medium_domain_tests, "decimal", toNumberDecimal);
runTestSuite(decimal_large_domain_tests, "decimal", toNumberDecimal);
