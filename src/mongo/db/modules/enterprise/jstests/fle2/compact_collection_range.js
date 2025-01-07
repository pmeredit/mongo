// Verify compact collection capability with range fields

/**
 * @tags: [
 * assumes_read_concern_unchanged,
 * directly_against_shardsvrs_incompatible,
 * assumes_unsharded_collection,
 * requires_fcv_80,
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";

const kDBName = 'compact_collection_db';
const kCollName = 'encrypted';

const kRangeQuery = "range";
const kSampleEncryptedFields = {
    fields: [
        {
            path: "age",
            bsonType: "int",
            queries: {
                "queryType": kRangeQuery,
                min: NumberInt(0),
                max: NumberInt(0x7FFFFFFF),
                sparsity: 1,
                trimFactor: 0,
            }
        },
        {
            path: "balance",
            bsonType: "double",
            queries: {
                queryType: kRangeQuery,
                min: -1000000.0,
                max: 1000000.0,
                precision: 9,
                sparsity: 3,
                trimFactor: 0
            }
        },
    ],
};
const kUnboundedRangeQuery = {
    queryType: kRangeQuery,
    sparsity: 1,
    trimFactor: 0
};
const kUnboundedRangeEncryptedFields = {
    fields: [
        {path: "int", bsonType: "int", queries: kUnboundedRangeQuery},
        {path: "long", bsonType: "long", queries: kUnboundedRangeQuery},
        {path: "date", bsonType: "date", queries: kUnboundedRangeQuery},
        {path: "double", bsonType: "double", queries: kUnboundedRangeQuery},
        {path: "decimal", bsonType: "decimal", queries: kUnboundedRangeQuery},
    ],
};

function getEncryptionInformation(edb) {
    const infos = edb.getCollectionInfos({name: kCollName});
    const ei = {schema: {}};
    ei.schema[`${kDBName}.${kCollName}`] = infos[0].options.encryptedFields;
    jsTest.log(ei);
    return ei;
}

jsTest.log("Test compaction using default anchorPaddingFactor");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    client.assertEncryptedCollectionCounts(coll.getName(), 1, 50, 50);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(coll.getName(), 1, 50, 0);
});

jsTest.log("Test compaction of a single insert");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    client.assertEncryptedCollectionCounts(coll.getName(), 1, 50, 50);

    assert.commandWorked(coll.compact({anchorPaddingFactor: 1.0}));
    client.assertEncryptedCollectionCounts(coll.getName(), 1, 50, 0);

    assert.commandWorked(coll.cleanup());
});

jsTest.log("Test valid range compaction padding factors");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    [0.0, 0.1, 0.42, 0.5, 0.75, 0.999].forEach(function(apf) {
        assert.commandWorked(coll.insert({age: NumberInt(0), balance: 0.0}));
        assert.commandWorked(coll.compact({anchorPaddingFactor: apf}));
    });
});

jsTest.log("Add a second similar record, and a unique one");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 1000.0}));
    assert.commandWorked(coll.insert({age: NumberInt(40), balance: 2000.0}));
    assert.commandWorked(coll.compact({anchorPaddingFactor: 1.0}));
});

jsTest.log("Try to compact at an invalid padding factor");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    client.assertEncryptedCollectionCounts(coll.getName(), 1, 50, 50);

    [-1.0, -0.5, -0.001, 1.001, 1000.0, "hotdog", [], {}].forEach(function(apf) {
        assert.commandFailed(coll.compact({anchorPaddingFactor: apf}));
    });
});

jsTest.log("Test compact with explicit compactionTokens");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    const tokens = coll._getCompactionTokens();

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assert.commandWorked(coll.compact({compactionTokens: tokens}));

    // Unknown tokens ignored.
    const extraTokens =
        Object.extend({dummy: BinData(0, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")}, tokens);
    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assert.commandWorked(coll.compact({compactionTokens: tokens}));
    assert.commandWorked(coll.cleanup({cleanupTokens: tokens}));
});

jsTest.log("Test compact with missing compactionTokens");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    const kMissingToken = 7294900;
    const tokens = coll._getCompactionTokens();

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assert.commandFailedWithCode(coll.compact({compactionTokens: {}}), kMissingToken);
    assert.commandFailedWithCode(coll.compact({compactionTokens: {age: tokens.age}}),
                                 kMissingToken);
    assert.commandFailedWithCode(coll.compact({compactionTokens: {balance: tokens.balance}}),
                                 kMissingToken);
});

const kInvalidSearchedESCPositionsError = 7666502;
function assertInvalidToken(res) {
    assert.commandFailedWithCode(res, [ErrorCodes.BadValue, kInvalidSearchedESCPositionsError]);
    assert(res.errmsg.includes('Invalid value for ESCTokensV2 leaf tag') ||
               res.errmsg.includes('invalid searched ESC positions'),
           tojson(res));
    return res;
}

jsTest.log("Test compact with invalid compaction Tokens");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];

    const kInvalidToken = 85747;
    const realTokens = coll._getCompactionTokens();
    const tokens = {age: realTokens.balance, balance: realTokens.age};

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assertInvalidToken(coll.compact({compactionTokens: tokens}));
});

jsTest.log("Test compaction while passing explicit encryptionInformation");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];
    const ei = getEncryptionInformation(edb);

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));
    assert.commandWorked(coll.compact({encryptionInformation: ei}));
});

jsTest.log("Test compaction while passing invalid encryptionInformation");
runEncryptedTest(db, kDBName, kCollName, kSampleEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];
    const ei = getEncryptionInformation(edb);

    const kMissingEFCField = 8574705;
    const kMissingEFCSchema = 6371205;
    const kMissingIDLField = 40414;

    assert.commandWorked(coll.insert({age: NumberInt(20), balance: 0.0}));

    // Missing a required field.
    ei.schema[coll.getFullName()].fields.splice(1);
    assert.commandFailedWithCode(coll.compact({encryptionInformation: ei}), kMissingEFCField);

    // Missing all fields.
    ei.schema[coll.getFullName()].fields = [];
    assert.commandFailedWithCode(coll.compact({encryptionInformation: ei}), kMissingEFCField);

    // Missign expected schema.
    ei.schema['wrongCollName'] = ei.schema[coll.getFullName()];
    delete ei.schema[coll.getFullName()];
    assert.commandFailedWithCode(coll.compact({encryptionInformation: ei}), kMissingEFCSchema);

    // No schemas present.
    assert.commandFailedWithCode(coll.compact({encryptionInformation: {schema: {}}}),
                                 kMissingEFCSchema);

    // Empty encryptionInformation.
    const errmsg =
        assert.commandFailedWithCode(coll.compact({encryptionInformation: {}}), kMissingIDLField)
            .errmsg;
    assert.eq(
        errmsg,
        "BSON field 'compactStructuredEncryptionData.encryptionInformation.schema' is missing but a required field");
});

jsTest.log("Test compaction of unbounded range fields");
runEncryptedTest(db, kDBName, kCollName, kUnboundedRangeEncryptedFields, (edb, client) => {
    const coll = edb[kCollName];
    const intEdges = 33;
    const longEdges = 65;
    const dateEdges = 65;
    const doubleEdges = 65;
    const decimalEdges = 129;
    let totalEdges = 0;
    const doc = {
        int: NumberInt(20),
        long: NumberLong(30),
        date: ISODate("2022-01-01T07:30:10.957Z"),
        double: 3.14,
        decimal: NumberDecimal(100.43)
    };
    assert.commandWorked(coll.insert(doc));
    totalEdges = intEdges + longEdges + dateEdges + doubleEdges + decimalEdges;
    client.assertEncryptedCollectionCounts(coll.getName(), 1, totalEdges, totalEdges);

    assert.commandWorked(coll.compact());
    client.assertEncryptedCollectionCounts(coll.getName(), 1, totalEdges, 0);
});
