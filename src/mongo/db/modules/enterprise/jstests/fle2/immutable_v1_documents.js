/**
 * Test v1 on-disk encrypted data are not modified by v2 encrypted writes.
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_80
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {
                "path": "rank",
                "bsonType": "int",
                "queries":
                    {"queryType": "range", "min": NumberInt(1), "max": NumberInt(2), "sparsity": 1}
            },
            {"path": "class", "bsonType": "string"}
        ]
    }
}));

const edb = client.getDB();

const v1Documents = [
    {
        _id: 1,
        first: BinData(
            6,
            "B4VDyCHpZUCCsVLUB3BlhXsCrWfoWpkcxVKmVorGcuGjLkjoD3H4ha7K1lN1HxZsnpFvAU1uTdlr7i0+cwvrfgSW2yx7AfZVSA5UnUs5PbU4uc0rGi17lBAlEse+laGwlr01W78VXJP0qHs1ASbNJkYb8rZLfBeSV88MkIu1I7dtn1N1tiPl96XrDqf06xGzVlKGKTeF5WepFxGh/EXwbwVo7PU6bTnteezjN0HoBA46auKcJDK4U9Rca3J+rx3njeBv4bjvW2AD7bY2VMuXC+Nn9Cnrnhu4LsUaWfc="),
        last: "took",
        __safeContent__: [BinData(0, "LRen58KwK7DyV7687+eI072OAARUQAZCFzXB0aQ0rrU=")]
    },
    {
        _id: 2,
        rank: BinData(
            6,
            "CcZpQJc7SUxxnHRvTrvrPwIQ34nxxDabv5Tsp1TPyvD5rJ9iuZXr2FI8VjHbxJWATOR11qW2+jeUxpc1o1JPH3S1eNyzphnbiYGpje+3ma1TSJoLZG3NBojEpKOS3sqSeQDygkXy4MqGwKI45wERzTIV9Nhm6QnjtEVwrXoSmHfhEBKv8ugZ71jlKF8zOY0p4nuaP4uwgKOaO54YQ7SIxW7EAsP+k+jrNqqp0pidqWhbNNSRTlVZjAxxny7VwDvyEyfvL9QkXZQXQ7mW3Tv5R9j5AXRALm2ZIm1P2xWPMDqGyIoQQWacYYy/yB2b1+oN26PY/x6sZQAsG5ioa1uAtZ29Pfy7R5vnT76knaeiP16cQOTwPJielWnc8WsY84Sq1GsjS6dcYrBH8fcuejI1s+vvQW2EFiz0FlTgyG5thEkjYg=="),
        last: "gamgee",
        __safeContent__: [
            BinData(0, "OlEX5PWTdqT/kDWotOxAWKdlcaCsgQZeVPzWVi/3Pes="),
            BinData(0, "CwiVFANDx5EwD5ZgOrPo52nbRJholYbdLOo4LeJXS3o=")
        ]
    },
    {
        _id: 3,
        class: BinData(
            6,
            "Bvv9q/SHr0y9pCn2LcSGJnICDtkAQZ6TH2CWLoD7FX8nHj6xNKCHLT94RKBNR7aKrdV46RcF/r9VG6cGztDNZ7ryWFNkHa0oRAJCHk6repRHmUY="),
        last: "brandybuck"
    },
    {_id: 4, last: "baggins", location: "mordor"}
];

// Populate the collection with canned v1 documents using unencrypted client.
// bypassDocumentValidation is enabled so dummy __safeContent__ tags can be inserted.
let res =
    dbTest.runCommand({"insert": "basic", documents: v1Documents, bypassDocumentValidation: true});
assert.commandWorked(res);

const updateTests = [
    {
        title: "Test setting v2 indexed equality value on documents with v1 encrypted values fails",
        op: {$set: {first: "kim"}},
        result: 7293202
    },
    {
        title: "Test setting v2 indexed range value on documents with v1 encrypted values fails",
        op: {$set: {rank: NumberInt(1)}},
        result: 7293202
    },
    {
        title: "Test setting v2 unindexed value on documents with v1 encrypted values fails",
        op: {$set: {class: "hobbit"}},
        result: 7293202
    },
    {
        title: "Test setting unencrypted value on documents with v1 encrypted values fails",
        op: {$set: {location: "hobbiton"}},
        result: 7293202
    },
    {
        title: "Test replacing a v1 document with one containing no encrypted values fails",
        op: {last: "smeagol"},
        result: 7293202
    },
    {
        title: "Test replacing a v1 document with one containing encrypted values fails",
        op: {first: "sam"},
        result: 7293202
    },
    {
        title: "Test unsetting an encrypted (or nonexistent) field on a v1 document fails",
        op: {$unset: {first: ""}},
        result: 7293202
    },
    {
        title: "Test unsetting an unencrypted field on a v1 document fails",
        op: {$unset: {last: ""}},
        result: 7293202
    },
];

// Test modifications of documents 1 thru 3 (ie. those that contain v1 fields)
// using a v2 encrypted client. Verify immutability.
for (const test of updateTests) {
    jsTestLog(test.title);
    for (let id = 1; id < v1Documents.length; id++) {
        res = edb.erunCommand({update: "basic", updates: [{q: {_id: id}, u: test.op}]});
        assert.commandFailedWithCode(res, 7293202, "Failed on document: " + id);

        res = edb.erunCommand({findAndModify: "basic", query: {_id: id}, update: test.op});
        assert.commandFailedWithCode(res, 7293202, "Failed on document: " + id);
    }
}

// Test setting v2 encrypted values on an existing unencrypted document works
res = edb.erunCommand({
    update: "basic",
    updates: [
        {q: {location: "mordor"}, u: {$set: {first: "frodo", class: "hobbit", rank: NumberInt(1)}}}
    ]
});
assert.commandWorked(res);
client.assertEncryptedCollectionCounts("basic", 4, 3, 3);
client.assertOneEncryptedDocumentFields(
    "basic", {location: "mordor"}, {first: "frodo", rank: NumberInt(1)});

// Test findAndModify remove on a v1 document works
const schema = edb.getCollectionInfos({name: "basic"})[0].options.encryptedFields;
res = client.getDB().runCommand({
    findAndModify: "basic",
    query: {_id: 1},
    remove: true,
    encryptionInformation: {schema: {"basic.basic": schema}}
});
assert.commandWorked(res);
client.assertEncryptedCollectionCounts("basic", 3, 3, 3);
