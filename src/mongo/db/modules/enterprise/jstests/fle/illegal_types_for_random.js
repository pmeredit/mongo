/**
 * Test that random encryption is banned for the correct set of BSON types.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.illegal_types_for_random;

    const schema = {
        type: "object",
        properties:
            {foo: {encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: [UUID()]}}}
    };

    const insertCommandTemplate =
        {insert: coll.getName(), documents: [{foo: 1}], jsonSchema: schema, isRemoteSchema: false};

    // Test that the following types can be successfully marked for encryption:
    //  - array
    //  - binData
    //  - bool
    //  - date
    //  - dbPointer
    //  - decimal
    //  - double
    //  - int
    //  - long
    //  - object
    //  - objectId
    //  - regex
    //  - string
    //  - timestamp
    function assertInsertingEncryptedValueSucceeds(val) {
        insertCommandTemplate.documents[0].foo = val;
        assert.commandWorked(testDb.runCommand(insertCommandTemplate));
    }

    assertInsertingEncryptedValueSucceeds([1, 2, 3]);
    assertInsertingEncryptedValueSucceeds(BinData(0, "data"));
    assertInsertingEncryptedValueSucceeds(true);
    assertInsertingEncryptedValueSucceeds(false);
    assertInsertingEncryptedValueSucceeds(ISODate());
    assertInsertingEncryptedValueSucceeds(DBRef("namespace", ObjectId()));
    assertInsertingEncryptedValueSucceeds(NumberDecimal(3.0));
    assertInsertingEncryptedValueSucceeds(3.0);
    assertInsertingEncryptedValueSucceeds(NumberInt(3));
    assertInsertingEncryptedValueSucceeds(NumberLong(3));
    assertInsertingEncryptedValueSucceeds({a: 1, b: 2, c: 3});
    assertInsertingEncryptedValueSucceeds(ObjectId());
    assertInsertingEncryptedValueSucceeds(/regex/);
    assertInsertingEncryptedValueSucceeds("string");
    assertInsertingEncryptedValueSucceeds(Timestamp(1, 1234));

    // Test that the following types cannot be marked for encryption:
    // - maxKey
    // - minKey
    // - null
    // - undefined
    function assertInsertingEncryptedValueFails(val) {
        insertCommandTemplate.documents[0].foo = val;
        assert.commandFailedWithCode(testDb.runCommand(insertCommandTemplate), 31041);
    }

    assertInsertingEncryptedValueFails(MaxKey);
    assertInsertingEncryptedValueFails(MinKey);
    assertInsertingEncryptedValueFails(null);
    assertInsertingEncryptedValueFails(undefined);

    mongocryptd.stop();
}());
