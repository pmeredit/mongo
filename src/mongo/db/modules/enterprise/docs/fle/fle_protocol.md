# Queryable Encryption (FLE2) Protocol

- [Queryable Encryption (FLE2) Protocol](#queryable-encryption-fle2-protocol)
- [Terminology](#terminology)
- [Walkthrough: Insert](#walkthrough-insert)
- [Walkthrough: Delete](#walkthrough-delete)
- [Walkthrough: Find](#walkthrough-find)
- [Walkthrough: Update](#walkthrough-update)
- [Walkthrough: Explicit Encryption and Decryption](#walkthrough-explicit-encryption-and-decryption)
- [Walkthrough: Explicit Equality Comparison](#walkthrough-explicit-equality-comparison)
- [Reference: Cryptography](#reference-cryptography)
- [Reference: BinData 6 subtypes](#reference-bindata-6-subtypes)
- [Reference: Keys and Tokens](#reference-keys-and-tokens)
- [Reference: Schema](#reference-schema)
- [Reference: Naming rules for State collections](#reference-naming-rules-for-state-collections)
- [Reference: Unindexed Encrypted Payload](#reference-unindexed-encrypted-payload)
- [Reference: Indexed Equality Encrypted Values](#reference-indexed-equality-encrypted-values)
- [Reference: ESC Schema](#reference-esc-schema)
  - [Null Anchor record](#null-anchor-record)
  - [Anchor record](#anchor-record)
  - [Non-anchor record](#non-anchor-record)
- [Reference: ECOC Schema](#reference-ecoc-schema)
- [Reference: Insert pseudo-code](#reference-insert-pseudo-code)
  - [Insert: Client-side](#insert-client-side)
  - [Insert: Server-side](#insert-server-side)
- [Reference: Find pseudo-code](#reference-find-pseudo-code)
  - [Find: Client-side](#find-client-side)
  - [Find: Server-side](#find-server-side)
- [Reference: Update pseudo-code](#reference-update-pseudo-code)
  - [Update: Server-side](#update-server-side)
- [Reference: CompactStructuredEncryptionData](#reference-compactstructuredencryptiondata)
- [Reference: CleanupStructuredEncryptionData](#reference-cleanupstructuredencryptiondata)
- [Reference: Payloads](#reference-payloads)
  - [Formats for Create Collection and listCollections](#formats-for-create-collection-and-listcollections)
  - [Formats for Query Analysis](#formats-for-query-analysis)
  - [Formats for client -\> server CRUD](#formats-for-client---server-crud)
  - [Formats for client -\> server Query](#formats-for-client---server-query)
- [Compact Command](#compact-command)
- [Cleanup Command](#cleanup-command)
- [Reference: Server Status Compact and Cleanup](#reference-server-status-compact-and-cleanup)
- [Reference: Explicit Encryption and Decryption](#reference-explicit-encryption-and-decryption)
- [Reference: Explicit Equality Comparison](#reference-explicit-equality-comparison)

# Terminology

- EDC - Encrypted Data Collection (i.e. the user collection)
- ECC - Encrypted Cache Collection (QE Protocol V1 only, Obsolete)
- ECOC - Encrypted Compaction Collection
- ESC - Encrypted State Collection

- || - concatenates 1 or more fixed sized values together
- BSON() is a function that converts to the enclosed JSON into BSON
- byte() denotes something is 8-bit sized
- DecryptAEAD() decrypts a value with a encryption AEAD cipher and cipher mode
- Decrypt() decrypts a value with a encryption non-AEAD cipher and cipher mode
- Derive() compute one or more tokens from the specified token. See Keys and Tokens.
- EncryptAEAD() encrypts a value with a encryption AEAD cipher and cipher mode
- Encrypt() encrypts a value with a encryption non-AEAD cipher and cipher mode
- HMAC() - computes a HMAC and returns the bytes

Assumptions

- All integers are written with little-endian encoding

# Walkthrough: Insert

User create a collection named "testColl" in "testDB"

```js
dbTest.createCollection("testColl", {
  encryptedFields: {
    fields: [
      {
        path: "encryptedField",
        keyId: UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
        bsonType: "string",
        queries: {queryType: "equality"},
      },
      {
        path: "nested.otherEncryptedField",
        keyId: UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
        bsonType: "string",
        queries: [{queryType: "equality"}],
      },
    ],
  },
});
```

listCollections returns the following

```js
{
    "name" : "testColl",
    "type" : "collection",
    "options" : {
        "encryptedFields" : {
            "escCollection" : "enxcol_.testColl.esc",
            "ecocCollection" : "enxcol_.testColl.ecoc",
            "fields" : [
                {
                    "keyId" : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                    "path" : "encryptedField",
                    "bsonType" : "string",
                    "queries" : {
                        "queryType" : "equality",
                        "contention" : NumberLong(0)
                    }
                },
                {
                    "keyId" : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
                    "path" : "nested.otherEncryptedField",
                    "bsonType" : "string",
                    "queries" : [
                        {
                            "queryType" : "equality",
                            "contention" : NumberLong(0)
                        }
                    ]
                }
            ]
        }
    },
    "info" : {
        "readOnly" : false,
        "uuid" : UUID("ff285bd1-8662-4736-88c9-68b892028f23")
    },
    "idIndex" : {
        "v" : 2,
        "key" : {
            "_id" : 1
        },
        "name" : "_id_"
    }
}
```

User creates this insert for testDB.testColl

```json
{
  "_id": 1,
  "encryptedField": "secret",
  "nested": {
    "otherEncryptedField": "secret"
  }
}
```

Shell/libmongocrypt sends the following to query analysis:

```js
{
    insert: "testColl",
    $db: "testDB",
    documents: [{
        "_id" : 1,
        "encryptedField" : "secret",
        "nested" {
            "otherEncryptedField" : "secret",
        }
    }],
    encryptionInformation: {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

Query analysis returns the following by adding placeholders for fields to encrypt

```js

{
    hasEncryptionPlaceholders : true,
    result: {
        insert: "testColl",
        $db: "testDB",
        documents: [{
            "_id" : 1,
            "encryptedField" : BinData(6, byte(0x3) + BSON({
                    t : 1,  // new field
                    a : 3,  // new algorithm type
                    ki : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9")
                    v: "secret",
                    cm: 0   // new field
                }
            )),
            "nested" {
                "otherEncryptedField" : BinData(6, byte(0x3) + BSON({
                        t : 1,  // new field
                        a : 3,  // new algorithm type
                        ki : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9")
                        v: "secret",
                        cm: 0   // new field
                    }
                )),
            }
        }
        ],
        encryptionInformation: {
            type : 1,
            schema: {
                "testDB.testColl" : {
                    // omitted for brevity
                    // see EncryptedFields output from listCollections above
                }
            }
        }
    }
}
```

Libmongocrypt replaces the placeholders information for insert/update and sends it to the server

```js
{
    insert: "testColl",
    $db: "testDB",
    documents: [{
        "_id" : 1,
        "encryptedField" : BinData(6, byte(0xB) + BSON({
                d : EDCDerivedFromDataTokenAndCounter
                s : ESCDerivedFromDataTokenAndCounter
                p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
                u : 11d58b8a-0c6c-4d69-a0bd-70c6d9befae9
                t : 2
                e : ServerDataEncryptionLevel1Token
                l : ServerDerivedFromDataToken
                k : 0
                v : EncryptAEAD(UserKey, "secret")
            }
        )),
        "nested" {
            "otherEncryptedField" : BinData(6, byte(0xB) + BSON({
                    d : EDCDerivedFromDataTokenAndCounter
                    s : ESCDerivedFromDataTokenAndCounter
                    p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
                    u : 11d58b8a-0c6c-4d69-a0bd-70c6d9befae9
                    t : 2
                    e : ServerDataEncryptionLevel1Token
                    l : ServerDerivedFromDataToken
                    k : 0
                    v : EncryptAEAD(UserKey, "secret")
                }
            )),
        }
    }],
    encryptionInformation :
    {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

# Walkthrough: Delete

In QE Protocol V2, Delete is not special. There is no special code for delete unlike in QE V1.

# Walkthrough: Find

User creates this find for testDB.testColl

```js
{
    "encryptedField" : "secret",
}
```

Shell/libmongocrypt sends the following to query analysis:

```js
{
    find: "testColl",
    $db: "testDB",
    filter: {
        "encryptedField" : "secret",
    },
    encryptionInformation : {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

Query analysis returns the following by transforming the $eq into a $eq with a payload.

```js
{
    hasEncryptionPlaceholders : true,
    result: {
        find: "testColl",
        $db: "testDB",
        filter: {
            "encryptedField" : {
                $eq : BinData(6, byte(0x3) + BSON({
                    t : 2,  // new field
                    a : 3,  // new algorithm type
                    ki : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9")
                    v: "secret",
                    cm: 0   // new field
                }
            )) }
        },
        encryptionInformation :
        {
            type : 1,
            schema: {
                "testDB.testColl" : {
                    // omitted for brevity
                    // see EncryptedFields output from listCollections above
                }
            }
        }
    }
}
```

Libmongocrypt replaces the placeholders information for find, adds tokens and sends it to the server

```js
{
    find: "testColl",
    $db: "testDB",
    filter: {
        "encryptedField" : {
            $eq : BinData(6, byte(0xC) + BSON({
                d : EDCDerivedFromDataToken
                s : ESCDerivedFromDataToken
                l : ServerDerivedFromDataToken,
                cm : 0,
            }))
        }
    },
    encryptionInformation : {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

On the server-side, find() rewrites the query after querying ESC.

```js
{
    find: "testColl",
    $db: "testDB",
    filter: {
        __safeContent__ :
            $in : [
                tags...
            ]
        }
    },
}
```

# Walkthrough: Update

User creates this update for testDB.testColl

```js
testDB.testColl.update(
  {
    _id: 1,
  },
  {
    $set: {encryptedField: "newSecret"},
  },
);
```

Shell/libmongocrypt sends the following to query analysis:

```js
{
    update: "testColl",
    $db: "testDB",
    updates: [
        q : {
            "_id" : 1,
        },
        u : {
            $set : { "encryptedField" : "newSecret" }
        }
    ],
    encryptionInformation :
    {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

Query analysis returns the following by adding placeholders for fields to encrypt

```js
{
    hasEncryptionPlaceholders : true,
    result: {
        update: "testColl",
        $db: "testDB",
        updates: [{
            q : {
                "_id" : 1,
            },
            u : {
                $set : {
                    "encryptedField" : BinData(6, byte(0x3) + BSON(
                    {
                    t : 1,  // new field
                    a : 3,  // new algorithm type
                    ki : UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9")
                    v: "newSecret",
                    cm: 0
                    }
                }
            }
        }],
        encryptionInformation : {
            type : 1,
            schema: {
                "testDB.testColl" : {
                    // omitted for brevity
                    // see EncryptedFields output from listCollections above
                }
            }
        },
    }
}
```

Libmongocrypt replaces the placeholders information for insert/update and sends it to the server

```js
{
    update: "testColl",
    $db: "testDB",
    updates: [{
        q : {
            "_id" : 1,
        },
        u : {
            $set : {
                "encryptedField" : BinData(6, byte(0xB) + BSON(
                {
                d : EDCDerivedFromDataTokenAndCounter
                s : ESCDerivedFromDataTokenAndCounter
                p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
                u : 11d58b8a-0c6c-4d69-a0bd-70c6d9befae9
                t : 2
                e : ServerDataEncryptionLevel1Token
                l : ServerDerivedFromDataToken
                k : 0
                v : EncryptAEAD(UserKey, "secret")
                }
            }
        }
    }],
    encryptionInformation : {
        type : 1,
        schema: {
            "testDB.testColl" : {
                // omitted for brevity
                // see EncryptedFields output from listCollections above
            }
        }
    }
}
```

The resulting update is sent to the server.

# Walkthrough: Explicit Encryption and Decryption

When a user does not have implicit encryption, they will need to manually encrypt their fields. Implicit decryption is supported in all version and the customer does not need to do anything special.

```js
client.encrypt(
  UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
  "secret",
  (optional_contention = 1),
);
```

returns the following

```js
uint8_t[] = byte(0xB) + BSON({
        d : EDCDerivedFromDataTokenAndCounter
        s : ESCDerivedFromDataTokenAndCounter
        p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
        u : 11d58b8a-0c6c-4d69-a0bd-70c6d9befae9
        t : 2
        e : ServerDataEncryptionLevel1Token
        l : ServerDerivedFromDataToken
        k : 0
        v : EncryptAEAD(UserKey, "secret")
})
```

```js
client.decrypt(EncryptedFieldValue);
```

where
EncryptedFieldValue = [Index Equality Encrypted Value](##reference-indexed-equality-encrypted-values)
or
`EncryptedFieldValue = byte(0xB) + FLE2InsertUpdatePayload` returns the following:

```js
"secret";
```

# Walkthrough: Explicit Equality Comparison

When a user does not have implicit encryption, they will need to manually generate the FLE 2 equality comparisons.

Without implicit encryption, if a user writes the following:

```js
db.coll.find( "encryptedField" : { $eq : "secret" } )
```

it will not work. Instead the user must write

```js
db.coll.find({
    "encryptedField" : $eq : BinData(0xC, UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
    "secret",
    optional_max_contention=1) } )
```

the `$eq` comparison is transformed into the following BSON object:

```js
{
    "$eq" : BinData(6, byte(0xC) + BSON({
        d : EDCDerivedFromDataToken
        s : ESCDerivedFromDataToken
        l : ServerDerivedFromDataToken
        cm : 0
    }
}
```

# Reference: Cryptography

HMAC is HMAC-SHA-256
Encrypt/Decrypt is AES-256-CTR. It does not provide integrity, only confidentiality.
EncryptAEAD/DecryptAEAD is AES-256-CBC-HMAC-SHA-256. Note: It uses CBC instead of CTR to obscure length of the plaintext a little.

# Reference: BinData 6 subtypes

In FLE 1, Bindata 6 had several subtypes as denoted by the first byte of a bindata field. FLE 2 adds several new ones described below

0. FLE 1 Encryption Placeholder
1. FLE 1 Deterministic Encrypted Value
2. FLE 1 Random Encrypted Value
3. FLE 2 Encryption Placeholder (see FLE2EncryptionPlaceholder below)
4. FLE 2 V1 Insert Update Payload (OBSOLETE)
5. FLE 2 V1 Find Equality Payload (OBSOLETE)
6. FLE 2 V1 Unindexed Encrypted Value (OBSOLETE)
7. FLE 2 V1 Equality Indexed Encrypted Value (OBSOLETE)
8. FLE 2 Transient Raw (Internal to server, never persisted)
9. FLE 2 V1 Range Indexed Encrypted Value (OBSOLETE)
10. FLE 2 V1 Range Range Payload (OBSOLETE)
11. FLE 2 V2 Insert Update Payload (see FLE2InsertUpdatePayloadV2 below)
12. FLE 2 V2 Find Equality Payload (see FLE2FindEqualityPayloadV2 below)
13. FLE 2 V2 Find Range Payload (see FLE2FindRangePayloadV2 below)
14. FLE 2 V2 Equality Indexed Encrypted Value (See Indexed Equality Encrypted Values)
15. FLE 2 V2 Range Indexed Encrypted Value (Not in this document)
16. FLE 2 V2 Unindexed Encrypted Value (similar to type 2)

# Reference: Keys and Tokens

`S_KeyId` or `IndexKey` is a per-field key material. It is 96 bytes since that is the same size as the original FLE 1 value. From this index key, a normal of tokens are derived via HMAC as described below.

`K_KeyId` is either chosen by the user or is `S_KeyId/IndexKey`.

Since it is possible for `IndexKey == UserKey`, we choose the key and HMAC components assuming they are shared.

**Key Derivation**

- AEAD AES Key (aka `Ke`) is the first 32 bytes of `IndexKey`.
- AEAD HMAC Key (aka `Km`) is the second 32 bytes of `IndexKey`.
- Token HMAC Key is the third 32 bytes of `IndexKey`.
  They key derivation is different then FLE1.
  Terminology:

```
Key = Function to derive it = Latex description
f = field
v = value
u = or [1,r] where r = contention factor
u == 0 if field has no contention otherwise u = random secure sample {1, .. max contention}.
```

Tokens:

```
CollectionsLevel1Token = HMAC(IndexKey, 1) = K_{f,1}
ServerTokenDerivationLevel1Token = HMAC(IndexKey, 2) = K_{f,2}
ServerDataEncryptionLevel1Token = HMAC(IndexKey, 3) = K_{f,3} = Fs[f,3]

EDCToken = HMAC(CollectionsLevel1Token, 1) = K^{edc}_f
ESCToken = HMAC(CollectionsLevel1Token, 2) = K^{esc}_f
ECOCToken = HMAC(CollectionsLevel1Token, 4) = K^{ecoc}_f = Fs[f,1,4]

EDCDerivedFromDataToken = HMAC(EDCToken, v) = K^{edc}_{f,v} = Fs[f,1,1,v]
ESCDerivedFromDataToken = HMAC(ESCToken, v) = K^{esc}_{f,v} = Fs[f,1,2,v]

EDCDerivedFromDataTokenAndContentionFactorToken = HMAC(EDCDerivedFromDataToken, u) = Fs[f,1,1,v,u]
ESCDerivedFromDataTokenAndContentionFactorToken = HMAC(ESCDerivedFromDataToken, u) = Fs[f,1,2,v,u]

EDCTwiceDerivedToken = HMAC(EDCDerivedFromDataTokenAndContentionFactorToken, 1) = Fs_edc(1)
ESCTwiceDerivedTagToken = HMAC(ESCDerivedFromDataTokenAndContentionFactorToken, 1) = Fs_esc(1)
ESCTwiceDerivedValueToken = HMAC(ESCDerivedFromDataTokenAndContentionFactorToken, 2) = Fs_esc(2)

ServerDerivedFromDataToken = HMAC(ServerTokenDerivationLevel1Token, v) = K_{f,2,v} = Fs[f,2,v]

ServerCountAndContentionFactorEncryptionToken = HMAC(ServerDerivedFromDataToken, 1) = Fs[f,2,v,1]
ServerZerosEncryptionToken = HMAC(ServerDerivedFromDataToken, 2) = Fs[f,2,v,2]

```

# Reference: Schema

Four collections ( r = read, w = write)

- EDC - (user data) - all
- ESC - insert(r/w), query(r), update(r/w), compact (r/w)
- ECOC - insert(w), update(w), delete(w), compact (r/w)

# Reference: Naming rules for State collections

Naming rules for state collections:

ESC: `enxcol_.<base_collection_name>.esc`
ECOC: `enxcol_.<base_collection_name>.ecoc`

Example for User Collection Named: `example`
ESC: `enxcol_.example.esc`
ECOC: `enxcol_.example.ecoc`

# Reference: Unindexed Encrypted Payload

This is the same as type 2 but with HMAC-SHA256.

```c
struct {
    uint8_t fle_blob_subtype = 16;
    uint8_t key_uuid[16];
    uint8  original_bson_type;
    ciphertext[ciphertext_length];
}
```

# Reference: Indexed Equality Encrypted Values

The Encrypted Data Collection (EDC) is read and written by the user. It stores the encrypted data and embedded search tags. EDC collection documents have the following format which consists of encrypted fields and safeContent fields:

```js
{
        encryptedFields: BinData(6), // see below
        __safeContent__ : [
                tags, //see below
        ]
}
```

Fields values have the following contents in pseudo code:

`BinData(6, byte(0xE) + S_KeyId + BSONType + Encrypt(ServerDataEncryptionLevel1Token, v) + metadataBlock` and
`metadataBlock = Encrypt(k) + tag + Encrypt(32 bytes of zero)`

```c
struct {
    uint8_t fle_blob_subtype = 0xE;
    uint8_t key_uuid[16]; // S_KeyId aka IndexKeyId
    uint8  original_bson_type;
    ciphertext[ciphertext_length];
    metadataBlock;
}

```

where `ciphertext_length = sizeof(struct) - 17 - sizeof(metadataBlock)`.

The format of the metadata block is as follows:

```c
struct {
    uint8_t[32] encryptedCountersBlob;
    uint8_t[32] tag;
    uint8_t[32] encryptedZerosBlob;
}
```

where `encryptedZerosBlob = Encrypt(ServerZerosEncryptionToken, 16 bytes of 0x0)`,
`encryptedCountersBlob = Encrypt(ServerCountAndContentionFactorEncryptionToken, counter || contentionFactor)`

The format of the encrypted data is as follows:

```c
struct {
    uint8_t[length] cipherText; // UserKeyId + EncryptAEAD(K_KeyId, value)
}
```

where `ciphertext_length = sizeof(struct) - (16)`. Everything except `cipherText` is associatedData.

Tags are formatted as

`HMAC(EDCTwiceDerivedToken, count)`

# Reference: ESC Schema

Encrypted State Collection (ESC) stores information about how many field/value pairs exist in EDC. It is written during insert and update. It is read during queries, insert, update and compaction. It has three different sets of documents: anchors, null-anchors and non-anchors.

Example set of non-anchor values for a given field and value pair:

| count |
| ----- |
| 3     |
| 4     |
| 5     |
| 6     |

Anchors
| **apos** | **cpos** |
|----------|----------|
| 1 | 2 |
| 2 | 7 |
| 3 | 11 |

```
where
  cpos = position of non-anchor record in the range [1..UINT64_MAX]
  apos = position of anchor record in the range [1..UINT64_MAX]
```

There are three types of records in the ESC collection:

1. Null Anchor: 0 or 1 records
2. Anchor: 0 or more records
3. Non-Anchor: 0 or more records

### Null Anchor record

Null records, also known as \bot records, store the start of the anchor records. These records are created and updated by cleanup. They are read during find, insert, update, compact, and cleanup.

```js
{
  _id: HMAC(ESCTwiceDerivedTagToken, 0 || 0);
  value: Encrypt(ESCTwiceDerivedValueToken, apos || cpos);
}
```

### Anchor record

Anchor records are sequentially numbered records. They are used to find non-anchor records. These records are created by compact. They are read during find, insert, update, compact, and cleanup.

```js
{
  _id: HMAC(ESCTwiceDerivedTagToken, 0 || apos);
  value: Encrypt(ESCTwiceDerivedValueToken, 0 || cpos);
}
```

### Non-anchor record

Non-Anchor records are sequentially numbered records. They are used to find number of field value pairs. These records are inserted by insert and update. They are deleted by compact and cleanup.

```js
{
  _id: HMAC(ESCTwiceDerivedTagToken, cpos);
}
```

# Reference: ECOC Schema

Encrypted Compaction Collection (ECOC) stores information about which were inserted into ESC. The presence of records in ECOC is a signal that compaction may help reduce the space usage of ESC. It is an unordered collection of documents. The value field may have duplicate entries in the collection.

```js
{
   _id : ObjectId() -- omitted so MongoDB can auto choose it
   fieldName : String,
   value : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
}
```

# Reference: Insert pseudo-code

Insert consists of two phases: client-side and server-side. See [Insert walkthrough](#walkthrough-insert)

## Insert: Client-side

1. driver/libmongocrypt: Retrieve `encryptedFields` for the target collection
2. libmongocrypt/query analysis: Use query analysis to transform encrypted fields into `FLE2EncryptionPlaceholder` and append `encryptionInformation`.
3. libmongocrypt: Transform placeholder fields into `FLE2InsertUpdatePayloadV2` or `FLE2UnindexedEncryptedValueV2`. Append `encryptionInformation`.
4. driver: Send document to server.

## Insert: Server-side

Server-side is responsible for adding new records into ESC, adding new records into ECOC, and finalizing the EDC document.

When the server receives an insert command with an `encryptionInformation` field, it processes the command as a queryable encryption insert. For each document in the command, it performs the following:

1. Each `InsertUpdatePayloadV2` value in the document is assigned a "counter" value, which is just a number that tracks how many times the underlying plaintext value has been inserted using a specific contention factor value, and is incremented on every insertion of that value.

   e.g. If `"encryptedField"` is an equality encrypted field, then the first five insertions of `{ encryptedField: "secret" }` at contention factor 0 will be assigned counter values 1 through 5.

   The server can tell which `InsertUpdatePayloadV2` are alike since they will have the same `ESCDerivedFromDataAndContentionFactorToken`.

   The server keeps track of which counter values have been used by inserting this counter-derived information in the ESC collection:

   `{ _id: HMAC(HMAC(ESCDerivedFromDataAndContentionFactorToken, 1), counter) }`.

2. Using the counter value and the `EDCDerivedFromDataAndContentionFactorToken`, the server calculates a globally unique identifier or "tag" for this particular field value insertion:

   `tag = HMAC(HMAC(EDCDerivedFromDataAndContentionFactorToken, 1), counter)`

   All tags created for this document are included in the final BSON document, in an array field called `__safeContent__`.

3. Each `InsertUpdatePayloadV2` value is transformed to its final on-disk format. For equality fields, this takes the form:

   ```js
   BinData(
     6,
     byte(0xe) ||
       index_key_id ||
       bson_type ||
       server_encrypted_value ||
       encrypted_counter_and_tag,
   );
   ```

   where `byte(0xE)` stands for a `FLE2EqualityIndexedValueV2` type.

   `server_encrypted_value` is the re-encrypted user ciphertext, calculated by the server as:

   `AES_256_CTR_Encrypt(ServerDataEncryptionLevel1Token, v)`

   The `encrypted_counter_and_tag` is an additional binary blob that also includes the `tag` generated for this `InsertUpdatePayloadV2`. This `tag` is used during updates, for when we need to remove stale tags from `__safeContent__`.

   For example, when inserting this document:

   `{ "_id": 1, "encryptedField": "secret" }`

   The final document inserted will look like:

   ```js
   {
       "_id": 1,
       "encryptedField": BinData(6,
           (byte(0xE) ||
            index_key_id ||
            bson_type ||
            server_encrypted_value ||
            encrypted_counter_and_tag)),
       "__safeContent__": [
           BinData(0, tag)
       ]
   }
   ```

### How does the server find the next counter value to use?

The ESC stores some info about the counter usage for some `ESCDerivedFromDataAndContentionFactorToken`, _T_, in the form of a 32-byte HMAC code.

One way to obtain the last-used counter value for _T_ is to sequentially derive HMAC codes starting at counter value `1`, and see if a document with a similar `_id` value exists in the ESC. The last-used counter value will be the last one for which an ESC entry exists.

In practice, the server first performs exponential guessing to find some upper bound value, `rho`, for which there's no ESC entry; then, it performs a binary search on the range 1 thru `rho`. (`EmuBinary`)

# Reference: Find pseudo-code

Find consists of two phases: client-side and server-side. The client-side is the same as Insert. See [Find walkthrough](#walkthrough-find)

## Find: Client-side

### Request

1. driver/libmongocrypt: Retrieve `encryptedFields` for the target collection
2. libmongocrypt/query analysis: Use query analysis to transform encrypted fields into `FLE2EncryptionPlaceholder` and append `encryptionInformation`.
3. libmongocrypt: Transform placeholder fields into `FLE2FindEqualityPayloadV2` and append `encryptionInformation`
4. driver: Send document to server

### Response

The driver gets back the on-disk representation of the encrypted data in the find response:

```js
{
    "_id": 1,
    "encryptedField": BinData(6,
        (byte(0xE) ||
         index_key_id ||
         bson_type ||
         server_encrypted_value ||
         encrypted_counter_and_tag)),
    "__safeContent__": [
        BinData(0, tag)
    ]
}
```

The driver performs the following steps to decrypt the value of `"encryptedField"``:

```python
indexKey = getKeyById(index_key_id)

serverToken = HMAC(indexKey[64:96], 3)

clientCiphertext = AES_256_CTR_Decrypt(serverToken, server_encrypted_value)

userKeyId = clientCiphertext[0:16]

userKey = getKeyById(userKeyId)

plaintext = DecryptAEAD(userKey, clientCiphertext[16:]) # "secret"
```

Finally, the driver replaces the encrypted BinData with the decrypted value so that the returned document looks like:

```js
{
    "_id": 1,
    "encryptedField": "secret",
    "__safeContent__": [ BinData(0, tag) ]
}
```

## Find: Server-side

Server-side is responsible for querying ESC for each encrypted field, and generate a set of tags to search for.

When the server receives a find command with an `encryptionInformation` field, it processes the command as a queryable encryption find. If the query filter contains a queryable encryption find payload (e.g. `FindEqualityPayloadV2`), the server performs a rewrite of the query filter to be a `$in` lookup over cryptographic tags in `__safeContent__` fields. (See also: the [FLE query architecture guide](https://github.com/10gen/mongo-enterprise-modules/blob/master/docs/fle/query_architecture.md#server-side-query-rewriting))

In a nutshell, the query rewrite (for equality) works as follows:

1. For every `FindEqualityPayloadV2` in the filter query, generate all the cryptographic tags that were ever created for the underlying plaintext value, for every contention factor between 0 and `cm`.

   So, for every value `c in range(0, cm)`, calculate:

   ```python
   edcDerivedFromDataAndContentionFactorToken = HMAC(edcDerivedFromDataToken, c)
   escDerivedFromDataAndContentionFactorToken = HMAC(escDerivedFromDataToken, c)
   lastUsedCounter = EmuBinary(escDerivedFromDataAndConentionFactorToken)
   tags = HMAC(HMAC(edcDerivedFromDataAndContentionFactorToken, 1), counter) for counter in range(1, lastUsedCounter + 1)
   ```

2. Rewrite the query such that occurrences of `{$eq: FindEqualityPayload}` are removed, and becomes a `$in` lookup over `__safeContent__`.

The final query filter becomes:

```js
{
  __safeContent__: {
    $in: tags;
  }
}
```

# Reference: Update pseudo-code

Update consists of two phases: client-side and server-side. The client-side is the same as Insert. See [Update walkthrough](#walkthrough-update)

Prohibited features:
multi = true

## Update: Server-side

Server-side update is a combination of the steps for Find, Insert and Delete. The document has to be updated twice.
In the case of $set, the first pass update the new value and add new tags. In the case of $unset, the first pass removes the value. The second pass, in both cases, removes the old tags from **safeContent**.

1. Transform the update into a findAndModify command
2. Transform the query field per "Find: Server-side" if needed
3. Transform the update field per "Insert: Server-side" if needed
4. Append the tags from Step 3 as `{ $push : {__safeContent__ : [tags] }` to the update field.
   If the update already has `$push`, merge them.
5. Run the following algorithm

```python
original_document = db.coll.findAndModify(new: false)
original_encrypted_fields = GetEncryptedFields(EncryptedFields, original_document)
new_document = db.coll.find(_id)
new_encrypted_fields = GetEncryptedFields(EncryptedFields, new_document)
tags_to_remove = GetRemovedTags(original_encrypted_fields, new_encrypted_fields)
db.coll.findAndModify($pull: tags_to_remove)
```

In `GetRemovedTags`, in order to get the set of "stale" tags to `$pull` from the `__safeContent__` array, the server first identifies which fields were modified and/or removed from the pre-image document by comparing the old & new encrypted fields. Then, for every field that was modified/removed, it obtains the its stale tag from the `encrypted_counter_and_tag` part of the original payload.

# Reference: CompactStructuredEncryptionData

**Request:**

```js
{
    compactStructuredEncryptionData : "<collection name>",
    $db : "<db name>",
    compactionTokens : {
       encryptedFieldPath : Bindata(subtype 0),
       ...
    },
}
```

`compactionTokens` is map of indexed encrypted paths from indexed paths to `ECOCToken`.

**Reply:**

```js
{
    ok : 1,
    stats : {
        ecoc : {
            read : NumberLong,
            deleted : NumberLong,
        },
        esc : {
            read : NumberLong,
            inserted : NumberLong,
            updated : NumberLong,
            deleted : NumberLong,
        },
    }
}
```

These stats represent the records read and modified by compact. The number of documents that query sees may be higher then these counts as these counts represent the documents compact reads.

A new action type “compactStructuredEncryptionData” will be added and this will be included in “readWriteAnyDB”, and “dbOwner”. This actionType will not be added to “dbAdmin” since only the owner of data has the ability to create the compaction tokens which a user in generic dbAdmin may not have the ability to.

# Reference: CleanupStructuredEncryptionData

The `cleanupStructuredEncryptionData` command can be used to compact both anchors and non-anchors in the ESC into null anchors. Whereas `compactStructuredEncryptionData` only inserts anchors and removes non-anchors, `cleanupStructuredEncryptionData` also performs updates of null anchors and maintains a temporary side collection where it stores the `_id` of to-be-deleted anchors. This side collection is dropped at the end of the cleanup operation after all the anchors saved therein are deleted from the ESC.

`cleanupStructuredEncryptionData` cannot run concurrently with a `compactStructuredEncryptionData` (or another `cleanupStructuredEncryptionData`) on the same encrypted collection, and so will block until an ongoing `compactStructuredEncryptionData` completes.

**Request:**

```js
{
    cleanupStructuredEncryptionData : "<collection name>",
    $db : "<db name>",
    cleanupTokens : {
       encryptedFieldPath : Bindata(subtype 0),
       ...
    },
}
```

`cleanupTokens` is map of indexed encrypted paths from indexed paths to `ECOCToken`.

**Reply:**

```js
{
    ok : 1,
    stats : {
        ecoc : {
            read : NumberLong,
            deleted : NumberLong,
        },
        esc : {
            read : NumberLong,
            inserted : NumberLong,
            updated : NumberLong,
            deleted : NumberLong,
        },
    }
}
```

These stats represent the records read and modified by cleanup. The number of documents that query sees may be higher then these counts as these counts represent the documents cleanup reads.

A new action type “cleanupStructuredEncryptionData” will be added and this will be included in “readWriteAnyDB”, and “dbOwner”. This actionType will not be added to “dbAdmin” since only the owner of data has the ability to create the compaction tokens which a user in generic dbAdmin may not have the ability to.

# Reference: Payloads

Payloads are described in server's IDL format.

`FleAlgorithmInt` (src/mongo/db/matcher/schema/encrypt_schema.idl) is extended with type 3 to mean kFLE2

## Formats for Create Collection and listCollections

```yaml
QueryTypeConfig:
  description: "Information about query support for a field"
  strict: true
  fields:
    queryType:
      description: "Type of supported queries"
      type: QueryType
    contention:
      description: "Contention factor for field, 0 means it has extremely high set number of distinct values"
      type: long
      default: 0

EncryptedField:
  description: "Information about encrypted fields"
  strict: true
  fields:
    keyId:
      description: "UUID of key in key vault to use for encryption"
      type: uuid
    path:
      description: "Path to field to encrypt"
      type: string
    bsonType:
      description: "BSON type of field to encrypt"
      type: string
    queries:
      description: "List of supported query types"
      type:
        variant: [QueryTypeConfig, array<QueryTypeConfig>]
      optional: true

EncryptedFieldConfig:
  description: "Information about encrypted fields and state collections"
  strict: true
  fields:
    escCollection:
      description: "Encrypted State Collection name, defaults to enxcol_.<collection>.esc"
      type: string
      optional: true
    ecocCollection:
      description: "Encrypted Compaction Collection name, defaults to enxcol_.<collection>.ecoc"
      type: string
      optional: true
    fields:
      description: "Array of encrypted fields"
      type: array<EncryptedField>
```

## Formats for Query Analysis

`EncryptionInformation` describes the information appended to the query sent to query analysis.

```yaml
EncryptionInformation:
  description: "Implements Encryption Information which includes the schema for FLE 2 that is consumed by query_analysis and write_ops"
  strict: true
  fields:
    type:
      description: "The version number"
      type: safeInt
      default: 1
    schema:
      description: "A map of namespaces paths to EncryptedFieldConfig"
      type: object
```

The encryption placeholder payload is returned by query analysis for each encrypted field. Here is the format. It is stored inside a bindata 6 field with a prefix byte of 0x3. Note that the `u` for the keys must be between [0,u) if u != 0.

```yaml
FLE2EncryptionPlaceholder:
   description: "Implements Encryption BinData (subtype 6) sub-subtype 3, the intent-to-encrypt
       mapping. Contains a value to encrypt and a description of how it should be encrypted."
   strict: true
   fields:
     t:
       description: "The type number, determines what payload to replace the placeholder with"
       type: Fle2PlaceholderType
       cpp_name: type
     a:
       description: "The encryption algorithm to be used."
       type: FleAlgorithmInt
       cpp_name: algorithm
     ki:
       description: "Used to query the key vault by _id. If omitted, ka must be specified."
       type: uuid
       cpp_name: keyId
       optional: true
    ku:
       description: "UserKeyId, Used to query the key vault by _id.,
           Typically same as IndexKeyId unless explicit encryption is used."
       type: uuid
       cpp_name: userKeyId
    v:
       description: "value to encrypt"
       type: IDLAnyType
       cpp_name: value
    cm:
       description: "Queryable Encryption max contention counter"
       type: long
       cpp_name: maxContentionCounter
```

**type**
Depending on the type number, shell/libmongocrypt should replace the placeholder with the right payload.
| type | placeholder |
|------|---------------------------|
| 1 | FLE2InsertUpdatePayloadV2 |
| 2 | FLE2FindEqualityPayloadV2 |

**algorithm**

| algorithm | description      |
| --------- | ---------------- |
| 1         | Unindexed        |
| 2         | Indexed Equality |

`hasEncryptionPlaceholder` is added to the returned document. It is removed before being sent to the server.

## Formats for client -> server CRUD

FLE2InsertUpdatePayloadV2 and EncryptionInformation describe the per-field payload and new top-level attribute in the insert and update commands. It is stored inside a bindata 6 field with a prefix byte of 0x4.

```yaml
FLE2InsertUpdatePayloadV2:
  description: "Payload of an indexed field to insert or update"
  strict: true
  fields:
    d:
      description: "EDCDerivedFromDataTokenAndCounter"
      type: bindata_generic
      cpp_name: edcDerivedToken
    s:
      description: "ESCDerivedFromDataTokenAndCounter"
      type: bindata_generic
      cpp_name: escDerivedToken
    p:
      description: "Encrypted tokens"
      type: bindata_generic
      cpp_name: encryptedTokens
    u:
      description: "Index KeyId"
      type: uuid
      cpp_name: indexKeyId
    t:
      description: "Encrypted type"
      type: safeInt
      cpp_name: type
    v:
      description: "Encrypted value"
      type: bindata_generic
      cpp_name: value
    e:
      description: "ServerDataEncryptionLevel1Token"
      type: bindata_generic
      cpp_name: serverEncryptionToken
    l:
      description: "ServerDerivedFromDataToken"
      type: bindata_generic
      cpp_name: serverDerivedFromDataToken
    k:
      description: "Randomly sampled contention factor value"
      type: long
      cpp_name: contentionFactor
      validator: {gte: 0}

EncryptionInformation:
  description: "Implements Encryption Information which includes the schema for FLE 2 that is consumed by query_analysis, queries and write_ops"
  strict: true
  fields:
    type:
      description: "The version number"
      type: safeInt
      default: 1
    schema:
      description: "A map of NamespaceString to EncryptedFieldConfig"
      type: object
```

where the values in `FLE2InsertUpdatePayloadV2` are computed as follows:

```js
{
  d : EDCDerivedFromDataTokenAndCounter
  s : ESCDerivedFromDataTokenAndCounter
  p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter)
  v : UserKeyId + EncryptAEAD(K_KeyId, value),
  e : ServerDataEncryptionLevel1Token,
  l : ServerDerivedFromDataToken
  k : Randomly sampled contention factor value
}
```

## Formats for client -> server Query

FLE2FindEqualityPayloadV2 and EncryptionInformation describe the per-field payload and new top-level attribute in the find and aggregation commands. The FLE2FindEqualityPayloadV2 is the payload for the match expression `$eq` that replaced on the server with a `$in` like expression after running EmuBinary.

`$eq`:

```js
{
$eq : {
        d : EDCDerivedFromDataToken
        s : ESCDerivedFromDataToken
        l: ServerDerivedFromDataToken,
        cm : 0,
    }
}
```

```yaml
FLE2FindEqualityPayloadV2:
  description: "Payload for an equality find"
  strict: true
  fields:
    d:
      description: "EDCDerivedFromDataToken"
      type: bindata_generic
      cpp_name: edcDerivedToken
    s:
      description: "ESCDerivedFromDataToken"
      type: bindata_generic
      cpp_name: escDerivedToken
    l:
      description: "ServerDerivedFromDataToken"
      type: bindata_generic
      cpp_name: serverDerivedFromDataToken
    cm:
      description: "FLE2 max counter"
      type: long
      cpp_name: maxCounter
      optional: true
```

where the values in `FLE2FindEqualityPayload` are computed as follows:

```js
{
    d : EDCDerivedFromDataToken
    s : ESCDerivedFromDataToken
    e : max contention factor of field - comes from FLE2EncryptionPlaceholder.cm,
}
```

# Compact Command

```yaml
compactStructuredEncryptionData:
  description: "Parser for the 'compactStructuredEncryptionData ' Command"
  command_name: compactStructuredEncryptionData
  api_version: ""
  namespace: concatenate_with_db
  strict: true
  reply_type: CompactStructuredEncryptionDataCommandReply
  fields:
    compactionTokens:
      description: "Map of field path to ECOCToken"
      type: object

ECOCStats:
  description: "Stats about records in ECOC compact touched"
  fields:
    read: exactInt64
    deleted: exactInt64

ECStats:
  description: "Stats about records in ESC compact touched"
  fields:
    read: exactInt64
    inserted: exactInt64
    updated: exactInt64
    deleted: exactInt64

CompactStats:
  description: "Stats about records in ECOC and ESC compact touched"
  fields:
    ecoc: ECOCStats
    esc: ECStats

CompactStructuredEncryptionDataCommandReply:
  description: "Reply from the {compactStructuredEncryptionData: ...} command"
  strict: true
  fields:
    stats: CompactStats
```

# Cleanup Command

```yaml
cleanupStructuredEncryptionData:
  description: "Parser for the 'cleanupStructuredEncryptionData' command"
  command_name: cleanupStructuredEncryptionData
  api_version: ""
  namespace: concatenate_with_db
  strict: true
  reply_type: CleanupStructuredEncryptionDataCommandReply
  fields:
    cleanupTokens:
      description: "Map of field path to ECOCToken"
      type: object

CleanupStats:
  description: "Stats about records in ECOC and ESC cleanup touched"
  fields:
    ecoc: ECOCStats
    esc: ECStats

CleanupStructuredEncryptionDataCommandReply:
  description: "Reply from the {cleanupStructuredEncryptionData: ...} command"
  strict: true
  is_command_reply: true
  fields:
    stats: CleanupStats
```

# Reference: Server Status Compact and Cleanup

```js
{
    fle : {
        compactStats : CompactStats, // see IDL for CompactStats
        cleanupStats : CleanupStats // see IDL for CleanupStats
    }
}
```

# Reference: Explicit Encryption and Decryption

Explicit encryption is a function that part of the client driver object. It is configured with the key vault.

Encrypt generates the `FLE2InsertUpdatePayloadV2` payload

```ts
// From https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.rst#id32
enum IndexType {
    None = 0,
    Equality = 1,
    Range = 2,
}

class EncryptOpts {
   keyId : Optional<Binary>
   keyAltName: Optional<String>
   algorithm: String
   contentionFactor: Option<Number>, // New in FLE 2
   indexType: Option<IndexType>, // New in FLE 2
}

class ClientEncryption {
   ...
   // Encrypts a BSONValue with a given key and algorithm.
   // Returns an encrypted value (BSON binary of subtype 6). The underlying implementation may return an error for prohibited BSON values.
   encrypt(value: BSONValue, opts: EncryptOpts): Binary;

   // Decrypts an encrypted value (BSON binary of subtype 6). Returns the original BSON value.
   decrypt(value: Binary): BSONValue;
   ...
}
```

Encrypt returns an encrypted payload depending on the index type.

```
switch EncryptOpts.indexType:
    case None:
        byte(0x10) + Unindexed Encrypted Payload
    case Equality:
        byte(0xB) + FLE2InsertUpdatePayloadV2
```

Decrypt can decrypts a value generated by `encrypt()` or an encrypted value from the server.

`client.decrypt(value uint8_t[])` returns `BSONValue`.

# Reference: Explicit Equality Comparison

To generate a `$eq` operate like implicit encryption does, a user must call

```
client.FL2_TO_BE_NAMED_GENERATE_EQ(keyId UUID, value BSONElement, opts FL2_TO_BE_NAMED_GENERATE_EQ_Options)

class FL2_TO_BE_NAMED_GENERATE_EQ_Options {
    max_contention_factor int = 0,
}

```

which returns

`BSON( {"$eq" : BinData(6, byte(0xC) + FLE2FindEqualityPayloadV2 ) })`.
