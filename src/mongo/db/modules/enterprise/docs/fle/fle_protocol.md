# Queryable Encryption (FLE2) Protocol

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
- [Reference: Indexed Equality Encrypted Values - EDC Schema](#reference-indexed-equality-encrypted-values---edc-schema)
- [Reference: ESC Schema](#reference-esc-schema)
    - [Null record](#null-record)
    - [Insert record](#insert-record)
    - [Positional record](#positional-record)
    - [Compaction placeholder record](#compaction-placeholder-record)
- [Reference: ECC Schema](#reference-ecc-schema)
    - [Null record](#null-record-1)
    - [Regular record](#regular-record)
    - [Compaction placeholder record](#compaction-placeholder-record-1)
- [Reference: ECOC Schema](#reference-ecoc-schema)
<!-- - [Reference: OST-1 v3 algorithms](#reference-ost-1-v3-algorithms) -->
- [Reference: EmuBinary](#reference-emubinary)
- [Reference: Merge](#reference-merge)
- [Reference: Insert pseudo-code](#reference-insert-pseudo-code)
  - [Insert: Client-side](#insert-client-side)
  - [Insert: Server-side](#insert-server-side)
- [Reference: Delete pseudo-code](#reference-delete-pseudo-code)
  - [Delete: Client-side](#delete-client-side)
  - [Delete: Server-side](#delete-server-side)
- [Reference: Find pseudo-code](#reference-find-pseudo-code)
  - [Find: Client-side](#find-client-side)
  - [Find: Server-side](#find-server-side)
- [Reference: Update pseudo-code](#reference-update-pseudo-code)
  - [Update: Server-side](#update-server-side)
- [Reference: CompactStructuredEncryptionData](#reference-compactstructuredencryptiondata)
- [Reference: Compact pseudo-code](#reference-compact-pseudo-code)
- [Reference: Payloads](#reference-payloads)
  - [Formats for Create Collection and listCollections](#formats-for-create-collection-and-listcollections)
  - [Formats for Query Analysis](#formats-for-query-analysis)
  - [Formats for client -> server CRUD](#formats-for-client---server-crud)
  - [Formats for client -> server Query](#formats-for-client---server-query)
- [Compact Command](#compact-command)
- [Reference: Server Status Compact](#reference-server-status-compact)
- [Reference: Explicit Encryption and Decryption](#reference-explicit-encryption-and-decryption)
- [Reference: Explicit Equality Comparison](#reference-explicit-equality-comparison)
- [Reference: JSON Schema Validation Merging](#reference-json-schema-validation-merging)


# Terminology

- EDC - Encrypted Data Collection (i.e. the user collection)
- ECC - Encrypted Cache Collection
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
dbTest.createCollection("testColl", { encryptedFields : {
    "fields": [
        {
            "path": "encryptedField",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "bsonType": "string",
            "queries": { "queryType": "equality" }
        },
        {
            "path": "nested.otherEncryptedField",
            "keyId": UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "bsonType": "string",
            "queries": [{ "queryType": "equality" }]
        },
    ]
}});
```


listCollections returns the following

```js
{
    "name" : "testColl",
    "type" : "collection",
    "options" : {
        "encryptedFields" : {
            "escCollection" : "fle2.testColl.esc",
            "eccCollection" : "fle2.testColl.ecc",
            "ecocCollection" : "fle2.testColl.ecoc",
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
    "_id" : 1,
    "encryptedField" : "secret",
    "nested" : {
        "otherEncryptedField" : "secret",
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
        "encryptedField" : BinData(6, byte(0x4) + BSON({
                d : EDCDerivedFromDataTokenAndCounter
                s : ESCDerivedFromDataTokenAndCounter
                c : ECCDerivedFromDataTokenAndCounter
                p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter || ECCDerivedFromDataTokenAndCounter)
                v : EncryptAEAD(UserKey, "secret")
            }
        )),
        "nested" {
            "otherEncryptedField" : BinData(6, byte(0x4) + BSON({
                    d : EDCDerivedFromDataTokenAndCounter
                    s : ESCDerivedFromDataTokenAndCounter
                    c : ECCDerivedFromDataTokenAndCounter
                    p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter || ECCDerivedFromDataTokenAndCounter)
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

User creates this delete for testDB.testColl

```json
{
    "_id" : 1,
}
```

Shell/libmongocrypt sends the following to query analysis:
```js
{
    delete: "testColl",
    $db: "testDB",
    deletes: [{
        q: {
            "_id" : 1
        },
        limit: 1
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

Query analysis returns the following by adding placeholders for fields to encrypt
(Note there are no changes)
```js
{
    hasEncryptionPlaceholders : false,
    result: {
        delete: "testColl",
        $db: "testDB",
        deletes: [{
            q: {
                "_id" : 1
            },
            limit: 1
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
}
```

Libmongocrypt replaces the placeholders information for delete, adds tokens and sends it to the server

```js
{
    delete: "testColl",
    $db: "testDB",
    deletes: [{
        q: {
            "_id" : 1
        },
        limit: 1
    }],
    encryptionInformation : {
        type : 1,
        tokens : {
            "encryptedField" : {
                data : ServerDataEncryptionLevel1Token,
                ecoc : ECOCToken,
            },
            "nested.otherEncryptedField" : {
                data : ServerDataEncryptionLevel1Token,
                ecoc : ECOCToken,
            }
        },
        "encryptedFields" : {
            // omitted for brevity
            // see EncryptedFields output from listCollections above
        }
    }
}
```

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


Query analysis returns the following by transforming the $eq into a new pseudo match expression $fle_pseudo_to_be_named_by_query. This new pseudo match expression is explained in the reference section

```js
{
    hasEncryptionPlaceholders : true,
    result: {
        find: "testColl",
        $db: "testDB",
        filter: {
            "encryptedField" : { 
                $fle_pseudo_to_be_named_by_query : BinData(6, byte(0x3) + BSON({
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
            $fle_pseudo_to_be_named_by_query : BinData(6, byte(0x5) + BSON({
                d : EDCDerivedFromDataToken
                s : ESCDerivedFromDataToken
                c : ECCDerivedFromDataToken,
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

On the server-side, find() rewrites the query after querying ESC and ECC.


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
        "_id" : 1,
    },
    {
        $set : { "encryptedField" : "newSecret" }
    }
)
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
                "encryptedField" : BinData(6, byte(0x4) + BSON(
                {
                d : EDCDerivedFromDataTokenAndCounter
                s : ESCDerivedFromDataTokenAndCounter
                c : ECCDerivedFromDataTokenAndCounter
                p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter || 
                                       ECCDerivedFromDataTokenAndCounter)
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
client.encrypt(UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
            "secret",
            optional_contention=1)
```

returns the following
```js
uint8_t[] = byte(0x4) + BSON({
        d : EDCDerivedFromDataTokenAndCounter
        s : ESCDerivedFromDataTokenAndCounter
        c : ECCDerivedFromDataTokenAndCounter
        p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter ||
                               ECCDerivedFromDataTokenAndCounter)
        v : EncryptAEAD(UserKey, "secret")
})
```


```js
client.decrypt(EncryptedFieldValue)
```
where
`EncryptedFieldValue = byte(0x7) + S_KeyId + BSONType + Encrypt(ServerDataEncryptionLevel1Token, Struct(K_KeyId, v, count, d, s, c))`
or
`EncryptedFieldValue = byte(0x4) + FLE2InsertUpdatePayload` returns the following:

```js
"secret"
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
    "encryptedField" : db.FL2_TO_BE_NAMED_GENERATE_EQ(UUID("11d58b8a-0c6c-4d69-a0bd-70c6d9befae9"),
    "secret",
    optional_max_contention=1) } )
```

the function `FL2_TO_BE_NAMED_GENERATE_EQ` returns the following BSON object:

```js
{
    "$fle_pseudo_to_be_named_by_query" : BinData(6, byte(0x5) + BSON({
        d : EDCDerivedFromDataToken
        s : ESCDerivedFromDataToken
        c : ECCDerivedFromDataToken
        e : 0
    }
}
```

# Reference: Cryptography

HMAC is HMAC-SHA-256
Encrypt/Decrypt is AES-256-CTR. It does not provide integrity, only confidentiality.
EncryptAEAD/DecryptAEAD is AES-256-CTR-HMAC-SHA-256.

# Reference: BinData 6 subtypes

In FLE 1, Bindata 6 had several subtypes as denoted by the first byte of a bindata field. FLE 2 adds several new ones described below

0. FLE 1 Encryption Placeholder
1. FLE 1 Deterministic Encrypted Value
2. FLE 1 Random Encrypted Value
3. FLE 2 Encryption Placeholder (see FLE2EncryptionPlaceholder below)
4. FLE 2 Insert Update Payload (see FLE2InsertUpdatePayload below)
5. FLE 2 Find Payload (see FLE2FindEqualityPayload below)
6. FLE 2 Unindexed Encrypted Value
7. FLE 2 Equality Indexed Encrypted Value (see EDC Schema below)

<!-- TODO - add table for where each is used -->

# Reference: Keys and Tokens

`S_KeyId` or `IndexKey` is a per-field key material. It is 96 bytes since that is the same size as the original FLE 1 value. From this index key, a normal of tokens are derived via HMAC as described below.

`K_KeyId` is either chosen by the user or is `S_KeyId/IndexKey`.

Since it is possible for `IndexKey == UserKey`, we choose the key and HMAC components assuming they are shared.

**Key Derivation**
* AEAD AES Key (aka `Ke`) is the first 32 bytes of `IndexKey`.
* AEAD HMAC Key (aka `Km`) is the second 32 bytes of `IndexKey`.
* Token HMAC Key is the third 32 bytes of `IndexKey`.
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
ServerDataEncryptionLevel1Token = HMAC(IndexKey, 3) = K_{f,3} = Fs[f,3]

EDCToken = HMAC(CollectionsLevel1Token, 1) = K^{edc}_f
ESCToken = HMAC(CollectionsLevel1Token, 2) = K^{esc}_f
ECCToken = HMAC(CollectionsLevel1Token, 3) = K^{ecc}_f
ECOCToken = HMAC(CollectionsLevel1Token, 4) = K^{ecoc}_f = Fs[f,1,4]

EDCDerivedFromDataToken = HMAC(EDCToken, v) = K^{edc}_{f,v} = Fs[f,1,1,v]
ESCDerivedFromDataToken = HMAC(ESCToken, v) = K^{esc}_{f,v} = Fs[f,1,2,v]
ECCDerivedFromDataToken = HMAC(ECCToken, v) = K^{ecc}_{f,v} = Fs[f,1,3,v]

EDCDerivedFromDataTokenAndContentionFactorToken = HMAC(EDCDerivedFromDataToken, u) =
Fs[f,1,1,v,u] ESCDerivedFromDataTokenAndContentionFactorToken = HMAC(ESCDerivedFromDataToken, u)
= Fs[f,1,2,v,u] ECCDerivedFromDataTokenAndContentionFactorToken = HMAC(ECCDerivedFromDataToken,
u) = Fs[f,1,3,v,u]

EDCTwiceDerivedToken = HMAC(EDCDerivedFromDataTokenAndContentionFactorToken, 1) = Fs_edc(1)
ESCTwiceDerivedTagToken = HMAC(ESCDerivedFromDataTokenAndContentionFactorToken, 1) = Fs_esc(1)
ESCTwiceDerivedValueToken = HMAC(ESCDerivedFromDataTokenAndContentionFactorToken, 2) = Fs_esc(2)
ECCTwiceDerivedTagToken = HMAC(ECCDerivedFromDataTokenAndContentionFactorToken, 1) = Fs_ecc(1)
ECCTwiceDerivedValueToken = HMAC(ECCDerivedFromDataTokenAndContentionFactorToken, 2) = Fs_ecc(2)
```

# Reference: Schema

Four collections ( r = read, w = write)
- EDC - (user data) - all
- ESC - insert(r/w), query(r), update(r/w), compact (r/w)
- ECC - query(r), delete(r/w), update(r/w), compact (r/w)
- ECOC - insert(w), update(w), delete(w), compact (r/w)

# Reference: Naming rules for State collections

Naming rules for state collections:

ESC: `fle2.<base_collection_name>.esc`
ECC: `fle2.<base_collection_name>.ecc`
ECOC: `fle2.<base_collection_name>.ecoc`

Example for User Collection Named: `example`
ESC: `fle2.example.esc`
ECC: `fle2.example.ecc`
ECOC: `fle2.example.ecoc`


# Reference: Unindexed Encrypted Payload

This is the same as type 2 but with the new CTR-AEAD mode for the cipher.

```c
struct {
    uint8_t fle_blob_subtype = 6;
    uint8_t key_uuid[16];
    uint8  original_bson_type;
    ciphertext[ciphertext_length];
}
```

# Reference: Indexed Equality Encrypted Values - EDC Schema

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

`BinData(6, byte(0x7) + S_KeyId + BSONType + Encrypt(ServerDataEncryptionLevel1Token, Struct(K_KeyId, v, count, d, s, c)))`

```c
struct {
    uint8_t fle_blob_subtype = 7;
    uint8_t key_uuid[16]; // S_KeyId aka IndexKeyId
    uint8  original_bson_type;
    ciphertext[ciphertext_length];
}
```

where `ciphertext_length = sizeof(struct) - 17`. Everything except `cipherText` is associatedData.

The format of the encrypted data is as follow:

```c
struct {
    uint8_t[length] cipherText; // UserKeyId + EncryptAEAD(K_KeyId, value)
    uint64_t counter;
    uint8_t[32] edc;  // EDCDerivedFromDataTokenAndCounter
    uint8_t[32] esc;  // ESCDerivedFromDataTokenAndCounter
    uint8_t[32] ecc;  // ECCDerivedFromDataTokenAndCounter
}
```

where `ciphertext_length = sizeof(struct) - (16 + 8 + 3 * 64)`. Everything except `cipherText` is associatedData.

Tags are formatted as

`HMAC(EDCTwiceDerivedToken, count)`


# Reference: ESC Schema

Encrypted State Collection (ESC) stores information about how many field/value pairs exist in EDC. It is written during insert and update. It is read during queries, insert, update and compaction.

Example set of values for a given field and value pair:

| **pos** | **count** |
| ------- | --------- |
| null    | 3         |
| 1       | 4         |
| 2       | 5         |
| 3       | 6         |


```js
{
   _id : HMAC(ESCTwiceDerivedTagToken, type || pos )
   value : Encrypt(ESCTwiceDerivedValueToken,  count_type || count)
}
```
```
where
 type = uint64_t
 pos = uint64_t
 count_type = uint64_t
 count = uint64_t

where type
  0 - null record
  1 - insert record, positional record, or compaction record

where count_type:
  0 - regular count
  [1, UINT64_MAX) = position
  UINT64_MAX - compaction placeholder
```

There are four types of records in the ESC collection:
1. Null: 0 or 1 records
2. Insert: 0 or more records
3. Positional: 0 or more records
4. Compaction: 0 or 1 records

### Null record

Null records, also known as \bot records, store the initial count of the field/value pair. These records are created and updated by compact. It is read during EmuBinary.

```js
{
   _id : HMAC(ESCTwiceDerivedTagToken, 0 )
   value : Encrypt(ESCTwiceDerivedValueToken,  pos || count)
}
```

### Insert record

Insert records record an updated item count. They are created by insert and update.

```js
{
   _id : HMAC(ESCTwiceDerivedTagToken, 1 || pos )
   value : Encrypt(ESCTwiceDerivedValueToken,  0 || count)
}
```

### Positional record

Positional records is created or updated by compaction.

```js
{
   _id : HMAC(ESCTwiceDerivedTagToken, 1 || pos )
   value : Encrypt(ESCTwiceDerivedValueToken,  pos_update || count)
}
```

### Compaction placeholder record

A compaction placeholder records is a short-lived record created and deleted by compaction. It is used to synchronize with insert and update to ensure their ESC records have no gaps

```js
{
   _id : HMAC(ESCTwiceDerivedTagToken, 1 || pos )
   value : Encrypt(ESCTwiceDerivedValueToken,  UINT64_MAX || 0)
}
```

# Reference: ECC Schema

Encrypted Cache Collection (ECC) stores information about which records in EDC were deleted. It is written during delete and update. It is read during queries, update, delete and compaction.


Example set of values for a given field and value pair:

| **pos** | **gap** |
| ------- | ------- |
| null    | 2       |
| 3       | (3,5)   |
| 4       | (10,10) |

This encodes that gaps `[(3,5), 10]`.

```js
{
   _id : HMAC(ECCTwiceDerivedTagToken, type || pos )
   value : Encrypt(ECCTwiceDerivedValueToken,  count OR start || end )
}
```
```
 where
  type = uint64_t
  pos = uint64_t
  value is either:
       count = uint64_t  // Null records
    OR
       start = uint64_t  // Other records
       end = uint64_t

 where type:
   0 - null record
   1 - regular record or compaction record

 where start and end:
   [0..UINT_64_MAX) - regular start and end
   UINT64_MAX - compaction placeholder
```

Record types:

There are three types of records in the ECC collection:
1. Null: 0 or 1 records
2. Regular: 0 or more records
4. Compaction: 0 or 1 records


### Null record
Null records record the starting position for a set of regular records. They are created by compact

```js
{
   _id : HMAC(ECCTwiceDerivedTagToken, 0 )
   value : Encrypt(ECCTwiceDerivedValueToken,  count)
}
```

### Regular record
Regular records record a list of deleted ranges for a given field/value pair. They are created by update, delete and compact. Updates and deletes insert tuples of the form (N, N). Compact creates records of (M, N) where M < N.

```js
{
   _id : HMAC(ECCTwiceDerivedTagToken, 1 || pos )
   value : Encrypt(ECCTwiceDerivedValueToken,  start || end)
}
```

### Compaction placeholder record

A compaction placeholder records is a short-lived record created and deleted by compaction. It is used to synchronize with delete and update to ensure their ECC records have no gaps

```js
{
   _id : HMAC(ECCTwiceDerivedTagToken, 1 || pos )
   value : Encrypt(ECCTwiceDerivedValueToken,  UINT64_MAX || UINT64_MAX)
}
```

# Reference: ECOC Schema

Encrypted Compaction Collection (ECOC) stores information about which where inserted into ESC and ECC. The presence of records in ECOC is a signal that compaction may help reduce the space usage of ESC and ECC. It is an unordered collection of documents. The value field may have duplicate entries in the collection.

```js
{
   _id : ObjectId() -- omitted so MongoDB can auto choose it
   fieldName : String,
   value : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter || ECCDerivedFromDataTokenAndCounter)
}
```

<!--
  # Reference: OST-1 v3 algorithms

 - add Merge
  - add EmuBinary - Figure 28 - everything - finds highest value in ECC or ESC
  - add GetCounter - Figure 29 - find - means to get a counter from ESC, embedded in find psuedo code
  - add InsertFields - Figure 30 - update/delete - add to ESC and ECOC, embedded in insert psuedo code
  - add GetGaps - Figure 31 - find - queries ECC, embedded in find psuedo code
  - add InTags - Figure 32 - find conjunction - unknown - ?
  - add GarbageCollect - Figure 33 - update/delete - add to ECC and ECOC, embedded in delete psuedo code
-->

# Reference: EmuBinary

See Figure 28 in OST-1 v3

EmuBinary is identical for both ESC and ECC except for one if condition decoding the NULL document.

```python
def EmuBinary(TagToken, ValueToken, coll_name: "esc" or "ecc"):
    lambda = 0
    i = 0

    r_null_doc = db.coll.find(_id: HMAC(TagToken, null) )

    if r_null_doc != null:
        if coll_name == "esc":
            [pos || count] = Decrypt(TagValue, r_null_doc.value)
        else:
            pos = Decrypt(TagValue, r_null_doc.value)
        lambda = pos +1

    rho = db.coll.count()

    while True:
        r_doc = db.coll.find( _id: HMAC(TagToken, rho + lambda))
        if r_doc != null:
            rho = 2 * rho
        else:
            break

    median = 0,
    min = 1
    max = rho

    for j >= 1 and j <= ceil(log2(rho)):
        median = ceil( (max - min) / 2 ) + min
        r_doc = db.coll.find( _id: HMAC(TagToken(median + lambda))

        if r_doc != null:
            min = median
            if j == ceil(log2(rho)):
                i = min + lambda
        else: # if r_doc == null:
            max = median
            if j == ceil(log2(rho)) and min == 1:
                r_doc = db.coll.find( _id: HMAC(TagToken(1 + lambda))
                if r_doc != null:
                    i = 1 + lambda
            else j == ceil(log2(rho)) and min != 1
                i = min + lambda

    return i

```

# Reference: Merge

 Goal is to merge a series of gaps where a gap is defined a set of sequence of continuous integers. For instance: `[1,2,3]` can be represented as `(1,3)`

Examples:
```[1,2,3,5] = [(1,3),5]
[1,2,3,5,8,9,10] = [(1,3),5,(8,10)]
[(1,3),4,5,8,9,10] = [(1,5), (8,10)]
```

Constraints:
1. There will be no overlapping intervals. For instance `[(1,4), (3,5)]` is an illegal state.
2. Gaps may be width >= 0. A zero width interval is a single item.

Figure 7, Merge algorithm is one approach. There may be more efficient structures. A sorted list of tuples or an interval tree (https://en.wikipedia.org/wiki/Interval_tree) or others.

# Reference: Insert pseudo-code

Insert consists of two phases: client-side and server-side.

## Insert: Client-side

1. Retrieve `encryptedFields` for the target collection
2. Use query_analysis to transform encrypted fields into `FLE2EncryptionPlaceholder` and append `EncryptionInformation`.
3. Transform placeholder fields into `FLE2InsertUpdatePayload` or FLE 2 Unindexed Encrypted Value. Append `EncryptionInformation`
4. Send document to server


## Insert: Server-side

Server-side is responsible for adding new records into ESC, adding new records into ECOC, and finalizing the EDC document.

```python
tags = new vector()

for encrypted field f in document as FLE2InsertUpdatePayload:
    [ESCTwiceDerivedTagToken, ESCTwiceDerivedValueToken] = Derive(f.s)

    a = EmuBinary(ESCTwiceDerivedTagToken, ESCTwiceDerivedValueToken, "esc")
    if a == 0 then
        set pos = 1 and count = 1
    else
        r_esc = db.esc.find(HMAC(ESCTwiceDerivedTagToken, a))
        [sigma_1, sigma_2] = Decrypt(ESCTwiceDerivedValueToken, r_esc.value)
        if sigma_1 = compaction placeholder then
            abort
        if a == null then
            pos = sigma_1 + 2
            count = sigma_2 + 1
        else
            pos = a + 1
            count = sigma_2 + 1

    db.esc.insert( _id: HMAC(ESCTwiceDerivedTagToken, pos), 
                   Encrypt(ESCTwiceDerivedValueToken,  0 || count))

    EDCTwiceDerivedToken = Derive(f.d)

    tags.add(HMAC(EDCTwiceDerivedToken, count))

    db.ecoc.insert(field: fieldName, value: v.p )

    f := BinData(6, byte(0x7) + S_KeyId + BSONType + 
                    Encrypt(Struct(f.p, count, f.d, f.s, f.c)))

Append Tags vector as __safeContent__ to document
```

# Reference: Delete pseudo-code

Delete consists of two phases: client-side and server-side.

## Delete: Client-side

1. Retrieve `encryptedFields` for the target collection
2. Use query_analysis to transform encrypted fields into `FLE2EncryptionPlaceholder` and append `EncryptionInformationForQueryAnalysis`.
3. Transform placeholder fields into `FLE2FindEqualityPayload` and append `EncryptionInformation`
4. Send document to server


## Delete: Server-side
<!-- 
TODO - two cases - searches on encrypted key and non-encrypted keys
TODO - find and modify transformation
 -->
```python
document = db.edc.delete()

for encrypted field f in EncryptionInformation:
    ef = document[f.name]
    Struct(K_KeyId, v, count, d, s, c) = Decrypt(f.e (aka ServerDataEncryptionLevel1Token), ef)

    ECCDerivedFromDataTokenAndCounter = Derive(f.c)
    [ECCTwiceDerivedTagToken, ECCTwiceDerivedValueToken] = Derive(ECCDerivedFromDataTokenAndCounter)

    a = EmuBinary(ECCTwiceDerivedTagToken, ECCTwiceDerivedValueToken, "ecc")
    if a != 0 then
        r_ecc = db.ecc.find( HMAC(ECCTwiceDerivedTagToken, a) )
        sigma = Decrypt(ECCTwiceDerivedValueToken, r_ecc)
        if sigma == UINT64_MAX then
            abort

    if a == null then
        tag = HMAC(ECCTwiceDerivedTagToken, sigma + 2)
    else a == 0 then
        tag = HMAC(ECCTwiceDerivedTagToken, 1)
    else
        tag = HMAC(ECCTwiceDerivedTagToken, sigma + 1)

    db.ecc.insert(tag, Encrypt(ECCTwiceDerivedValueToken, count))

    db.ecoc.insert(field: fieldName, 
                   value: Encrypt(f.o, ESCDerivedFromDataTokenAndCounter || 
                                       ECCDerivedFromDataTokenAndCounter))
```

# Reference: Find pseudo-code

Find consists of two phases: client-side and server-side. The client-side is the same as Insert.

## Find: Client-side

1. Retrieve `encryptedFields` for the target collection
2. Use query_analysis to transform encrypted fields into `FLE2EncryptionPlaceholder` and append `EncryptionInformation`.
3. Transform placeholder fields into `FLE2FindEqualityPayload` and append `EncryptionInformation`
4. Send document to server


## Find: Server-side

Server-side is responsible for querying ESC and ECC for each encrypted field, and generate a set of tags to search for.

```python
tags = new vector()

for encrypted field f in document as FLE2FindEqualityPayload:
    for u in f.cm
        ESCDerivedFromDataTokenAndCounter = Derive(f.s)
        ECCDerivedFromDataTokenAndCounter = Derive(f.c)
        [ESCTwiceDerivedTagToken, ESCTwiceDerivedValueToken] = Derive(ESCDerivedFromDataTokenAndCounter)
        [ECCTwiceDerivedTagToken, ECCTwiceDerivedValueToken] = Derive(ECCDerivedFromDataTokenAndCounter)

        a = EmuBinary(ESCTwiceDerivedTagToken, ESCTwiceDerivedValueToken, "esc")
        if a != 0
            r_esc = db.esc.find(HMAC(ESCTwiceDerivedTagToken, 1 || a))
            [sigma_1, sigma_2] = Decrypt(ESCTwiceDerivedValueToken, r_esc.value)
            count = sigma_2


            r_ecc = db.ecc.find(HMAC(ECCTwiceDerivedTagToken, 0 ))
            if r_ecc != null then:
                sigma_ecc = Decrypt(ECCTwiceDerivedValueToken, r_ecc.value )
                pos = sigma_ecc + 2
            else
                pos = 1

            g = new set()

            do
                r_ecc = db.ecc.find(HMAC(ECCTwiceDerivedTagToken, 1 || pos))
                sigma_ecc = Decrypt(ECCTwiceDerivedValueToken, r_ecc.value )
                if sigma_ecc != compaction placeholder:
                    g.add(sigma_ecc)
                else
                    break

                pos += 1
            loop

            for t in ([1..count] - g)
               tags.add(HMAC(EDCTwiceDerivedToken, t))

Finally, Replace $fle_pseudo_to_be_named_by_query with:
    __safeContent__ :
            $in : [
                tags...
            ]
        }

Run find()

```

# Reference: Update pseudo-code

Insert consists of two phases: client-side and server-side. The client-side is the same as Insert.

Prohibited features:
multi = true

## Update: Server-side

Server-side update is a combination of the steps for Find, Insert and Delete. The document has to be updated twice.
In the case of $set, the first pass update the new value and add new tags. In the case of $unset, the first pass removes the value. The second pass, in both cases, removes the old tags from __safeContent__.

1. Transform the update into a findAndModify command
2. Transform the query field per "Find: Server-side" if needed
3. Transform the update field per "Insert: Server-side" if needed
4. Append the tags from Step 3 as `{ $push : {__safeContent__ : [tags] }` to the update field.
   If the update already has `$push`, merge them.
5. Run the following algorithm
```python
Option 1 - diff the encrypted fields in the two documents
original_document = db.coll.findAndModify(new: false)
original_encrypted_fields = GetEncryptedFields(EncryptedFields, original_document)
new_document = db.coll.find(_id)
new_encrypted_fields = GetEncryptedFields(EncryptedFields, new_document)
tags_to_remove = original_encrypted_fields - new_encrypted_fields
db.coll.findAndModify($pull: tags_to_remove)
```
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
        ecc : {
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



# Reference: Compact pseudo-code

CompactStructuredEncryptionData can be run on either mongos or mongod. It does the following psuedo code.

```python
Steps:
1. Rename from fle2.coll.ecoc to fle2.coll.ecoc_compact
2. Create fle2.coll.ecoc respecting original fle2.coll.ecoc's sharding

3. foreach field in compactStructuredEncryptionData.compactionTokens
    key_pairs = set()

    documents = db.ecoc.find(field.name)
    foreach document in documents:
        key_pair = Decrypt(field.value, document.value)
        key_pairs.add(key_pair)

4. foreach key_pair in key_pairs:
    [ESCDerivedFromDataTokenAndCounter, ECCDerivedFromDataTokenAndCounter] = key_pair

    a = EmuBinary(ESCTwiceDerivedTagToken, ESCTwiceDerivedValueToken, "esc")
    r_esc = db.esc.find(_id : HMAC(ESCTwiceDerivedTagToken, a)) where sigma_1 || sigma_2 = r_esc
    if a == 0 then
        esc_pos = sigma_1 + 2
        esc_count = sigma_2
    else a >= 1 then
        esc_pos = a + 1
        esc_count = sigma_2

    db.esc.insert(HMAC(ESCTwiceDerivedTagToken, 1 || pos), 
                  Encrypt(ESCTwiceDerivedValueToken, UINT64_MAX || 0 ))

    if insert fails then
        esc_pos += 1
        count += 1
        retry db.esc.insert()...

    if a != 0 then
        r_esc = db.esc.find(HMAC(ECCTwiceDerivedTagToken, 0 ))
        if !r_esc.empty() then
            sigma_1 || sigma_2 = r_esc
            esc_ipos = sigma_1 + 2
        else
            esc_ipos = 1
    else
       esc_ipos = esc_pos

  // Begin Part 2 - ECC
    g = set()
    r_ecc = db.ecc.find(_id:  HMAC(ECCTwiceDerivedTagToken, 0 ))
    if !r_ecc.empty() then
        sigma = Decrypt(ECCTwiceDerivedValueToken, r_ecc.value)
        ecc_pos = sigma + 2
    else
        ecc_pos = 1

    ecc_ipos = ecc_pos
    while true
        r_ecc = db.ecc.find( _id : HMAC(ECCTwiceDerivedTagToken, 1 || ecc_pos ))
        if r_ecc.empty then
            break
        g.add(Decrypt(ECCTwiceDerivedValueToken, r_ecc.value))
        ecc_pos += 1

    g_merged = Merge(g)
    if g_merged != g then
        db.ecc.insert(_id: HMAC(ECCTwiceDerivedTagToken, 1 || pos ),
                      value: Encrypt(ECCTwiceDerivedValueToken,  UINT64_MAX))
        if insert fails:
            set ecc_pos +=1
            retry insert

  // Begin Part 3 -
    if g_merged.size() != ecc_count then // if user has not deleted all records for a value
        // i.e. g_merged != {1..ecc_count}
        if g_merged != g then
            for k in [g_merged.count(), 1] then
                db.ecc.insert(HMAC(ECCTwiceDerivedTagToken(1 || ecc_pos + k), 
                              Encrypt(ECCTwiceDerivedValueToken, g.at(k)))

            if ecc_ipos != 1 then
                db.ecc.update(_id: HMAC(ECCTwiceDerivedTagToken, 0),
                              value: Encrypt(ECCTwiceDerivedValueToken, ecc_pos - 1))
            else
                db.ecc.insert(_id: HMAC(ECCTwiceDerivedTagToken, 0),
                              value: Encrypt(ECCTwiceDerivedValueToken, ecc_pos - 1))

            for k in [ecc_ipos, ecc_pos] then
                db.ecc.delete(_id: HMAC(ECCTwiceDerivedTagToken, 1 || k))

            for k in [esc_ipos, esc_pos] then
                db.esc.delete(_id: HMAC(ESCTwiceDerivedTagToken, 1 || k ))

            if esc_ipos != 1 then
                db.esc.update(HMAC(ESCTwiceDerivedTagToken, 0 ),
                        value : Encrypt(ESCTwiceDerivedValueToken, esc_pos -1 || esc_count))
            else
                db.esc.insert(HMAC(ESCTwiceDerivedTagToken, 0 ),
                        value : Encrypt(ESCTwiceDerivedValueToken, esc_pos -1 || esc_count))
        else
            for k in [ecc_ipos, ecc_pos] then
                db.ecc.delete(_id: HMAC(ECCTwiceDerivedTagToken, 1 || k))

            for k in [esc_ipos, esc_pos] then
                db.esc.delete(_id: HMAC(ESCTwiceDerivedTagToken, 1 || k ))

            if esc_pos != 1 then
                db.ecc.delete(_id: HMAC(ECCTwiceDerivedTagToken, 0))
                db.esc.delete(_id: HMAC(ESCTwiceDerivedTagToken, 0))

5. if db.ecoc.count() > 0 then:
    warn the user as this indicates a bug or they were missing compactionTokens in the initial request

6. fle2.coll.ecoc_compact.drop()
```


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
            description: "Encrypted State Collection name, defaults to fle2.<collection>.esc"
            type: string
            optional: true
        eccCollection:
            description: "Encrypted Cache Collection name, defaults to fle2.<collection>.ecc"
            type: string
            optional: true
        ecocCollection:
            description: "Encrypted Compaction Collection name, defaults to fle2.<collection>.ecoc"
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
        deleteTokens:
            description: "A map of field paths to FLEDeletePayload"
            type: object
            optional: true
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
     ka:
       description: "Used to query the key vault by keyAltName. If omitted,
           ki must be specified."
       type: string
       cpp_name: keyAltName
       optional: true
     v:
       description: "value to encrypt"
       type: IDLAnyType
       cpp_name: value
     cm:
       description: "FLE2 max contention counter"
       type: long
       cpp_name: maxContentionCounter
       optional: true
```

**type**
Depending on the type number, shell/libmongocrypt should replace the placeholder with the right payload.
| type | placeholder             |
| ---- | ----------------------- |
| 1    | FLE2InsertUpdatePayload |
| 2    | FLE2FindEqualityPayload |


**algorithm**

| algorithm | description      |
| --------- | ---------------- |
| 1         | Unindexed        |
| 2         | Indexed Equality |

`hasEncryptionPlaceholder` is added to the returned document. It is removed before being sent to the server.

## Formats for client -> server CRUD

FLE2InsertUpdatePayload and EncryptionInformation describe the per-field payload and new top-level attribute in the insert and update commands. It is stored inside a bindata 6 field with a prefix byte of 0x4.

```yaml
FLE2InsertUpdatePayload:
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
     c:
       description: "ECCDerivedFromDataTokenAndCounter"
       type: bindata_generic
       cpp_name: eccDerivedToken
     p:
       description: "Encrypted tokens"
       type: bindata_generic
       cpp_name: encryptedTokens
     u:
       description: "User KeyId"
       type: uuid
       cpp_name: userKeyId
     v:
       description: "Encrypted value"
       type: bindata_generic
       cpp_name: value
     t:
       description: "Encrypted value type"
       type: bindata_generic
       cpp_name: type
     e:
       description: "ServerDataEncryptionLevel1Token"
       type: bindata_generic
       cpp_name: serverEncryptionToken

EncryptionInformation:
    description: "Implements Encryption Information which includes the schema for FLE 2 that is consumed by query_analysis, queries and write_ops"
    strict: true
    fields:
        type:
            description: "The version number"
            type: safeInt
            default: 1
        deleteTokens:
            description: "A map of field paths to FLEDeletePayload"
            type: object
            optional: true
        schema:
            description: "A map of NamespaceString to EncryptedFieldConfig"
            type: object
```

where the values in `FLE2InsertUpdatePayload` are computed as follows:
```js
{
  d : EDCDerivedFromDataTokenAndCounter
  s : ESCDerivedFromDataTokenAndCounter
  c : ECCDerivedFromDataTokenAndCounter
  p : Encrypt(ECOCToken, ESCDerivedFromDataTokenAndCounter || ECCDerivedFromDataTokenAndCounter)
  v : UserKeyId + EncryptAEAD(K_KeyId, value),
  e : ServerDataEncryptionLevel1Token,
}
```


FLEDeletePayload and EncryptionInformation describe the per-field payload and new top-level attribute in the insert and update commands.
```yaml
FLEDeletePayload:
    description: "Payload of an indexed field to delete"
    strict: true
    fields:
      o:
        description: "ECOCToken"
        type: bindata_generic
        cpp_name: ecocToken
      e:
        description: "ServerDataEncryptionLevel1Token"
        type: bindata_generic
        cpp_name: serverEncryptionToken
```

where the values in `FLEDeletePayload` are computed as follows:
```js
{
   e : ServerDataEncryptionLevel1Token,
   o : ECOCToken,
}
```

## Formats for client -> server Query

FLE2FindEqualityPayload and EncryptionInformation describe the per-field payload and new top-level attribute in the find and aggregation commands. The FLE2FindEqualityPayload is the payload for a new match expression `$fle_psuedo_eq` that replaced on the server with a `$in` like expression after running EmuBinary.

`$fle_psuedo_eq`:
```js
{
$fle_pseudo_to_be_named_by_query : {
        d : EDCDerivedFromDataToken
        s : ESCDerivedFromDataToken
        c : ECCDerivedFromDataToken,
        cm : 0,
    }
}
```

```yaml
FLE2FindEqualityPayload:
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
      c:
        description: "ECCDerivedFromDataToken"
        type: bindata_generic
        cpp_name: eccDerivedToken
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
    c : ECCDerivedFromDataToken
    e : max contention factor of field - comes from FLE2EncryptionPlaceholder.cm,
}
```

# Compact Command

```yaml
compactStructuredEncryptionData :
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
        read : exactInt64
        deleted : exactInt64

ECStats:
    description: "Stats about records in ECC & ESC compact touched"
    fields:
        read : exactInt64
        inserted : exactInt64
        updated : exactInt64
        deleted : exactInt64

compactStats:
    description: "Stats about records in ECC, ECOC and ESC compact touched"
    fields:
        ecoc: ECOCStats
        ecc: ECStats
        esc: ECStats

CompactStructuredEncryptionDataCommandReply:
    description: 'Reply from the {compactStructuredEncryptionData: ...} command'
    strict: true
    fields:
        stats: compactStats

```

# Reference: Server Status Compact

```js
{
    fle : {
        compactStats : compactStats // see IDL for compactStats
    }
}
```

# Reference: Explicit Encryption and Decryption

Explicit encryption is a function that part of the client driver object. It is configured with the key vault.

Encrypt generates the `FLE2InsertUpdatePayload` payload

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
        byte(0x6) + Unindexed Encrypted Payload
    case Equality:
        byte(0x4) + FLE2InsertUpdatePayload
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

`BSON( {"$fle_pseudo_to_be_named_by_query" : BinData(6, byte(0x5) + FLE2FindEqualityPayload ) })`.


# Reference: JSON Schema Validation Merging

We have to merge two JSON Schema documents. One provided by the user and one generated by MongoDB FLE 2 code.

The FLE 2 version.

<!-- TODO: Update ... -->
```js
{
    properties : {
        encryptedField : {
            bsonType : "string",
            ... 
        },
        nested : {
            bsonType: object
            otherEncryptedField : {
                bsonType : "string",
                ...
            },
        }
    }
}
```

And a user may have a hand written schema like:

<!-- TODO: Update ... -->

```js
{
    properties : {
        publicField : {
            bsonType : "string",
            ...
        },
        nested : {
            bsonType: object
            otherPublicField : {
                bsonType : "string",
                ...
            },
        }
    }
}
```

```
Rules
U = User document
G = Generated documents
1. A field in G and U can have the same name if they are either both object or array
2. It is an error for a field to otherwise exist in both G and U
```

While this can be thought of as merging two arbitrary JSON documents, since the generated one is controlled by MongoDB, this is a simpler problem

```
Merge_Schema(U, G)
1. If U = null, return G
2. If G = null, return U
2. Merge_Base(U.children, G.children)

Merge_Base(U_list, G_list):
If U_list = null, return G_list
If G_list = null, return U_list

doc = {}
for f in U_list
    if f in G_list:
        g = G_list[f]
        if f.bsonType != object && f.bsonType != array then
            throw error
        else
            Merge_Base(f.children, g.children)
        G_list.remove(g)
    else
        doc.add(f)

for g in G_list:
    doc.add(g)

return doc
```

