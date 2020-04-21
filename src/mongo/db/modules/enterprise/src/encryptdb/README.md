# Encrypted Storage Engine

## Table of Contents

- [High Level Overview](#high-level-overview)
  - [Symmetric Cryptography](#symmetric-cryptography)
- [Page Format](#page-format)
- [Key Management](#key-management)

## High Level Overview

The Encrypted Storage Engine is an engine that encrypts and decrypts pages of data for the database.
The engine operates on a series of callback functions stored in a struct called WT_ENCRYPTOR that
lives in WiredTiger. These functions are called by WiredTiger when running a query. The functions
are as follows:

- `sizing`: returns the maximum padding an encryption operation might add to the plaintext data.
- `customize`: customizes a WT_ENCRYPTOR struct for a specific keyid with the correct key.
- `encrypt`: encrypts data.
- `decrypt`: decrypts data.
- `destroyEncryptor`: called when a WT_ENCRYPTOR is no longer used and its resources can be freed.

A struct called the ExtendedWTEncryptor wraps the WT_ENCRYPTOR. It contains extra metadata for
performing key retrieval and management. The struct is defined
[here](wiredtiger_encryption_callbacks.cpp).

If the server is started up with the encrypted storage engine, all queries that perform a write on
an encrypted database will call encrypt immediately before writing a page to disk (see
[__wt_encrypt](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/support/crypto.c#L70)
and
[__wt_bt_write](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/btree/bt_io.c#L298)).
Similarly, all queries that perform a read on an encrypted database will decrypt the data
immediately after reading a page from disk (see
[__wt_decrypt](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/support/crypto.c#L17)
and
[__wt_bt_read](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/btree/bt_io.c#L62)).

### Symmetric Cryptography

## Page Format

Data in Wired Tiger is stored in pages. When WiredTiger encrypts data, it encrypts a page together.
Each encrypted page has a format in which it is encrypted, which is composed of metadata and the
ciphertext. There are currently two versions of the page schema for encryption (k1 is only available
in GCM). Their differences are listed below.

- Page Schema
  - `k0`: Only one key is used for the entire database
  - `k1`: Each page selects its own key for encryption

Note that while each page selects the key for encryption independently, some pages will likely share
keys depending on the generation.

The metadata depends on the format of each page. The structure for each page format is listed below,
and the definitions of each format is listed [here](symmetric_crypto.h).

- `HeaderCBCV0`: [ IV (16 bytes) | ciphertext ]
- `HeaderGCMV0`: [ Tag (12 bytes) | IV (12 bytes) | ciphertext ]
- `HeaderGCMV1`: [ Tag (12 bytes) | Extra Data (13 bytes) | IV (12 bytes) | ciphertext ]

Note that `GCMV0` should only be used in databases before `v4.0`. Any database `v4.2` and greater
should use `GCMV1`.

The IV is used by the encryption algorithm to encrypt and decrypt the data. The Tag is only used in
GCM mode for authentication. The extra data is used in the k1 page schema for key selection.

The class [`EncryptedMemoryLayout`](symmetric_crypto.h) exists to reflect the division of
the encrypted page and provide convenient access to the ciphertext and the different parts of the
metadata. The encrypt function in the WiredTiger callback calls aesEncrypt
[here](wiredtiger_encryption_callbacks.cpp#L158). The aesEncrypt function then checks the schema and
encrypts the data using the [`EncryptedMemoryLayout`](symmetric_crypto.h). A similar
process is followed by the decrypt function using aesDecrypt.

The [`EncryptedDataProtector`](encrypted_data_protector.h) is a class that is called from a
callback when writing temporary files to disk. When rollback files are created, or when storage is
performing an External Sort and needs to be written to disk, the encryption hooks ensure that the
files are encrypted. The `EncryptedDataProtector` always uses CBC mode to encrypt, and therefore
`PageSchema::k0`, regardless of the cipher mode selected.

In the instance of a replication rollback, the database produces encrypted rollback files if it
using the Encrypted Storage Engine. A separate binary called `mongoDecrypt` exists as a standalone
tool to decrypt these rollback files.

## Key Management

The Encrypted Storage Engine uses two types of keys: database keys and master keys. Every mongod
instance _should_ use its own independent database and master keys. Database keys are used to
encrypt the data in the corresponding database. The database keys are written directly to disk in
the same repository that the database files are stored, under the folder called `key.store` and in a
file named after the keyname. This setup is known as the local keystore. Database keys can be rolled
over by starting a mongod with the flag `eseDatabaseKeyRollover`.

Master keys are used to encrypt database keys. The master keys are either retrieved directly from a
key management service called KMIP, or loaded from a file on disk called the keyfile. Master keys
can only be rotated by shutting down the mongod. When a master key is rotated, all the files
associated with the old keystore are replaced by new keystore files, and the data is re-encrypted
with new database keys.

The class [`EncryptionKeyManager`](encryption_key_manager.h) manages access to the keys and
is called from the WiredTiger callbacks. It [interacts with KMIP](encryption_key_manager.cpp)
to create and delete master keys, creates the local keystore, and manages the local database keys.
The [`EncryptionKeyManager`](encryption_key_manager.h) interacts with a class called
[`KeyStore`](keystore.h) to manage the local keystore.

The Encrypted Storage Engine uses a protocol called `KMIP` which stands for Key Management
Interoperability Protocol. The protocol defines a set of operations for interacting with a key
management service. ESE allows you to store your master key in an external key manager and use KMIP
to communicate with that key management service. All of the functions that ESE uses for KMIP are in
[`kmip_service.h`](kmip_service.h).
