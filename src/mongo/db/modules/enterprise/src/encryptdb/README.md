# Encrypted Storage Engine

## Table of Contents

- [High Level Overview](#high-level-overview)
  - [Symmetric Cryptography](#symmetric-cryptography)
    - [Cipher Modes](#cipher-modes)
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
[\_\_wt_encrypt](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/support/crypto.c#L70)
and
[\_\_wt_bt_write](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/btree/bt_io.c#L298)).
Similarly, all queries that perform a read on an encrypted database will decrypt the data
immediately after reading a page from disk (see
[\_\_wt_decrypt](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/support/crypto.c#L17)
and
[\_\_wt_bt_read](https://www.github.com/mongodb/mongo/tree/master/src/third_party/wiredtiger/src/btree/bt_io.c#L62)).

### Symmetric Cryptography

MongoDB uses the [Advanced Encryption Standard (AES)](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard) to
perform symmetric cryptography in the encrypted storage engine. AES uses
[block ciphers](https://en.wikipedia.org/wiki/Block_cipher) with a symmetric key. This means that AES will encrypt
and decrypt data in small chunks, called blocks. AES _always_ uses 128-bit blocks. AES has multiple steps of encryption,
which involve generating new keys and performing transformations on the block of data being encrypted. The operations
of these steps are defined by the [cipher mode.](#cipher-modes)
There are three types of AES that all use different-length symmetric secret keys:

- AES-128: 128-bit keys
- AES-192: 192-bit keys
- AES-256: 256-bit keys

The secret key is used for _both_ encryption and decryption. If one were to look at AES as a black box, it
receives a symmetric secret key, a plain text buffer, and sometimes an Initialization Vector (IV) as input, and outputs
an encrypted ciphertext. The secret key should be generated with a random key generator tool, although we leave key
generation up to our users. We do generate the IV, which is non-deterministic. The IV functions in a similar way as a
password salt, in that it adds more layers of entropy to the final ciphertext. The cipher text is deterministic based
off of the IV, meaning that if the same plain text is encrypted with the same key and the same IV, the ciphertext will
always be the same. This also means that cipher modes that don't use IVs will always have a one-to-one correspondence
between plain text and ciphertext. We don't use these cipher modes, and we never reuse IVs, so our ciphertexts will
always be different.

#### Cipher Modes

AES always uses a block cipher to encrypt data, but there are many unique cipher modes available to it. A cipher mode
describes how data is moved and transformed between each step of AES. Of all of these cipher modes, the Encrypted
Storage Engine only uses two of them: [CBC](#cbc-cipher-block-chaining) and [GCM](#gcm-galoiscounter-mode). Those modes
are described below, as well as two others that predicate CBC and GCM in order to provide background information on
their implementation and design.

##### ECB (Electronic Code Book)

ECB is the simplest cipher mode available to AES. ECB divides the plain text input into even blocks of 128 bits, and
will pad the last block until it fits evenly. Every block is encrypted independently with the same key and the same
algorithm. Because of this, blocks in ECB do not depend on each other, so they can be encrypted and decrypted in
parallel, and the ciphertext is resistent to complete corruption. ECB also does _not_ take an IV as input. ECB is much
easier to reverse-engineer since it has no elements of non-determinism, and leaks lots of information into the
ciphertext. As such, **_ECB is not used by the Encrypted Storage Engine._**

##### CBC (Cipher Block Chaining)

CBC Adds non-determinism to the mix by use of an IV. The IV is generated randomly using
[`aesGenerateIV`](https://github.com/10gen/mongo-enterprise-modules/blob/v4.4/src/encryptdb/symmetric_crypto.h#L24),
which, for CBC mode, just fills a block-sized buffer (128 bits, or 16 bytes) with random bytes. Like ECB, data is
divided into 128-bit blocks and is padded to fill all blocks evenly. CBC Will then execute the following steps for
encryption:

1. XOR the first plain text block with the IV
2. The result of step 1 is then encrypted into a new block
3. That new ciphertext block is then XOR'd against the next plain text block
4. The result is then encrypted
5. Repeat steps 3-4 until all plain text blocks have been encrypted

CBC is the default cipher mode for the Encrypted Storage Engine.

##### CTR (Counter)

CTR functions by turning a block cipher into a [stream cipher](https://en.wikipedia.org/wiki/Stream_cipher). It
generates the next keystream block by incrementing a counter and encrypting that value. The counter can be any function
that produces a sequence of numbers that is guaranteed to not repeat for a very long time. This means that the counter
has to be deterministic. Encryption on each block can be performed in parallel, as they are independent. The steps for
encryption on each block are as follows:

1. XOR the IV with the current value of the counter
2. Encrypt the result from step 1 with the secret key
3. XOR the result from step 2 with the plain text input to produce the ciphertext

**_CTR mode is not used by the Encrypted Strage Engine_**.

##### GCM (Galois/Counter Mode)

GCM uses CTR to produce a ciphertext for each block, and then embeds cryptographically secure authentication data into
the ciphertext, which hardens the data against tampering. The counter function that ESE uses for GCM is a simple
incremental counter. It is 96 bits long, divided into a 64-bit integer and a 32-bit integer. The 64-bit integer is
incremented every time an encrypted write is performed, and the 32-bit integer is incremented every time the server is
booted. We chose this model of counter for the following reasons:

- The counter has to be deterministic and non-repeating (at least for an insurmountable amount of time)
- In order to avoid repeated use of IVs in the event of sudden shutdown, the counter has to be "persisted" in some way
- The counter has to exist in memory, as writing it to disk for every encrypted write would be very slow

**_GCM is only available on Linux, since OpenSSL is the only crypto library we use that implements it._**

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

Keys stored in a KMIP speaking server have a
[concept of state](https://docs.oasis-open.org/kmip/kmip-spec/v2.0/os/kmip-spec-v2.0-os.html#_Toc6497510)
that determines whether the key is in use. When a key is created, it is put into the pre-active
state. It must first be put into the active state before it can be used. If a key is not specified
when starting an Encrypted mongod that is using KMIP, the node will reach out to KMIP and create a
key. The node will then activate the key. To ensure that the ESE is not using an inactive key, the
node runs a [periodic job](src/kmip/../../../kmip/kmip_key_active_periodic_job.h), constantly
polling the KMIP server to get information on the key state to ensure that the key is active while
the node is alive. If the KMIP server is down, the polling job will log a warning. If the state of
the key is not active, the mongo node will shut down. When a key is being rotated out, the state of
this key will not be checked. However the state of the new key will be checked.
