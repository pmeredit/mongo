# Field Level Encryption

## Table of Contents

- [High Level Overview](#high-level-overview)
  - [FLE1](#fle1)
  - [FLE2](#fle2)
- [Shell Hooks](#shell-hooks)
- [MongoCryptD](#mongocryptd)

## High Level Overview

**Field Level Encryption (FLE)** is a feature that allows for client-side encryption of particular fields in
MongoDB collections. The client decides what is encrypted, and the driver will automatically encrypt commands and
decrypt results. This adds another layer of security to the stored data if the encryption of the network or storage
engine has been compromised. This also allows encryption to be done client side, meaning the server will
never be able to decrypt the information without access to the master keys. There are two types of field level encryption:
[_manual_](https://docs.mongodb.com/manual/core/security-explicit-client-side-encryption/) and
[_automatic_](https://docs.mongodb.com/manual/core/security-automatic-client-side-encryption/).
Automatic field level encryption is an _enterprise-only feature_. Both types of Field Level Encryption use the
same algorithms.

Manual FLE requires the application author to write encryption and decryption logic for specific fields using provided
functions in the driver or shell.

Automatic FLE relies on a daemon process called **`mongocryptd`** which is spawned by the
driver, or on **`Mongo Crypt Shared`** library which is dynamically loaded by the driver.
The client will send commands to `mongocryptd` or `Mongo Crypt Shared` library,
which marks appropriate BSON fields to be encrypted, and then
the driver encrypts them using [libmongocrypt](https://github.com/mongodb/libmongocrypt). When receiving encrypted data
back from the server, the driver will retrieve a key from the server's key vault automatically for the appropriate
fields and will use that key to decrypt them.

The shell can perform manual and automatic FLE as well. Manual FLE is performed by calling API functions via a
[`ClientEncryption`](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/keyvault.js#L121) object, which can be
acquired with [`getClientEncryption`](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/keyvault.js#L135). A
shell performs automatic encryption by creating a `Mongo` object configured with appropriate schema encryption rules to
connect to the database. For an example of this, see
[here](https://docs.mongodb.com/manual/reference/method/Mongo/#mongo-connection-automatic-client-side-encryption-enabled).
Only the enterprise shell is able to perform automatic encryption.

The shell is able to automatically decrypt encrypted fields by analyzing the `BinData` blob stored as the field's value
and extracting the information about the key and the algorithm used to encrypt the field. It will then retrieve the
encrypted key from the key vault, and use a different key from KMS to decrypt the database key, which is then used to
decrypt the body of the `BinData` object using the appropriate algorithm.

Starting with version 6.0 there are two versions of FLE supported - classic FLE (or FLE1) and Queryable Encryption,
also known as FLE2.

### FLE1

FLE1 is using two variants of `AEAD_AES_256_CBC_HMAC_SHA_512` algorithms:

- `AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic`
- `AEAD_AES_256_CBC_HMAC_SHA_512-Random`

Deterministic algorithm encrypts same plaintext value to the same ciphertext for all the values of field in a collection,
which allows indexing. Random variant of the algorithm always produces different ciphertext value for the given plaintext,
preventing statistical attack, but making indexing impossible. Effectively the user must make a choice between security
and usability.

### FLE2

FLE2 features both randomized data storage and data indexing simultaneously. Such functionality is achieved by using
additional structures (making it sometimed go by the name "Structured Encryption"), which allows the server to locate
required records without decrypting data being searched for. These structures are:

- `EDC` - Encrypted Data Collection (i.e. the user collection)
- `ECC` - Encrypted Cache Collection
- `ECOC` - Encrypted Compaction Collection
- `ESC` - Encrypted State Collection

In contrast to FLE1, the server is actively participating in reading, creating, and updating data structures
during CRUD operations. Client will submit necessary query parts to the server inside a
[payload](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/fle_field_schema.idl#L141-L211),
and server will use the elements of that payload to perform an operation on encrypted structures.

FLE2 is using following algorithms to encrypt data:

- `HMAC-SHA-256` as a MAC function
- `AES-256-CTR` for Encrypt/Decrypt. It does not provide integrity, only confidentiality.
- `AES-256-CTR-HMAC-SHA-256` for EncryptAEAD/DecryptAEAD for both confidentiality and integrity.

In addition to structutrally encrypted data, FLE2 also allows data to be encrypted as unindexed. This reduces the
load on server and only records encrypted values in `EDC`. FLE2 uses `AES-256-CTR-HMAC-SHA-256` algorithm
to secure unindexed data, which produces security and performance levels comparable to
`AEAD_AES_256_CBC_HMAC_SHA_512-Random` algorithm.

For details on FLE2 encryption, see [Queryable Encryption (FLE2) Protocol](../../docs/fle/fle_protocol.md) guide. Also,
following sources are helpful in understanding data and class structure:

- [fle_field_schema.idl](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/fle_field_schema.idl)
- [crypto primitives](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto)
- [db/fle_crud.h](https://github.com/mongodb/mongo/blob/master/src/mongo/db/fle_crud.h)
- [db/fle2_compact.h](https://github.com/mongodb/mongo/blob/master/src/mongo/db/commands/fle2_compact.h)
- [db/expression_type.h](https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_type.h)
- [db/server_rewrite.h](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/fle/server_rewrite.h)
- [shell/encrypted_dbclient_base.h](https://github.com/mongodb/mongo/blob/master/src/mongo/shell/encrypted_dbclient_base.h)
- [query_analysis](./query_analysis)

## Shell Hooks

The shell implements automatic client-side encryption by allowing the enterprise module to inject encryption logic into
the shell as a "reverse dependency." We do this because we provide a community version of the shell, but we can't ship
the enterprise-only FLE code with it. We implement manual encryption by injecting encryption logic into every
database connection object created by the shell.

Internally, **automatic FLE** is referred to as **implicit encryption**. There is a function pointer owned by the shell
called [`implicitEncryptedDBClientCallback`](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/encrypted_dbclient_base.cpp#L68)
This pointer [is set](https://github.com/10gen/mongo-enterprise-modules/blob/v4.4/src/fle/shell/implicit_encrypted_dbclient.cpp#L355)
when the shell is initialized. This only occurs in the enterprise-only FLE code, and the pointer is otherwise null. The
callback is called in [`createEncryptedDBClientBase`](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/encrypted_dbclient_base.cpp#L690),
which returns a `std::unique_ptr<ImplicitEncryptedDBClientBase>`.
[`ImplicitEncryptedDBClientBase`](https://github.com/10gen/mongo-enterprise-modules/blob/v4.4/src/fle/shell/implicit_encrypted_dbclient.cpp#L40)
implements `DBClientBase`, which represents a client connection to a database. This class calls the same
query analysis hooks as `mongocryptd` and handles calling appropriate functions that perform the encryption
and decryption of commands.

The shell owns another function pointer called
[`encryptedDBClientCallback`](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/scripting/mozjs/mongo.cpp#L96) which
is the equivalent callback for **manual FLE**. The pointer is also
[set](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/scripting/mozjs/mongo.h#L42) when the shell is initialized,
but it is [called](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/scripting/mozjs/mongo.cpp#L843) by the shell
[every time a new connection object is created](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/scripting/mozjs/mongo.cpp#L818)
by the user. When a user wants to manually encrypt something in the shell, they call `Mongo.encrypt()`, which is
implemented [here](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/encrypted_dbclient_base.cpp#L322). When
a shell user wants to decrypt a field, they call `Mongo.decrypt()`, which is implemented
[here](https://github.com/mongodb/mongo/blob/v4.4/src/mongo/shell/encrypted_dbclient_base.cpp#L464). Manual encryption
is able to encrypt with the same algorithms as automatic.

## MongoCryptD

`mongocryptd` is a separate process and program that is required for _automatic FLE_ to work. `mongocryptd` is spawned
by clients that have automatic FLE enabled, and then listens for incoming connections from localhost. `mongocryptd`
speaks MongoDB wire protocol. `mongocryptd` and the shell use the same query analysis code, but the shell does not
include all of the code that allows `mongocryptd` to function as its own process. This allows the shell to do everything
that `mongocryptd` can do without using a separate process as a passthrough for encryption. `mongocryptd` is responsible
for the following:

- Parses the
  [automatic encryption rules](https://docs.mongodb.com/manual/reference/security-client-side-automatic-json-schema/#field-level-encryption-json-schema)
  specified to the database connection. Automatic encryption rules use a strict subset of JSON schema syntax. If the
  automatic encryption rules contains invalid automatic encryption syntax or any document validation syntax,
  `mongocryptd` returns an error.
- Uses the specified automatic encryption rules to mark fields in read and write operations for encryption.
- Reject read/write operations that may return unexpected or incorrect results when applied to an encrypted field.
  See [Read/Write Support with Automatic Field Level Encryption](https://docs.mongodb.com/manual/reference/security-client-side-query-aggregation-support/)
  for more information.

Notice that `mongocryptd` is _not responsible for performing encryption/decryption_. For more information on `mongocryptd`,
see our [documentation](https://docs.mongodb.com/manual/reference/security-client-side-encryption-appendix/#mongocryptd).

`mongocryptd` is a small program made up of several parts. The skeleton of `mongocryptd` is all located in
[mongo/util/cryptd](https://github.com/mongodb/mongo/blob/master/src/mongo/util/cryptd). It includes a ["main"](https://github.com/mongodb/mongo/blob/master/src/mongo/util/cryptd/cryptd_main.cpp) file, files for
[handling command-line options](https://github.com/mongodb/mongo/blob/master/src/mongo/util/cryptd/cryptd_options.h), a file that defines a
[service entry-point](https://github.com/mongodb/mongo/blob/master/src/mongo/util/cryptd/cryptd_service_entry_point.h), and a [watchdog](https://github.com/mongodb/mongo/blob/master/src/mongo/util/cryptd/cryptd_watchdog.h) that is in
charge of safely shutting down `mongocryptd` if it has been idle for a long time.

All of the commands that `mongocryptd` knows how to handle are implemented in [fle/commands](./commands).

The heftiest part of `mongocryptd` is the query analysis pipeline, located in [fle/query_analysis](./query_analysis).
Queries are processed by the pipeline via `mongocryptd` as well as hooks in
[ImplicitEncryptedDBClientBase](./shell/implicit_encrypted_dbclient.cpp), denoted by the use of namespace `query_analysis`.

## Mongo Crypt Shared

`Mongo Crypt Shared` library is an alternative to `mongocryptd`, which provides the same functionality. Like `mongocryptd`, it parses the
[automatic encryption rules](https://docs.mongodb.com/manual/reference/security-client-side-automatic-json-schema/#field-level-encryption-json-schema)
specified to the database connection and automatically applies these rules to read and write operations. Unlike
`mongocryptd` a running daemon is not required for `Mongo Crypt Shared`, which is implemented as a dynamically loadable library
and runs in the same memory space as the client process.

In order to use `Mongo Crypt Shared`, the user application must ship the library for the desired platform together with application binary.
Drivers would automatically load the library and use it.

Code implementing `Mongo Crypt Shared` is located in [fle/lib](./lib). It wraps [fle/query_analysis](./query_analysis) component
used by `mongocryptd` and will therefore functionally operate in the same manner as `mongocryptd` does. Also see
[mongo_crypt.h](../src/fle/lib/mongo_crypt.h) for interface declarations.
