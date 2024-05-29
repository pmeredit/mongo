# Auditing

## Table of Contents

- [High Level Overview](#high-level-overview)
  - [Runtime Configuration](#runtime-configuration)
  - [AuditInterface](#auditinterface)
- [Encrypted Audit Logs](#encrypted-audit-logs)

## High Level Overview

Auditing is a system to track and log system activity for mongod and mongos deployments. It is
designed for administrators to monitor actions performed by by different users in the database.
Audit messages include information regarding any creation or changes for users, databases, roles,
and indexes. The full list of events can be found
[here](https://docs.mongodb.com/manual/reference/audit-message/#audit-action-details-results).
Auditing writes messages to the audit log, which can be rendered to console output, the syslog, a
JSON file, or a BSON file. More information about Auditing in general can be found
[here](https://docs.mongodb.com/manual/core/auditing/). Auditing can be enabled by starting the
database through [initialization
parameters](https://docs.mongodb.com/manual/tutorial/configure-auditing/#enable-and-configure-audit-output).

Auditing works internally through a series of functions, known as auditing hooks. These functions
live [here](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/audit.cpp). Each function is
called when an operation corresponding to the hook is run. For example, for a call to
[`CreateUser`](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/commands/user_management_commands.cpp#L765),
the invocation of the audit hook appears
[here](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/commands/user_management_commands.cpp#L869-L874).
The auditing hooks are no-ops in the community module; they are overridden through the enterprise
module
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_user_management.cpp).

The audit guarantee is that events from a single connection have total order, meaning that if an
audit event gets written, all events prior that need to be logged have been written. More
information about the audit guarantee from the user perspective can be found
[here](https://docs.mongodb.com/manual/core/auditing/#audit-guarantee). The audit guarantee is what
causes auditing to be slow. After every audit event, the event is written out to the audit log. When
performing the write to disk, the audit logger takes a mutex and writes out a single event. Because
this serializes individual writes, the audit logger is not very performant. The relevant code is
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_log.cpp#L91-107).
The audit logger creates an instance of `RotatableFileWriter::Use` which takes a mutex. Every audit
hook calls this function, making every individual write to the logger serial.

The audit filter is a piece of information that determines what events to log to the audit log. By
default, this is set at startup, but if runtime configuration is enabled, it will be set at runtime
(see [Runtime Configuration](#runtime-configuration)). The audit filter is a query expression which
matches the items that need to be audited. For examples of inputs, see
[here](https://docs.mongodb.com/manual/tutorial/configure-audit-filters/#examples). There is a
[GlobalAuditManager](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_manager_global.cpp#L18-L20)
that is initialized when auditing is started in the database. In the GlobalAuditManager, there is a
member called the audit filter. In every call to log an audit event, the match expression is checked
to see if the event being logged matches an item in the filter. An example for logCreateUser can be
found
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_user_management.cpp#L164-L172).
If the event being logged does not match the audit filter, the event does not get logged.

Audit messages for CRUD operations are emitted by the command dispatch layer if
`auditAuthorizationSuccess` is enabled or the authorization check for the command failed. This
ensures that the audit log is not flooded with messages. Unlike the audit filter,
`auditAuthorizationSuccess` can be altered (via a setParameter) at runtime even when runtime
configuration is disabled. If runtime configuration is enabled, this flag is a component of the
runtime audit configuration and cannot be modified directly with the setParameter (see
[Runtime Configuration](#runtime-configuration) for more details). Logging of audit events is done
in
[commands.cpp](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/commands.cpp#L747-L778). At
the bottom of that function, there is a call to `auditLogAuthEvent` which calls into
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_authz_check.cpp#L96-L129),
which in turn calls `_logAuthzCheck`.

### Runtime Configuration

When the `auditRuntimeConfiguration` flag is enabled, the audit configuration will be mutable at
runtime. This is facilitated through the `auditConfig` cluster server parameter. The schema of this
parameter is `{filter: <BSONObj>, auditAuthorizationSuccess: <bool>}`. Setting this parameter will
take immediate effect on all nodes in the cluster. Because of how cluster parameters work,
`mongos`'s will not receive the new audit config immediately, but will fetch it periodically when
the cluster server parameter refresher is run. This job is run every 30 seconds by default. See
[Cluster Server Parameters](https://github.com/mongodb/mongo/blob/master/docs/server-parameters.md#cluster-server-parameters)
for more details.

When FCV<=7.0, the runtime configuration works slightly differently. Specifically, rather than being
stored in a cluster server parameter, the runtime audit configuration is stored in the
`config.settings` collection, and is interacted with through the `setAuditConfig` and
`getAuditConfig` commands (these are deprecated in FCV>=7.1). Also, in FCV<=7.0, the audit
configuration has a `generation` field, which is an OID that is changed upon runs of
`setAuditConfig`. In FCV>=7.1, the `clusterParameterTime` field attached to all cluster parameters
has the same functionality. Finally, it's important to note that upon downgrading from FCV>=7.1 to
FCV<=7.0, if the audit configuration is set, the downgrade will be blocked until the audit
configuration is removed. Conversely, when upgrading from FCV<=7.0 to FCV>=7.1, if there is an
existing audit config, it will be migrated from `config.settings` to the `auditConfig` cluster
parameter.

### AuditInterface

Callsites within MongoDB invoke auditing via free functions on the `mongo::audit` namespace
as listed in [`audit.h`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/audit.h).
These free functions are thin wrappers which call matching methods on an instance
of [`AuditInterface`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/audit_interface.h)
which they obtain from a decoration on the `ServiceContext`.

In community builds of MongoDB, the only implementation of `AuditInterface` available is
[`AuditNoop`](https://github.com/mongodb/mongo/blob/b05c88308318b6ec3f8a383d13e61578c4625549/src/mongo/db/audit_interface.h#L473).
This implementation performs no actions and returns immediately from any attempt to record an audit event.

Enterprise builds may select between
[`AuditMongo`](https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/src/audit/mongo/README.md) or
[`AuditOCSF`](https://github.com/10gen/mongo/blob/master/src/mongo/db/modules/enterprise/src/audit/ocsf/README.md)
implemenations based on the `auditLog.schema` setting.

## Encrypted Audit Logs

If the audit destination is a file, the audit manager can be configured to encrypt
each log entry using AES-256-GCM and an ephemeral key generated during process startup. This ephemeral
_log encryption key_ is, in turn, encrypted with another _key encryption key_ that is provided externally,
either through KMIP or a local key file. In addition, the audit manager can be configured to
compress each log entry using the zstd algorithm prior to encrypting. Metadata about the compression
and encryption, key acquisition, and the encrypted log encryption key itself, are written at the start
of the audit log file as its header.

The compression and encryption of individual audit lines is performed by an
[`AuditEncryptionCompressionManager`](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/audit/audit_enc_comp_manager.h)
object, which is owned by the global
[`AuditManager`](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/audit/audit_manager.h#L195-L196)
object. If encryption is enabled, each audit event is encrypted
[here](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/audit/audit_log.cpp#L153-L172),
before it is written to the audit log file.

The `AuditEncryptionCompressionManager` depends on an `AuditKeyManager` object that generates the
log encryption key, and encrypts the log encryption key with the key encryption key, producing the
"wrapped" key that will be included in the log file header. Acquisition of the key encryption key
from the external source (e.g. KMIP server), and the subsequent wrapping of the log encryption key,
must happen during process startup. In particular, it must happen before the audit log is rotated,
because that is when the header line is written.

The `AuditEncryptionCompressionManager` is created and initialized lazily when first acquired from
the global `AuditManager` object through the
[`getAuditEncryptionCompressionManager()`](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/audit/audit_manager.cpp#L263-L288)
method. This method also initializes the `AuditKeyManager` instance that the encryption manager
depends on. This method cannot be called on an initializer, or before the global `ServiceContext`
has been initialized, because some `AuditKeyManager` implementations like `AuditKeyManagerKMIPGet`
or `AuditKeyManagerKMIPEncrypt` depend on the
[`KMIPService`](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/kmip/kmip_service.cpp#L33)
that decorates the global `ServiceContext`. When mongod or mongos starts up, the first call to
`getAuditEncryptionCompressionManager()` happens during audit log rotation
[here](https://github.com/10gen/mongo-enterprise-modules/blob/afa9120b0f3e2cf54e08301ef7c9e9ddbeb2e36a/src/audit/audit_log.cpp#L72).
Audit log rotation itself is initiated
[here](https://github.com/mongodb/mongo/blob/36468ab03f8a27498ce54862aadf5cc17957159a/src/mongo/db/mongod_main.cpp#L1472),
well after the global `ServiceContext` is initialized.

There are currently three implementations of the `AuditKeyManager`: `AuditKeyManagerLocal`,
`AuditKeyManagerKMIPGet`, and `AuditKeyManagerKMIPEncrypt`. `AuditKeyManagerLocal` retrieves the
key encryption key from a file containing a base64-encoded 256-bit key, and uses AES-256-CBC to
wrap the log encryption key. `AuditKeyManagerKMIPGet` retrieves the key from the KMIP server via
a Get request, and uses AES-256-GCM for key wrapping. `AuditKeyManagerKMIPEncrypt` does not pull
a key encryption key from the KMIP server, instead it sends the server the log encryption key in
an Encrypt request, and uses the returned encrypted key in the response as the wrapped key.
