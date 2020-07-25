# Auditing

## Table of Contents

- [High Level Overview](#high-level-overview)

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

The audit filter is a database startup specification that determines what events to log to the audit
log. The audit filter is a query expression which matches the items that need to be audited. For
examples of inputs, see
[here](https://docs.mongodb.com/manual/tutorial/configure-audit-filters/#examples). There is a
[GlobalAuditManager](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_manager_global.cpp#L18-L20)
that is initialized when auditing is started in the database. In the GlobalAuditManager, there is a
member called the audit filter that is set on startup. In every call to log an audit event, the
match expression is checked to see if the event being logged matches an item in the filter. An
example for logCreateUser can be found
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_user_management.cpp#L164-L172).
If the event being logged does not match the audit filter, the event does not get logged.

Audit messages for CRUD operations live in the command dispatch layer and only fire when the event
fails due to an authorization failure. This ensures that the audit log is not flooded with messages.
The calls to log the audit events for legacy operations are in the service entry point. For example,
the call to audit a delete operation is
[here](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/service_entry_point_common.cpp#L1566).
In the enterprise module, the function for `logDeleteAuthzCheck` calls the function
[`_shouldLogAuthzCheck`](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_authz_check.cpp#L61)
to see whether a call to delete should be logged. The function
[`_shouldLogAuthzCheck`](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_authz_check.cpp#L61)
logs the message only if the set parameter `auditAuthorizationSuccess` is enabled or the
authorization check for the delete failed. The call to log audit events for the majority of
operations, however, run through
[commands.cpp](https://github.com/mongodb/mongo/blob/r4.4.0/src/mongo/db/commands.cpp#L747-L778). At
the bottom of that function, there is a call to `auditLogAuthEvent` which calls into
[here](https://github.com/10gen/mongo-enterprise-modules/tree/r4.4.0/src/audit/audit_authz_check.cpp#L96-L129),
which follows the same pipeline as the legacy operations by calling `_logAuthzCheck`.
