# Audit Schema: mongo

The default `auditSchema` in MongoDB is `mongo` which may be encoded
as `JSON` or `BSON` and follows the general format outlined below.

## General Schema

A `mongo` audit log is made up of a series of BSON or JSON objects with the following fields:

| FieldName | FieldType         | Example                                                                 | Description                                                         | Notes                                                                                            |
| --------- | ----------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `atype`   | `AuditType`       | `"authenticate"`                                                        | Type of action being audited.                                       |                                                                                                  |
| `ts`      | `Date_t`          | `UTCDateTime("2024-05-21T14:10:23Z")`                                   | When the audited action took place.                                 |                                                                                                  |
| `tenant`  | `TenantId`        | `ObjectId("deadbeefcafeba5eba11f00f")`                                  | Tenant to which this action is scoped.                              | [Multitenancy](#multitenancy)                                                                    |
| `local`   | `Endpoint`        | `{ ip: "172.31.55.66", port: 27017 }`                                   | Local (server) endpoint of the active connection.                   | [Endpoint](#endpoint)                                                                            |
| `remote`  | `Endpoint`        | `{ ip: "10.11.12.13", port: 56071 }`                                    | Remote (client) endpoint of the active connection.                  | [Endpoint](#endpoint)                                                                            |
| `users`   | `array<UserName>` | `[ { user: "alice", db: "test" } ]`                                     | UserName of authenticated client originating the event.             | [Authentication](#authentication)                                                                |
| `roles`   | `array<RoleName>` | `[ { role: "readwrite", db: "test" }, { role: "read", db: "payroll"} ]` | Zero or many RoleNames held by the originating user.                | [Authentication](#authentication)                                                                |
| `params`  | `object`          | `{ msg: "Hello World" }`                                                | Parameters specific to the `atype` describing details of the event. | [Params](#params)                                                                                |
| `result`  | `ErrorCode`       | `0`                                                                     | Integer representation of the ErrorCode resulting from this action. | [`error_codes.yml`](https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml) |

### Multitenancy

The `tenant` field is only present when mutlitenancy is enabled, and the action actually took place on a tenant-specific resource.

### Endpoint

`local` and `remote` describe the server `local` and client `remote` endpoints involved in the action.
The contexts on these fields may vary depending on the type on connection.

| Connection Type  | Format                                    | Example                              | Description                                                          |
| ---------------- | ----------------------------------------- | ------------------------------------ | -------------------------------------------------------------------- |
| Internal         | `{ isSystemUser: true }`                  | `{ isSystemUser: true }`             | `DBDirectClient` or similar internal action initiated by the server. |
| IP               | `{ ip: <dottedQuadOrIPv6>, port: <int> }` | `{ ip: "192.168.0.1", port: 27017 }` | Standard connection via TCP/IP.                                      |
| Unix (named)     | `{ unix: <socketPath> }`                  | `{ unix: "/var/run/mongod.sock" }`   | Localhost connection via Unix domain socket.                         |
| Unix (anonymous) | `{ unix: "anonymous" }`                   | `{ unix: "anonymous" }`              | Localhost connection via anonymous Unix domain socket.               |

### Authentication

Although the `users` field is an array type, and pluralized, authentication in the server has been restricted to a single user at a time since MongoDB 5.0,
so this field will never contain more than one user.

The `roles` field may contain any number of entries based on the user (if present)'s role membership.

### Params

Current implementation should be referenced for detail descriptions of audit log formats,
however a few common auditTypes are covered here for convenience.

#### AType: authenticate

| FieldName   | FieldType | Example                         | Description                                                                                             | Notes                                                                   |
| ----------- | --------- | ------------------------------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `user`      | `string`  | `"alice"`                       | The symbolic name of the user being authenticated.                                                      |                                                                         |
| `db`        | `string`  | `"test"`                        | The database name (without tenant) of the user being authenticated.                                     | Any tenant information can be determined from the outer event envelope. |
| `mechanism` | `string`  | `"SCRAM-SHA-256"`               | The authentication mechanism used.                                                                      |                                                                         |
| ...         | ...       | `awsId: "....", awsArn: "...."` | Additional fields on a per-mechanism basis. See each mechanism's implementation of `appendExtraInfo()`. |

Example:

```
{ atype: "authenticate",
  ts: { $ts : "2024-05-21T14:10:23Z" },
  local: { ip: "172.31.55.66", port: 27017 },
  remote: { ip: "10.11.12.13", port: 56071 },
  params: {
    user: "eve",
    db: "test",
    mechanism: "SCRAM-SHA-1",
  },
  result: 13,  // ErrorCodes::Unauthorized, e.g. Authentication Failed
}
```

#### Atype: createUser

| FieldName                    | FieldType            | Example                                                                       | Description                                                      | Notes                                                                   |
| ---------------------------- | -------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `user`                       | `string`             | `"bob"`                                                                       | The symbolic name of the user being created.                     |                                                                         |
| `db`                         | `string`             | `"sales"`                                                                     | The database name (without tenant) of the user being created.    | Any tenant information can be determined from the outer event envelope. |
| `customData`                 | `object`             | `{ employeeId: 12345 }`                                                       | Arbitrary property bucket defined by the database administrator. | Omitted when empty.                                                     |
| `roles`                      | `array<RoleName>`    | `[ { role: "readwrite", db: "sales" }, { role: "read", db: "engineering" } ]` | Roles granted to the new user.                                   |                                                                         |
| `authenticationRestrictions` | `array<Restriction>` | `[ [ { clientSource: "10.0.0.0/8" , serverAddress: "172.16.0.0/12" } ] ]`     | Restrictions on how this user can connect, if any.               | Omitted when empty.                                                     |

Example:

```
{ atype: "createUser",
  ts: { $ts : "2024-05-21T14:10:23Z" },
  local: { ip: "172.31.55.66", port: 27017 },
  remote: { ip: "10.11.12.13", port: 56071 },
  params: {
    user: "bob",
    db: "sales",
    roles: [ { role: "readwrite", db: "sales" }, { role: "read", db: "engineering" } ],
  },
  result: 0,
}
```

#### AType: applicationMessage

| FieldName | FieldType | Example         | Description                                                |
| --------- | --------- | --------------- | ---------------------------------------------------------- |
| `msg`     | `string`  | `"Hello World"` | Message as presented in the `{logMessage: <msg>}` command. |

Example:

```
{ atype: "applicationMessage",
  ts: { $ts : "2024-05-21T14:10:23Z" },
  local: { ip: "172.31.55.66", port: 27017 },
  remote: { ip: "10.11.12.13", port: 56071 },
  params: {
    msg: "Hello World",
  },
  result: 0, // ErrorCodes::OK, this audit entry never fails.
}
```
