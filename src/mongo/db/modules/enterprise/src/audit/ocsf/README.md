# Audit Schema: OCSF

## General Schema

An `OCSF` audit log is made up of a series of JSON objects conforming to the official [OCSF Schema](https://schema.ocsf.io/) for applicable categories.

Where possible, `mongo` schema fields are directly mapped to `OCSF` schema.

### AuditType

The `mongo` `AuditType` enum is mapped to OCSF ActivityIds roughly according to the following table:

| `atype`                            | `type_uid`                                                            | OCSF Category | OCSF Class          | OCSF Activity                        |
| ---------------------------------- | --------------------------------------------------------------------- | ------------- | ------------------- | ------------------------------------ |
| addShard                           | [500101](https://schema.ocsf.io/1.2.0/classes/inventory_info)         | Configuration | Device Config State | Log                                  |
| applicationMessage                 | [100799](https://schema.ocsf.io/1.2.0/classes/process_activity)       | System        | Process Activity    | Other                                |
| auditConfigure                     | [500201 or 500203](https://schema.ocsf.io/1.2.0/classes/config_state) | Discovery     | Device Config State | 1=Create, 3=Update                   |
| authzCheck                         | [600301 - 600304](https://schema.ocsf.io/1.2.0/classes/api_activity)  | Application   | API Activity        | 1=Create, 2=Read, 3=Update, 4=Delete |
| authenticate                       | [300201](https://schema.ocsf.io/1.2.0/classes/authentication)         | IAM           | Authentication      | Logon                                |
| clientMetadata                     | [400101](https://schema.ocsf.io/1.2.0/classes/network_activity)       | Network       | Network Activity    | Open                                 |
| createCollection                   | [300401](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Create                               |
| createDatabase                     | [300401](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Create                               |
| createIndex                        | [300401](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Create                               |
| createRole                         | [300101](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Create                               |
| createUser                         | [300101](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Create                               |
| directAuthMutation                 | [300100](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Unknown                              |
| dropAllRolesFromDatabase           | [300106](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Delete                               |
| dropAllUsersFromDatabase           | [300106](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Delete                               |
| dropCollection                     | [300404](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Delete                               |
| dropDatabase                       | [300404](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Delete                               |
| dropIndex                          | [300404](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Delete                               |
| dropPrivilegesToRole               | [300107](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Attach Policy                        |
| dropRole                           | [300106](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Delete                               |
| dropUser                           | [300106](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Delete                               |
| enableSharding                     | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| getClusterParameter                | [600302](https://schema.ocsf.io/1.2.0/classes/api_activity)           | Application   | API Activity        | Read                                 |
| grantRolesToRole                   | [300107](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Attach Policy                        |
| grantRolesToUser                   | [300107](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Attach Policy                        |
| importCollection                   | [300401](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Create                               |
| logout                             | [300202](https://schema.ocsf.io/1.2.0/classes/authentication)         | IAM           | Authentication      | Logoff                               |
| refineCollectionShardKey           | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| removeShard                        | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| renameCollection                   | [300403](https://schema.ocsf.io/1.2.0/classes/entity_management)      | IAM           | Entity Management   | Update                               |
| replSetReconfig                    | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| revokePrivilegesFromRole           | [300108](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Detach Policy                        |
| revokeRolesFromRole                | [300108](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Detach Policy                        |
| revokeRolesFromUser                | [300108](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Detach Policy                        |
| rotateLog                          | [100799](https://schema.ocsf.io/1.2.0/classes/process_activity)       | System        | Process             | Other                                |
| setClusterParameter                | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| shardCollection                    | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| shutdown                           | [100702](https://schema.ocsf.io/1.2.0/classes/process_activity)       | System        | Process             | Terminate                            |
| startup                            | [100701](https://schema.ocsf.io/1.2.0/classes/process_activity)       | System        | Process             | Launch                               |
| updateCachedClusterServerParameter | [500201](https://schema.ocsf.io/1.2.0/classes/config_state)           | Configuration | Device Config State | Log                                  |
| updateRole                         | [300199](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Other                                |
| updateUser                         | [300199](https://schema.ocsf.io/1.2.0/classes/account_change)         | IAM           | Account Change      | Other                                |

Note: OCSF defines `type_uid` as a combindation of (`class_id` \* 100) + (`activity_id`), with `category_id` being the thousands place in a `class_id`.

### Network Endpoint

The OCSF Base Class fields `dst_endpoint` and `src_endpoint` are auto-serialized similar to the `local` and `remote` properties in a `mongo` schema log entry.
When connections represent ip:port combinations, the mapping is essentially identical:

```
{ src_endpoint: { ip: "192.168.123.45", port: 61060 }
  dst_endpoint: { ip: "172.31.0.1", port: 27017 } }
```

However, a Unix endpoint is remapped as using the "unix" `interface`, with the path entered in the `ip` field.

```
{ src_endpoint: { interface: "unix", ip: "anonumous" },
  dst_endpoint: { interface: "unix", ip: "/var/run/mongod.sock" } }
```

### Actor

The `users` field in the `mongo` schema is remapped to the `actor` field using unambiguous name formatting for the user.db format.

```
{
  actor: {
    type_id: 0, // Regular User
    name: "dbname.username",
  }
}
```

### Unmapped Fields

Any field normally captured by a mongo audit log entry which has no direct equivalent in OCSF is stored in the `unmapped` property.

```
{
  type_uid: 100799,
  category_id: 1,
  class_id: 1007,
  activity_id: 99,
  unmapped: {
    msg: "Hello World",
  }
}
```
