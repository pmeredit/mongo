# Magic Restore

Magic restore is a built-in tool within `mongod` designed to quickly and correctly restore customer backups.

Enabled with a server parameter, magic restore manages the complex steps required to restore a cluster to a consistent point in time, streamlining the process for Cloud. This includes updating internal replication and sharding metadata. To use magic restore, a user must supply a `restoreConfiguration` which specifies the options for the restore process. During the restore operation, the tool executes the necessary procedures and subsequently shuts down `mongod` cleanly. Note that normal database operations in `mongod` are unavailable while a restore is in progress. Magic restore operates on WiredTiger files produced by a backup cursor.

Users should run magic restore on all nodes of their cluster, as the tool operates per-node. Magic restore is supported for MongoDB 8.0 and later.

### Supported restores and topologies

Magic restore supports both point-in-time (PIT) and non-point-in-time restores. A non-PIT restore is consistent to the WiredTiger checkpoint timestamp specified by the backed-up data files. A PIT restore inserts and applies additional oplog entries up to a particular timestamp. The tool supports replica sets and sharded clusters, including embedded config servers. Users can also perform selective restores, shard ID renames, as well as enable or disable the balancer.

## Usage

To start magic restore, a user can start `mongod` (with enterprise features) with the `--magicRestore` server parameter. Magic restore also requires the use of the `--replSet` parameter. If sharded cluster is being restored, the user should omit `--shardsvr` and `--configsvr` parameters.

Usage is as follows:

```
./mongod --magicRestore --replSet <replica set name> --dbpath <path to backup files>
```

The `mongod` then expects the `restoreConfiguration` to be streamed in via a bytestream written to `stdin`. For a non-PIT restore, after receiving the configuration the restore `mongod` will automatically perform the restore without any additional input. For a PIT restore, `mongod` expects additional oplog entries to be streamed in bytestream to `stdin`, and stops reading once the bytestream produces an `EOF` condition. Once restore completes, `mongod` will be shut down. Afterwards, a user can start the `mongod` up normally, with all expected startup parameters.

> Notes: Cloud requires the `restoreConfiguration` to be input via `stdin` for security purposes. Tests use a named pipe to write the configuration and additional oplog entries into magic restore. `--replSet` is required to avoid running the restore procedure in a standalone mode, as standalone nodes may have subtle behavior differences that could impact how data is written.

The `restoreConfiguration` has the following shape:

```
{
    "nodeType": <node type>,
    "replicaSetConfig": <replica set config object>,
    "maxCheckpointTs": <timestamp>,
    "pointInTimeTimestamp": <timestamp>,
    "restoreToHigherTermThan": <long long>,
    "collectionsToRestore": [
        {
            "ns": <namespace>,
            "uuid": <UUID>,
        },
        ...
    ],
    "shardingRename": [
        {
            "sourceShardName": <shardId object>,
            "destinationShardName": <shardId object>,
            "destinationShardConnectionString": <string>
        },
        ...
    ],
    "shardIdentityDocument": <shard identity object>,
    "balancerSettings": <balancer settings object>,
    "automationCredentials": {
        createRoleCommands: [
            <createRole command objects>
        ],
        createUserCommands: [
            <createUser command objects>
        ],
    },
    "systemUuids": [
        {
            "ns": <namespace>,
            "uuid": <UUID>
        },
        ...
    ],
}
```

- (**Required**) `nodeType`
  - Refers to the type of node being restored. Acceptable values include `replicaSet`, `shard`, `configServer`, `configShard`.
- (**Required**) `replicaSetConfig`
  - Contains the new replica set configuration to install during the automated restore procedure. Note that the server expects a well-formed config and will not validate the contents of the object.
- (**Required**) `maxCheckpointTs`
  - Used to truncate the oplog at the beginning of the restore process. This can be considered the “starting point” of the PIT restore process.
- (Optional) `pointInTimeTimestamp`
  - Used to restore the node up to a point-in-time when inserting additional oplog entries for a PIT restore. This is considered the “end point” of a PIT restore. The additional oplog entries passed via standard input may extend beyond the point-in-time we want to restore to, so we use this timestamp as the endpoint. Omitting this will replay up to the top of the oplog.
  - The existence of this field determines if the restore is non-PIT or PIT.
  - Note that we do not validate this timestamp.
- (Optional) `restoreToHigherTermThan`
  - Installed as the latest term on the restored node, to ensure that the drivers can communicate with it.
  - If present, the server will write out a no-op oplog entry with the message: "restore incrementing term".
- (Optional) `collectionsToRestore`
  - Provides the ability to perform selective restores, where the only collections restored are passed as namespace and UUID pairs.
  - When omitted entirely or is an empty array, all collections are restored on the node.
  - If present, only the specified user collections are preserved. System collections are always preserved.
- (Optional) `shardingRename`
  - Used to rename shard IDs in sharding metadata documents.
- (Optional) `shardIdentityDocument`
  - Used to overwrite the existing shard identity document. Note that if passing this argument into a config shard, the shardName _must_ be `config`.
  - Note that the `clusterId` in the shard identity must be the same across all nodes in the cluster being restored. It is valid for this cluster ID to change from a source cluster to a restored cluster, as long as it's consistent across nodes.
  - If the `shardingRename` parameter has been provided, this parameter becomes required to account for the new shard ID.
- (Optional) `balancerSettings`
  - Used to set the balancer settings in a sharded cluster restore. If omitted, the balancer state will default to the value set in the data files.
- (Optional) `automationCredentials`
  - Includes `createRole` and `createUser` commands for the automation agent to perform on the restored node.
- (Optional) `systemUuids`
  - Used when creating new system collections on the restored node. If some system collections do not exist on the restored cluster, magic restore will create them. The collections magic restore may create include `config.settings`, `admin.system.users`, `admin.system.roles`. To ensure these collections have the same UUID across a replica set, specify the UUID for each namespace in this parameter.
  - If we need to create these system collections and the `systemUuids` parameter is missing, we will fail the restore.

#### Error handling

Magic restore is not retryable or resumable at this time. If magic restore encounters an error while running the restore procedure, `mongod` will crash with a fatal error. To re-run the process, a user must reset the backup data files and attempt the procedure again. This is because a restore stopped midway may have already started modifying data on disk, so we should ensure the next attempt starts from a clean slate.

#### Selective restores

When performing a selective restore with magic restore, a user must also pass in the `--restore` flag to the startup invocation:

```zsh
./mongod --magicRestore --restore --replSet <replica set name> --dbpath <path to backup files>
```

#### Magic restore progress monitoring

All magic restore log output is tagged with a `RESTORE` log component. Users can filter regular `mongod` logs by this component to produce restore-only log output. Additionally, on restore completion the tool will output the time taken for each step of the restore procedure for debugging purposes:

```
{
    Statistics: {
        Reading magic restore configuration: <ms>,
        Truncating oplog to max checkpoint timestamp and truncating local DB collections: <ms>,
        Inserting additional entries into the oplog for a point-in-time restore: <ms>,
        Truncating oplog to PIT timestamp: <ms>,
        Applying oplog entries for restore: <ms>,
        Updating sharding metadata: <ms>,
        Magic restore total elapsed time: <ms>
    }
}
```

## Architecture

When a `mongod` is started with the `--magicRestore` parameter, it initializes a [thread](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1068-L1079) that runs the magic restore process. This thread is started when `mongod` completes startup, by [registering](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1100-L1106) with the [global lifecycle monitor](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/server_lifecycle_monitor.h). This monitor runs registered tasks when the server reaches certain milestones, such as finishing startup.

### BSONStreamReader

The parameters and data required for magic restore are read in as bytes from `stdin`. The [`BSONStreamReader`](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.h#L12-L55) class manages the underlying input stream and parses the raw bytes into valid `BSONObj`s. The first item read by the stream reader is always the `restoreConfiguration`.

### Restore procedure

On a high level, the restore procedure is as follows:

- [Parse](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L881-L882) the restore configuration from the BSON stream.
- [Set](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L887-L889) the `initialDataTimestamp` to the sentinel value to only allow unstable checkpoints. We do not expect a node to require rollback after completing restore, so performing unstable-only checkpoints avoids maintaining history in the WiredTiger cache.
- [Truncate](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L916-L917) the oplog to the `maxCheckpointTs` from the restore configuration. This ensures any extra oplog entries from beyond the WiredTiger checkpoint timestamp are removed.
- [Drop](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L257-L272) replication metadata. This includes the current replica set config, the `oplogTruncateAfterPoint`, the `lastVote` document, and a few others.
- [Insert](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L923-L929) a new replica config and default `minValid` document. The replica set is received via the `restoreConfiguration`.
- If the restore is point-in-time:
  - [Parse additional oplog entries and insert them into the oplog](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L121-L160).
  - [Truncate](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L948-L954) the oplog to `pointInTimeTimestamp` from the restore configuration. This removes any newly inserted oplog entries from beyond the desired restore time.
  - [Apply](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L960-L961) the new oplog entries. Note that after applying each oplog entry batch, the replication recovery code will set the stable timestamp to the timestamp from the last entry in the batch. After we apply additional oplog entries in magic restore, the stable timestamp will be set to the top of the oplog.
- [Create](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L972-L973) internal collections if needed. The namespaces and UUIDs for these collections are passed in via the `systemUuids` parameter in the restore configuration.
- If we are restoring a shard node, update the sharding metadata:
  - If `balancerSettings` was specified in the restore configuration, [update](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L696-L697) the balancer settings document on config servers.
  - If the restore is selective, [run](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L706) the selective restore-specific steps. This will [create](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L642-L656) namespace-UUID pairs in the `config.system.collections_to_restore` collection, using the `collectionsToRestore` values from the restore configuration. Then, we will run the [`_configSvrRunRestore`](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/s/config/configsvr_run_restore_command.cpp#L185) command on config servers to remove any metadata for unrestored collections.
  - If the restore is performing shard renames, [update](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L710) all sharding metdata that references the old shard ID to reference the new shard ID.
  - [Abort](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L714-L735) any in-progress resharding operations.
  - [Drop](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L737-L741) the `config.placementHistory` collection.
  - If a new shard identity was passed in via the `shardIdentity` parameter in the restore configuration, [update](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L762-L774) the shard identity document.
  - [Drop](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L776-L792) cached config collections.
- [Drop](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L995-L996) all non-approved cluster parameters. The list of cluster parameters to persist across restore is [here](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L105-L114).
- If `restoreToHigherTermThan` is present in the restore configuration, [insert](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1009-L1010) a no-op oplog entry with the specified term. When `mongod` starts up after restore, it will set its current term from the last oplog entry. Note that we must [set the stable timestamp](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1014-L1018) to the timestamp of this oplog entry, as it is now top of the oplog.
- If `automationCredentials` is present in the restore configuration, [run](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1022-L1027) the `createRole` and `createUser` commands specified.
- [Set](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1029-L1043) the `initialDataTimestamp` and `oldestTimestamp`. Both these values are set to the value of the `stableTimestamp`.
- Finally, [output](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore.cpp#L1051-L1053) time statistics for the restore procedure and trigger shutdown.

### Timestamps

Backup data files produced by a `$backupCursor` contain a `recoveryTimestamp` that indicates the timestamp at which the data in the backup is consistent. This is also known as the checkpoint timestamp. In WiredTiger, the `stableTimestamp` of the data files is equivalent to this `recoveryTimestamp` at the start of restore. The data files additionally have an `oldestTimestamp` at some timestamp before the `stableTimestamp`. When the restore procedure starts, it sets the `initialDataTimestamp` to the sentinel value. This ensures WiredTiger only takes unstable checkpoints for the restore procedure, as we do not expect anything in the restore procedure to be recoverable via rollback.

During the restore procedure, if the restore is PIT or we insert a higher term oplog entry, the `stableTimestamp` is updated to timestamp at the top of the oplog. At the end of restore, restore will set the `initialDataTimestamp` and `oldestTimestamp` to the value of the `stableTimestamp`. If no additional oplog entries were inserted, this timestamp will be the `recoveryTimestamp` from the initial backup metadata.

By setting a valid `initialDataTimestamp`, we ensure that WiredTiger will take a stable checkpoint at shutdown, ensuring a clean restart. By setting the `oldestTimestamp` to the top of the oplog, we ensure we discard any history from before the restore. Snapshot reads from before the `stableTimestamp` will fail with a `SnapshotTooOld` error.

Note that magic restore sets the `stableTimestamp` to the top of the oplog without explicitly validating that this timestamp is majority committed. The assumption is that by performing the restore procedure on all nodes in the replica set (preferably operating on copies of the same backed up data files) the nodes will all agree on the stable timestamp, therefore making it majority committed. If for whatever reason a rollback attempts to reset a newly restored node to a timestamp before the `stableTimestamp`/`initialDataTimestamp`/`oldestTimestamp`, the node will `fassert`, as it cannot complete the rollback safely. However, if a restore completes successfully, it is unlikely a node will require a rollback.

### Testing

Unit tests live in [`magic_restore_test.cpp`](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/src/magic_restore/magic_restore_test.cpp). JavaScript tests are in the [`hot_backups/magic_restore/`](https://github.com/10gen/mongo/tree/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/src/mongo/db/modules/enterprise/jstests/hot_backups/magic_restore) directory in the enterprise module. The [`MagicRestoreTest`](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/jstests/libs/magic_restore_test.js#L10) test fixture manages the magic restore procedure for a replica set, while the [`ShardedMagicRestoreTest`](https://github.com/10gen/mongo/blob/0b33e47c67393aa9bb8e89c87a32984cf9f1b436/jstests/libs/sharded_magic_restore_test.js#L10) test fixture manages the restore procedure for a sharded cluster. Both fixtures provide helpers to make testing restore easier.
