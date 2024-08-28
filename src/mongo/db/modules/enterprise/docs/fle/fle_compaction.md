# Queryable Encryption Compaction

## Background

Writes of QE indexed-encrypted values are accompanied by writes to the auxiliary collections: ESC and ECOC.

When writing an indexed-encrypted value (either thru insert, update, or findAndModify), there is at
least one corresponding document insert to the ESC to mark the number of times the current
`{field name, value (or edge for range), contention factor}` combination has been written.
This document is called a
[non-anchor](https://github.com/10gen/mongo/blob/v8.0/src/mongo/db/modules/enterprise/docs/fle/fle_protocol.md#non-anchor-record),
and the value captured within it is called its "position" or "cpos".

In addition to the ESC inserts, there is at least one corresponding document insert to the ECOC
to save the ESC token derived for the current `{field name, value, contention factor}` combination.
This ESC token is stored, encrypted using the field's ECOC token (aka its "compaction token"), in the
document's `value` field.
(See [Reference: ECOC Schema](https://github.com/10gen/mongo/blob/v8.0/src/mongo/db/modules/enterprise/docs/fle/fle_protocol.md#reference-ecoc-schema))
This document is used to facilitate the QE compaction & cleanup operations.

As more and more encrypted values are written, the ESC and ECOC will keep growing in size,
but only a subset of the non-anchor documents in the ESC will be useful for the encrypted search
algorithm. Moreover, a large ESC size affects the latency of encrypted reads and writes because the
encrypted binary search algorithm (i.e. `EmuBinary`) will take longer.
In order to reduce the size of the ESC, the non-anchor documents that are no longer
useful to the algorithm can be erased in a process called "compaction".

For each `{field name, value, contention factor}` combination `C`, compaction searches for the
last non-anchor "cpos" used for `C`, and saves it in an
[anchor document](https://github.com/10gen/mongo/blob/v8.0/src/mongo/db/modules/enterprise/docs/fle/fle_protocol.md#anchor-record).
Like non-anchors, anchor documents are given a sequentially-numbered position called "apos",
which is captured in its `_id` field. Once compaction has inserted this anchor document into the ESC,
it then deletes the non-anchors with "cpos" less than or equal to the "cpos" it saved in the anchor
document.

In order for compaction to be able to search the ESC for the last "cpos" used for `C`, it requires
the ESC tokens for `C`, which stored in the corresponding ECOC document's `value` field as
ciphertext. The compaction operation is provided the ECOC tokens for each encrypted field, via
the `compactStructuredEncryptionData` command, so that it can decrypt the ESC tokens.

Once the ESC compaction is finished, the documents in the ECOC that were used in the process can be
erased.

Since compaction simply deletes non-anchors, just to insert new anchors in their place, over time,
the number of anchors may still grow the size of the ESC if compaction is being performed regularly.
A separate "cleanup" operation exists to "squash" the accumulated anchor documents for `C` into a
single "null anchor" document, while also performing the same non-anchor reduction as compaction. This
[null anchor record](https://github.com/10gen/mongo/blob/v8.0/src/mongo/db/modules/enterprise/docs/fle/fle_protocol.md#null-anchor-record)
captures both the last used "cpos" and "apos" for `C`.

Like `compactStructuredEncryptionData`, the `cleanupStructuredEncryptionData` command comes with the
per-field ECOC tokens, so that the cleanup operation may decrypt the ESC tokens in the ECOC documents.

Although cleanup performs the same functions, and more, as compact, cleanup is a more expensive
operation because instead of simply inserting new anchors, cleanup has to update or insert null
anchor records, as well as remove stale anchors. This requires more reads of the ESC to find the
null anchor, as well as more memory for storing the non-anchor and anchor `_id`s to be deleted.
As for information leakage, cleanup also reveals more information than compact because the number of
null anchors that are left in the ESC tells how many unique `{field name, value, contention factor}`
combinations have been written. The expected usage, therefore, is to run compaction more often
than cleanup.

## Compaction Algorithm

Below is a pseudocode of the compaction algorithm (this roughly corresponds to step 4 of Figure 14 in the
[OST-v7 paper](https://github.com/10gen/crypto-docs/blob/main/ost/ost-v7.pdf)).
This is implemented in the function
[`processFLECompactV2()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact.cpp#L675-L680).
The following requires that no new writes are happening on the ECOC, which is why the ECOC is
[renamed into a temporary namespace](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L168-L174)
beforehand.

Input: Let `CT` be a mapping of each encrypted field name to a tuple
`{ECOCToken, AnchorPaddingRootToken}`, where`AnchorPaddingRootToken` is only present for range fields.

1. Initialize a set `CF`. Then for each unique ECOC document, `E`:
   - Decrypt `E.value` with the `CT[E.fieldName].ECOCToken` to get `S_esc`
     and an optional `isLeaf` flag (range only).
   - Insert `{E.fieldName, S_esc, isLeaf}` into `CF`.
2. Initialize a string map `rangeFields`. Then for each range field `R` in `CF`:
   - `rangeFields[R.fieldName].uniqueTokens++;`
   - `rangeFields[R.fieldName].uniqueLeaves += (R.isLeaf ? 1 : 0);`
3. For every `C` in `CF`:
   - a. Start a transaction.
   - b. Use `C.S_esc` to derive tokens `S_1` and `S_2`.
   - c. Run `EmuBinary(S_1, S_2)` to obtain `a_1` and `a_2`. `a_1` is the latest non-anchor
     position, and `a_2` is the latest anchor position.
     - If `a_1` is 0, then abort the operation with an error.
     - If `a_1` is null, then skip to 3.E. (ie. nothing to compact)
     - If `a_2` is null:
       - Find the null anchor: Let `r = db.esc.find({_id: HMAC(S_1, (0 || 0))}`.
       - Decrypt `r.value` with `S_2` to obtain `(apos || cpos)`.
       - Set `a_2 = apos`.
   - d. Insert into ESC the anchor document:\
     `{_id: HMAC(S_1, (0 || a_2 + 1)), value: Encrypt(S_2, (0 || a_1))}`
   - e. Commit transaction.
4. For each `{fieldName, counters}` in `rangeFields`:
   - a. Start a transaction.
   - b. Calculate `pathLength = Edges(fieldName).size()`
   - c. Calculate `numPads = ceil(paddingFactor * ((pathLength * counters.uniqueLeaves) - counters.uniqueTokens))`
   - d. Use `CT[fieldName].AnchorPaddingRootToken` to derive tokens `S_1d` and `S_2d`.
   - e. Run `AnchorBinaryHops(S_1d, S_2d)` to obtain `a`.
   - f. For `i` in `[1..numPads]`:
     - `padding = {_id: HMAC(S_1d, (0 || a + i), value: Encrypt(S_2d, (0 || 0))}`
     - Insert `padding` into ESC.
   - g. Commit transaction.

Step 1 is implemented as one read-only transaction in
[`readUniqueECOCEntriesInTxn()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact.cpp#L231).
As a result of this step, the `CF` set shall contain the decrypted ESC tokens for every unique
`{field, value, contention}` combination.

In Step 3, each unique combination `C` is compacted within a single internal transaction. Tokens are
first derived in (3.b), that are then used to search for the last-used non-anchor and anchor positions
in (3.c). After handling special cases in (3.c), it generates and inserts a new anchor document
(3.d) which captures the last-used non-anchor position `a_1` in its `value` field, and whose anchor
position is the last-used one (`a_2`), incremented by 1.

Step 3 is implemented in
[`compactOneFieldValuePairV2()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact.cpp#L371-L374).
However, the part where it runs the `EmuBinary()` (3.c) search algorithm is done through the
`GetQueryableEncryptionCountInfo` command invocation via a call to
[`FLEQueryInterface::getTags()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/fle_crud.cpp#L1645C1-L1663C2). This command runs `EmuBinary()` in
[`getEdgeCountInfoForCompact()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/crypto/fle_crypto.cpp#L1861).

Steps 2 and 4 were added in 8.0 to support compaction of range indexed-encrypted fields, and the
addition of "padding" anchor documents. Padding anchor documents are similar in format to regular
anchor documents and are also assigned a sequential "position" values, but they use tokens
derived from the `AnchorPaddingRootToken` to generate their `_id` values, instead of tokens derived
from the ESC token. The purpose of these padding documents is to further reduce the information
leakage of compaction, with respect to range encrypted fields.

The deletion of the non-anchor documents from the ESC and the deletion of the temporary ECOC are done
after this algorithm executes, during the processing of the
[`compactStructuredEncryptionData` command](#compactstructuredencryptiondata-command).

## Cleanup Algorithm

Below is a pseudocode of the cleanup algorithm (this roughly corresponds to steps 4-6 of Figure 16 in the
[OST-v7 paper](https://github.com/10gen/crypto-docs/blob/main/ost/ost-v7.pdf)).
This is implemented in the function
[`processFLECleanup()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact.cpp#L827).
Like compaction, the following requires that the ECOC is
[renamed into a temporary namespace](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_cleanup_cmd.cpp#L258-L264)
beforehand.

Input: Let `CT` be a mapping of each encrypted field name to a tuple
`{ECOCToken, AnchorPaddingRootToken}`, where`AnchorPaddingRootToken` is only present for range fields.

1. Initialize a priority queue `PQ`.
2. Initialize a set `CF`. Then for each unique ECOC document, `E`:
   - Decrypt `E.value` with the `CT[E.fieldName].ECOCToken` to get `S_esc`
     and an optional `isLeaf` flag (range only).
   - Insert `{E.fieldName, S_esc, isLeaf}` into `CF`.
3. For every `C` in `CF`:
   - a. Start a transaction.
   - b. Use `C.S_esc` to derive tokens `S_1` and `S_2`.
   - c. Run `EmuBinary(S_1, S_2)` to obtain `a_1` and `a_2`. `a_1` is the latest non-anchor
     position, and `a_2` is the latest anchor position.
   - d. Let `r = db.esc.find({_id: HMAC(S_1, (0 || 0))}`.
   - e. Let `(apos, cpos) = (r == null) ? (0, 0) : Decrypt(S_2, r.value)`.
   - f. If `a_1` is 0, then abort the operation with an error.
   - g. If `a_2` is null and `a_1` is not null, update the null anchor with:\
      `{_id: HMAC(S_1, (0 || 0)), value: Encrypt(S_2, (apos || a_1))}`
   - h. If `a_2` is 0, insert a new null anchor:\
      `{_id: HMAC(S_1, (0 || 0)), value: Encrypt(S_2, (0 || a_1))}`
   - i. If `a_2 > 0`:
     1. If `a_1` is null:
        - Let `r_prime = db.esc.find({_id: HMAC(S_1, (0 || a_2))})`.
        - Decrypt `r_prime.value` with `S_2` to obtain `(0 || a_s)`
        - Set `a_1 = a_s`
     2. Upsert the null anchor document:\
        `{_id: HMAC(S_1, (0||0))}, value: Encrypt(S_2, (a_2 || a_1))}`
     3. For `i` in `[apos + 1..a_2]`:
        - Enqueue `a_i = HMAC(0, i)` into `PQ`.
   - j. Commit transaction.
4. For each range field `R` in `CF`:
   - a. Use `CT[fieldName].AnchorPaddingRootToken` to derive tokens `S_1d` and `S_2d`.
   - b. Start a transaction.
   - c. Perform steps 3.c. thru 3.i. (except 3.f) above, substituting `S_1d` and `S_2d` for
     `S_1` and `S_2`, respectively. \* Note: `a_1` will always be 0 for padding anchors, since paddings don't have non-anchors.
   - d. Commit transaction.
5. Return `PQ`

Step 1 sets up an in-memory priority queue that will hold the anchor `_id`s that will need to be removed
from the ESC. The size of this is bounded by the [`maxAnchorCompactionSize`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/crypto/fle_options.idl#L51-L57)
server parameter.

Step 2 is the same as step 1 of the compaction algorithm, after which the `CF` set shall contain the
decrypted ESC tokens for every unique `{field, value, contention}` combination.

Step 3 is implemented in [`cleanupOneFieldValuePair()`](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact.cpp#L644).
In a nutshell, this step performs the EmuBinary algorithm to obtain the latest positions
for both non-anchors and anchors, then updates (or inserts) the null anchor with those latest values.
Additionally, if there are anchors that can be removed from the ESC, it enqueues their `_id` into `PQ`.
`PQ` is a max priority queue of `PrfBlock` (32-byte array) that uses the standard
`std::less<std::array>` comparator.

Step 4 is introduced in 8.0 to support range fields. It is similar to step 3, but operates on "padding" anchors,
instead of normal anchors.

The deletion of the non-anchor & anchor documents from the ESC and the deletion of the temporary
ECOC are done after this algorithm executes, during the processing of the
[`cleanupStructuredEncryptionData` command](#cleanupstructuredencryptiondata-command).

## `compactStructuredEncryptionData` Command

Suppose a database `testdb` has a FLE2 collection named `foo`, whose auxiliary ESC and ECOC
collections are named `enxcol_.foo.esc` and `enxcol_.foo.ecoc`, respectively. On replica sets,
when the server gets a `compactStructuredEncryptionData` command for `testdb.foo`, it:

1. [Creates](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L110)
   a `FixedFCVRegion` to prevent an FCV change while the operation is ongoing.
1. Acquires IX lock on `testdb`.
1. Acquires IS lock on `testdb.foo`.
1. [Ensures](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L136)
   the command contains the required compaction tokens per the `encryptedFieldConfig` of `testdb.foo`.
1. Acquires X lock on the namespace `enxcol_.foo.ecoc.lock`, so that cleanup and compact command
   invocations on the same `testdb.foo` namespace will be serialized.
1. If `enxcol_.foo.ecoc` exists, and `enxcol_.foo.ecoc.compact` does not, the server
   [randomly reads](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L165-L166)
   non-anchor document `_id`s from the ESC into a set `escDeleteSet`, up to a limit defined by the
   `maxCompactionSize` cluster server parameter. This set serves as an in-memory snapshot of the ESC
   non-anchors **before** the ECOC is renamed.
1. [Renames](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L173-L174)
   `enxcol_.foo.ecoc` to `enxcol_.foo.ecoc.compact` if `enxcol_.foo.ecoc.compact` does not exist.
   This renamed ECOC collection serves as the ECOC snapshot used for compact/cleanup.
1. [Creates](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L195)
   a new `enxcol_.foo.ecoc` clustered collection (if needed). FLE2 writes that occur while the
   current compact/cleanup is in progress will write their ECOC documents in this newly created ECOC collection.
1. [Runs](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L219-L224)
   the [compaction algorithm](#compaction-algorithm).
1. [Deletes](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L232)
   all non-anchor documents ID'ed in `escDeleteSet` from the ESC.
1. [Drops](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_compact_cmd.cpp#L243-L247)
   `enxcol_.foo.ecoc.compact`.
1. Returns a reply containing compaction stat counters.

On sharded clusters, the `compactStructuredEncryptionData` command does the following:

1. `mongos` [rewrites](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/s/commands/cluster_fle2_compact_cmd.cpp#L111-L148)
   the command as `_shardsvrCompactStructuredEncryptionData` and forwards it to the
   shard server that is running the `ShardingDDLCoordinator` responsible for executing it.
1. The shard server that receives it will
   [validate](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/shardsvr_compact_structured_encryption_data_command.cpp#L136)
   the compact request contains the required compaction tokens, set up a `CompactStructuredEncryptionDataState`,
   and [schedule](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/shardsvr_compact_structured_encryption_data_command.cpp#L112-L123)
   it for execution by the `CompactStructuredEncryptionDataCoordinator`. \* If another cleanup operation is already taking place for `testdb.foo`, then the new command
   will "join" the current operation, and will receive the same response.
1. The `CompactStructuredEncryptionDataCoordinator` runs the compact operation in phases:
   1. ["Rename" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/compact_structured_encryption_data_coordinator.cpp#L365-L366)
      renames `enxcol_.foo.ecoc` to `enxcol_.foo.ecoc.compact` if possible, and populates the in-memory
      `escDeleteSet` with ESC non-anchor `_id`s.
   1. ["Compact" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/compact_structured_encryption_data_coordinator.cpp#L379)
      runs the [compaction algorithm](#compaction-algorithm), and deletes the ESC non-anchor documents
      ID'ed in `escDeleteSet`.
   1. ["Drop" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/compact_structured_encryption_data_coordinator.cpp#L390-L392)
      drops `enxcol_.foo.ecoc.compact`, and sets the final command response.

Note that since the `escDeleteSet` is in-memory only, it does not carry over if, in between the rename
and compact phases, the coordinator is interrupted and has to resume on a different node. However,
this is fine because a subsequent compact command will still be able to delete the non-anchors that
didn't get deleted the first time.

The `CompactStructuredEncryptionDataCoordinator` holds DDL locks (`MODE_X`) on the following
namespaces while it is running:

- the data collection (eg. `testdb.foo`)
- the ESC collection (eg. `testdb.enxcol_.foo.esc`)
- the ECOC collection (eg. `testdb.enxcol_.foo.ecoc`)
- the renamed ECOC collection (eg. `testdb.enxcol_.foo.ecoc.compact`)

## `cleanupStructuredEncryptionData` Command

Suppose a database `testdb` has a FLE2 collection named `foo`, whose auxiliary ESC and ECOC
collections are named `enxcol_.foo.esc` and `enxcol_.foo.ecoc`, respectively. On replica sets,
when the server gets a `cleanupStructuredEncryptionData` command for `testdb.foo`, it:

1. Runs steps 1-8 of the `cleanupStructuredEncryptionData` command above.
1. [Runs](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_cleanup_cmd.cpp#L287-L293)
   the [cleanup algorithm](#cleanup-algorithm). This returns a priority queue `PQ` of ESC anchor `_id`s.
1. [Deletes](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_cleanup_cmd.cpp#L296-L297)
   all non-anchor documents ID'ed in `escDeleteSet` from the ESC.
1. [Deletes](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_cleanup_cmd.cpp#L300)
   all anchor documents ID'ed in `PQ` from the ESC.
1. [Drops](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/commands/fle2_cleanup_cmd.cpp#L304)
   `enxcol_.foo.ecoc.compact`.
1. Returns a reply containing cleanup stat counters.

On sharded clusters, the `cleanupStructuredEncryptionData` command does the following:

1. `mongos` [rewrites](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/s/commands/cluster_fle2_cleanup_cmd.cpp#L111-L148)
   the command as `_shardsvrCleanupStructuredEncryptionData` and forwards it to the
   shard server that is running the `ShardingDDLCoordinator` responsible for executing it.
1. The shard server that receives it will
   [validate](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/shardsvr_cleanup_structured_encryption_data_command.cpp#L135)
   the cleanup request contains the required compaction tokens, set up a `CleanupStructuredEncryptionDataState`,
   and [schedule](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/shardsvr_cleanup_structured_encryption_data_command.cpp#L111-L122)
   it for execution by the `CleanupStructuredEncryptionDataCoordinator`. \* If another cleanup operation is already taking place for `testdb.foo`, then the new command
   will "join" the current operation, and will receive the same response.
1. The `CleanupStructuredEncryptionDataCoordinator` runs the cleanup operation in phases:
   1. ["Rename" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/cleanup_structured_encryption_data_coordinator.cpp#L471-L472)
      renames `enxcol_.foo.ecoc` to `enxcol_.foo.ecoc.compact` if possible, and
      populates the in-memory `escDeleteSet` with ESC non-anchor `_id`s.
   1. ["Cleanup" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/cleanup_structured_encryption_data_coordinator.cpp#L485-L486)
      runs the [cleanup algorithm](#cleanup-algorithm), deletes the ESC non-anchor documents ID'ed
      in `escDeleteSet`, and sets an in-memory priority queue `PQ` of ESC anchor `_id`s which will
      be deleted in the next phase.
   1. ["Anchor Delete" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/cleanup_structured_encryption_data_coordinator.cpp#L493-L494)
      deletes the anchors ID'ed in `PQ` from the ESC.
   1. ["Drop" phase](https://github.com/10gen/mongo/blob/585289829b3e38b4425520982039f694c5aee4c8/src/mongo/db/s/cleanup_structured_encryption_data_coordinator.cpp#L501-L504)
      drops `enxcol_.foo.ecoc.compact`, and sets the final command response.

Like sharded compaction, the `escDeleteSet` does not carry over if there a primary stepdown occurs
between the rename and cleanup phases. Similarly, `PQ` does not survive stepdowns between the cleanup
and the anchor deletion phases. This is fine by design.

Like the `CompactStructuredEncryptionDataCoordinator`, the `CleanupStructuredEncryptionDataCoordinator`
holds DDL locks (`MODE_X`) on the following namespaces while it is running:

- the data collection (eg. `testdb.foo`)
- the ESC collection (eg. `testdb.enxcol_.foo.esc`)
- the ECOC collection (eg. `testdb.enxcol_.foo.ecoc`)
- the renamed ECOC collection (eg. `testdb.enxcol_.foo.ecoc.compact`)

Therefore, a `CompactStructuredEncryptionDataCoordinator` cannot run at the same time as a
`CleanupStructuredEncryptionDataCoordinator` on the same FLE2 namespace.
