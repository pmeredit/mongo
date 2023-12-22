/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {findMatchingLogLines} from "jstests/libs/log.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

function uuidStr() {
    return UUID().toString().split('"')[1];
}

export class TestHelper {
    constructor(
        input, middlePipeline, interval = 0, sourceType = "kafka", useNewCheckpointing = false) {
        this.sourceType = sourceType;
        this.input = input;
        this.uri = 'mongodb://' + db.getMongo().host;
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = true;
        this.kafkaTopic = "topic1";
        this.dbConnectionName = "db1";
        this.dbName = "test";
        this.dlqCollName = uuidStr();
        this.inputCollName = uuidStr();
        this.outputCollName = uuidStr();
        this.processorId = uuidStr();
        this.tenantId = "jstests-tenant";
        this.outputColl = db.getSiblingDB(this.dbName)[this.outputCollName];
        this.inputColl = db.getSiblingDB(this.dbName)[this.inputCollName];
        this.dlqColl = db.getSiblingDB(this.dbName)[this.dlqCollName];
        this.spName = uuidStr();
        this.checkpointCollName = uuidStr();
        this.checkpointColl = db.getSiblingDB(this.dbName)[this.checkpointCollName];
        this.checkpointIntervalMs = null;  // Use the default.

        this.useNewCheckpointing = useNewCheckpointing;
        this.writeDir = "/tmp/checkpoint";
        this.restoreDir = "/tmp/checkpoint";
        this.spWriteDir = `${this.writeDir}/${this.tenantId}/${this.processorId}`;
        this.spRestoreDir = `${this.restoreDir}/${this.tenantId}/${this.processorId}`;
        mkdir(this.writeDir);
        mkdir(this.restoreDir);

        if (interval !== null) {
            this.checkpointIntervalMs = NumberInt(interval);
        }

        let checkpointOptions = {
            storage: {
                uri: this.uri,
                db: this.dbName,
                coll: this.checkpointCollName,
            },
            debugOnlyIntervalMs: this.checkpointIntervalMs,
        };
        if (this.useNewCheckpointing) {
            checkpointOptions.storage = null;
            checkpointOptions.localDisk = {writeDirectory: this.writeDir};
        }

        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: checkpointOptions
        };
        this.connectionRegistry = [
            {name: this.dbConnectionName, type: 'atlas', options: {uri: this.uri}},
            {
                name: this.kafkaConnectionName,
                type: 'kafka',
                options: {
                    bootstrapServers: this.kafkaBootstrapServers,
                    isTestKafka: this.kafkaIsTest,
                },
            },
            {
                name: '__testMemory',
                type: 'in_memory',
                options: {},
            }
        ];

        // Setup the pipeline.
        this.pipeline = [];
        // First, append either a kafka or changestream source.
        if (this.sourceType === 'kafka') {
            this.pipeline.push({
                $source: {
                    connectionName: this.kafkaConnectionName,
                    topic: this.kafkaTopic,
                    testOnlyPartitionCount: NumberInt(1),
                    timeField: {
                        $toDate: "$ts",
                    }
                }
            });
        } else if (this.sourceType === 'memory') {
            this.pipeline.push(
                {$source: {connectionName: '__testMemory', timeField: {$toDate: "$ts"}}});
        } else {
            this.pipeline.push({
                $source: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.inputCollName,
                    timeField: {
                        $toDate: "$fullDocument.ts",
                    }
                }
            });
        }
        for (let stage of middlePipeline) {
            this.pipeline.push(stage);
        }
        this.pipeline.push({
            $merge: {
                into: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.outputCollName
                }
            }
        });
        this.sp = new Streams(this.connectionRegistry);
        if (this.useNewCheckpointing) {
            this.sp.setUseUnnestedWindow(true);
        }
    }

    // Helper functions.
    run(firstStart = true) {
        if (this.useNewCheckpointing) {
            let idsOnDisk = this.getCheckpointIds();
            if (idsOnDisk.length > 0) {
                this.startOptions.checkpointOptions.localDisk.restoreDirectory =
                    `${this.restoreDir}/${this.tenantId}/${this.processorId}/${idsOnDisk[0]}`;
            } else {
                this.startOptions.checkpointOptions.localDisk.restoreDirectory = null;
            }
        }

        this.sp.createStreamProcessor(this.spName, this.pipeline);
        jsTestLog(`Starting ${this.spName} with options ${tojson(this.startOptions)}`);
        this.sp[this.spName].start(this.startOptions, this.processorId, this.tenantId);
        if (this.sourceType === 'kafka' || this.sourceType === 'memory') {
            // Insert the input.
            assert.commandWorked(db.runCommand({
                streams_testOnlyInsert: '',
                name: this.spName,
                documents: this.input,
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input));
        }
    }

    stop(assertWorked = true) {
        this.sp[this.spName].stop(assertWorked);
    }

    stats() {
        return this.sp[this.spName].stats();
    }

    // returns the commit checkpoint IDs
    // the 0-th index of the returned array is the most recently committed checkpoint
    getCheckpointIds() {
        if (this.useNewCheckpointing) {
            if (!pathExists(this.spWriteDir)) {
                return [];
            }
            let ids = listFiles(this.spWriteDir)
                          // Get the committed checkpoint directories by looking for manifest files.
                          .filter((checkpointDir) => {
                              let manifestFile = listFiles(checkpointDir.name)
                                                     .find(file => file.name.includes("MANIFEST"));
                              return manifestFile !== null;
                          })
                          // Retrieve the checkpointID from each directory.
                          .map(checkpointDir => checkpointDir.name.split('/').pop());
            // Sort by checkpoint ID.
            ids.sort();
            // Reverse the array so the most recently committed checkpoint ID is at index 0.
            return ids.reverse();
        } else {
            return this.checkpointColl.find({_id: {$regex: "^checkpoint"}})
                .sort({_id: -1})
                .toArray()
                .map(i => i._id);
        }
    }

    getResults() {
        return this.outputColl.find({}).sort({idx: 1}).toArray();
    }

    getStartOffsetFromCheckpoint(checkpointId) {
        assert(this.sourceType === 'kafka', "only valid to call for kafka source type");
        jsTestLog(`Getting start offset from ${checkpointId}`);
        let stateObject = null;
        if (this.useNewCheckpointing) {
            const log = assert.commandWorked(db.adminCommand({getLog: "global"}));
            // TODO(SERVER-81440): We should find a better way to determine the start offset.
            // One option is to expose it in (verbose) stats.
            // Log lines are unreliable because they get truncated.
            for (const line of findMatchingLogLines(log.log, {id: 77177})) {
                let entry = JSON.parse(line);
                if (entry.attr.checkpointId == checkpointId) {
                    stateObject = entry.attr.state;
                    jsTestLog(stateObject);
                }
            }
            assert(stateObject != null, "didn't find expected state object in the logs");
        } else {
            let sourceState =
                this.checkpointColl
                    .find({
                        _id: {
                            $regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`
                        }
                    })
                    .sort({_id: -1})
                    .toArray();
            stateObject = sourceState[0]["state"];
        }
        let startOffset = stateObject["partitions"][0]["offset"];
        jsTestLog(`Start offset for ${checkpointId} is ${startOffset}`);
        return startOffset;
    }

    getSourceState(checkpointId) {
        let sourceState =
            this.checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        return sourceState[0];
    }

    getStartingPointFromCheckpoint(checkpointId) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        if (this.useNewCheckpointing) {
            const log = assert.commandWorked(db.adminCommand({getLog: "global"}));
            let startingPoint = null;
            // TODO(SERVER-81440): We should find a better way to determine the start offset.
            // One option is to expose it in (verbose) stats.
            // Log lines are unreliable because they get truncated.
            for (const line of findMatchingLogLines(log.log, {id: 7788506})) {
                let entry = JSON.parse(line);
                if (entry.attr.checkpointId == checkpointId) {
                    startingPoint = JSON.parse(entry.attr.state)["startingPoint"];
                }
            }
            assert(startingPoint != null, "didn't find expected state object in the logs");
            if (startingPoint.hasOwnProperty("$timestamp")) {
                let ts = startingPoint["$timestamp"];
                return {resumeToken: null, startAtOperationTime: new Timestamp(ts["t"], ts["i"])};
            } else {
                return {resumeToken: startingPoint, startAtOperationTime: null};
            }
        } else {
            // This is a variant<object, Timestamp> corresponding to a resumeToken or
            // timestamp.
            let startingPoint = this.getSourceState(checkpointId)["state"]["startingPoint"];
            if (startingPoint instanceof Timestamp) {
                return {
                    resumeToken: null,
                    startAtOperationTime: startingPoint,
                };
            } else {
                return {resumeToken: startingPoint, startAtOperationTime: null};
            }
        }
    }

    getNextEvent(resumeAfterToken) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        const stream = this.inputColl.watch([], {resumeAfter: resumeAfterToken});
        if (stream.hasNext()) {
            return stream.next();
        } else {
            return null;
        }
    }

    errStr() {
        return `checkpointCollName: ${this.checkpointCollName}`;
    }

    removeCheckpointsNotInList(ids) {
        if (this.useNewCheckpointing) {
            let idsOnDisk = listFiles(this.spWriteDir).map(f => f.name.split('/').pop());
            for (let idOnDisk of idsOnDisk) {
                if (!ids.includes(idOnDisk)) {
                    let path = `${this.spWriteDir}/${idOnDisk}`;
                    removeFile(path);
                }
            }
        } else {
            // This deletes the id we just verified, and any new
            // checkpoints this replay created.
            this.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id), $not: /^operator/}});
        }
    }
}
