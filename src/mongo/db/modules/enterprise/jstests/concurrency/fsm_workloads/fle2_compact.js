/**
 * fle2_compact.js
 *
 * Performs insert/update operations on both an encrypted and unencrypted collection,
 * with sporadic compact operation on the encrypted collection.
 *
 * @tags: [
 * requires_fcv_70,
 * assumes_read_preference_unchanged,
 * assumes_balancer_off,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

export const $config = (function() {
    // 'data' is passed (copied) to each of the worker threads.
    var data = {
        encryptedFields: {
            fields: [
                {
                    path: "first",
                    bsonType: "string",
                    queries: {"queryType": "equality", "contention": 0}
                },
                {
                    path: "ssn",
                    bsonType: "string",
                    queries: {"queryType": "equality", "contention": 0}
                },
            ]
        },
        opIterationsPerState: 5,
        shardKey: {count: "hashed"},
        getRandomDocument: function() {
            const firsts = ["bob", "linda", "louise", "gene", "tina"];
            const ssns = ["123222212", "578862589", "678235522", "123456789", "987654321"];
            function getRandomValue(list) {
                let idx = Math.floor(Math.random() * list.length);
                return list[idx];
            }
            return {
                first: getRandomValue(firsts),
                ssn: getRandomValue(ssns),
            };
        },
        valuesInserted: {},
        assertUntilCommandWorksOrUnexpectedError: function(cmdFunc, cmdName) {
            let res;
            assert.soon(() => {
                res = cmdFunc();
                try {
                    assert.commandWorked(res);
                    return true;
                } catch (e) {
                    // OperationNotSupportedInTransaction may be thrown during two-phase commit of
                    // a FLE2 insert/update if the insert/update had to implicitly create the ECOC
                    // (because a parallel compaction is in the middle of renaming and replacing
                    // the ECOC)
                    assert.commandFailedWithCode(
                        res,
                        [ErrorCodes.WriteConflict, ErrorCodes.OperationNotSupportedInTransaction],
                        `${cmdName} failed, but not with expected error`);
                }
                return false;
            }, `Unable to execute ${cmdName} successfully`);
            return res;
        },
        ecocExists: function(eclient, encryptedCollName) {
            const ecocInfo = eclient.getDB().getCollectionInfos(
                {"name": eclient.getStateCollectionNamespaces(encryptedCollName).ecoc});
            return ecocInfo.length > 0;
        }
    };

    // 'states' are the different functions callable by a worker
    // thread. The 'this' argument of any exposed function is
    // bound as '$config.data'.
    var states = {
        init: function init(db, collName) {
            this.count = 0;
            this.encryptedClient = new EncryptedClient(db.getMongo(), db.getName());
            this.edb = this.encryptedClient.getDB();
            this.compactRuns = 0;
            this.iteration = 1;
        },

        insertDocs: function insertDocs(db, collName) {
            print(`Entering insert phase. sessionId: ${
                tojson(this.edb.getSession().getSessionId())}`);
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const insertDoc = this.getRandomDocument();

                insertDoc["tid"] = this.tid;
                insertDoc["count"] = ++this.count;

                this.valuesInserted[insertDoc.first] = 1;
                this.valuesInserted[insertDoc.ssn] = 1;

                // Insert a document into encrypted collection; retry on write conflict error
                let res = this.assertUntilCommandWorksOrUnexpectedError(
                    () => { return encryptedColl.einsert(insertDoc); }, "Insert");

                // Insert the same document in the unencrypted collection
                res = db[collName].insert(insertDoc);
                assert.writeOK(res);
            }
        },
        updateDocs: function updateDocs(db, collName) {
            print(`Entering update phase. sessionId: ${
                tojson(this.edb.getSession().getSessionId())}`);
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const indexToUpdate = Math.floor(Math.random() * this.count) + 1;
                const updateDoc = {$set: this.getRandomDocument()};
                const queryDoc = {count: indexToUpdate, tid: this.tid};

                this.valuesInserted[updateDoc["$set"].first] = 1;
                this.valuesInserted[updateDoc["$set"].ssn] = 1;

                // Update a document; retry on write conflict error
                let res = this.assertUntilCommandWorksOrUnexpectedError(
                    () => { return encryptedColl.eupdate(queryDoc, updateDoc); }, "Update");

                // Update same document in unencrypted collection
                res = db[collName].update(queryDoc, updateDoc);
                assert.commandWorked(res);
            }
        },
        compact: function compact(db, collName) {
            print(`Entering compact phase. sessionId: ${
                tojson(this.edb.getSession().getSessionId())}`);
            const encryptedColl = this.edb[this.encryptedCollName];

            // Insert a few encrypted documents with a thread unique value.
            // This is to make sure that values unique to each thread also get compacted,
            // not just the common values generated by getRandomDocument.
            const insertDoc = {first: ("tid" + this.tid + "_" + this.compactRuns)};
            for (let i = 0; i < 3; i++) {
                this.assertUntilCommandWorksOrUnexpectedError(
                    () => { return encryptedColl.einsert(insertDoc); }, "insert");
            }
            assert.soon(() => {
                // retry if the compact operation was interrupted by a stepdown before
                // it can replace the ECOC.
                this.encryptedClient.runEncryptionOperation(
                    () => { assert.commandWorked(encryptedColl.compact()); });
                return this.ecocExists(this.encryptedClient, this.encryptedCollName);
            }, 'Unable to complete a full compaction operation');
            this.compactRuns++;
        },
    };

    // 'transitions' defines how the FSM should proceed from its
    // current state to the next state. The value associated with a
    // particular state represents the likelihood of that transition.
    // All state functions should appear as keys within 'transitions'.
    const transitions = {
        init: {insertDocs: 1},
        insertDocs: {insertDocs: 0.4, updateDocs: 0.5, compact: 0.1},
        updateDocs: {
            insertDocs: 0.5,
            updateDocs: 0.4,
            compact: 0.1,
        },
        compact: {insertDocs: 0.5, updateDocs: 0.5},
    };

    function cleanupOnLastIteration(data, db, collName, func) {
        let lastIteration = ++data.iteration >= data.iterations;
        let exception = false;
        try {
            func();
        } catch (e) {
            lastIteration = true;
            exception = true;
            throw e;
        } finally {
            if (lastIteration) {
                print("Thread threw exception: " + exception);
                print("Compact run count: " + tojson(data.compactRuns));
                print("Values inserted (excludes thread-unique): " + tojson(data.valuesInserted));

                // save per-thread metadata so that assertions can be made on teardown
                const encryptedColl = db[data.encryptedCollName];
                let res = encryptedColl.einsert({
                    testData: 1,
                    uniqueValueCount: NumberLong(Object.keys(data.valuesInserted).length),
                    threadUniqueValueCount: NumberLong(data.compactRuns)
                });
                assert.writeOK(res);
            }
        }
    }

    // Wrap each state in a cleanupOnLastIteration() invocation.
    for (let stateName of Object.keys(states)) {
        const stateFn = states[stateName];
        states[stateName] = function(db, collName) {
            cleanupOnLastIteration(this, db, collName, () => stateFn.apply(this, arguments));
        };
    }

    function ESCCount(db, collName, query) {
        const escName = 'enxcol_.' + collName + '.esc';
        const actualCount = db.getCollection(escName).countDocuments(query);
        return actualCount;
    }

    // 'setup' is run once by the parent thread after the cluster has
    // been initialized, but before the worker threads have been spawned.
    // The 'this' argument is bound as '$config.data'. 'cluster' is provided
    // to allow execution against all mongos and mongod nodes.
    function setup(db, collName, cluster) {
        this.encryptedCollName = collName + ".encrypted";
        const eclient = new EncryptedClient(db.getMongo(), db.getName());
        eclient.createEncryptionCollection(
            this.encryptedCollName, {encryptedFields: this.encryptedFields}, this.shardKey);
        db.createCollection(collName);
    }

    // 'teardown' is run once by the parent thread before the cluster
    // is destroyed, but after the worker threads have been reaped.
    // The 'this' argument is bound as '$config.data'. 'cluster' is provided
    // to allow execution against all mongos and mongod nodes.
    function teardown(db, collName, cluster) {
        const eclient = new EncryptedClient(db.getMongo(), db.getName());
        const edb = eclient.getDB();
        const rawDocItr = db[collName].find();
        const ecoll = edb[this.encryptedCollName];

        while (rawDocItr.hasNext()) {
            const rawDoc = rawDocItr.next();
            const query = {count: rawDoc.count, tid: rawDoc.tid};
            const encDocs = eclient.runEncryptionOperation(
                () => { return edb[this.encryptedCollName].find(query).toArray(); });

            assert.eq(encDocs.length, 1);
            assert.eq(encDocs[0].first, rawDoc.first);
            assert.eq(encDocs[0].ssn, rawDoc.ssn);
        }

        const nonAnchorCountBefore =
            ESCCount(edb, this.encryptedCollName, {"value": {"$exists": false}});

        // Run a final compact & make sure the ESC count is correct

        assert.soon(() => {
            eclient.runEncryptionOperation(() => { assert.commandWorked(ecoll.compact()); });
            return this.ecocExists(eclient, this.encryptedCollName);
        }, 'Unable to complete a full compaction operation');

        let res = ecoll.find({testData: 1}).toArray();
        assert.eq(res.length, this.threadCount);

        const sumThreadUniqueValues =
            res.reduce((prev, cur) => prev + cur.threadUniqueValueCount, 0);
        const maxUniqueValues =
            res.reduce((prev, cur) => (prev > cur.uniqueValues ? prev : cur.uniqueValueCount), 0);
        print("thread-unique values: " + sumThreadUniqueValues);
        print("non-thread-unique values: " + maxUniqueValues);

        // After a v2 compaction, only anchors must be present in the ESC.
        // The exact number of anchors is no longer deterministic, but must be
        // greater than or equal to the number of unique values.
        const nonAnchorCount = ESCCount(edb, this.encryptedCollName, {"value": {"$exists": false}});
        assert.lte(nonAnchorCount, nonAnchorCountBefore);
        const anchorCount = ESCCount(edb, this.encryptedCollName, {"value": {"$exists": true}});
        assert.gte(anchorCount, sumThreadUniqueValues + maxUniqueValues);
    }

    return {
        threadCount: 2,
        iterations: 50,
        startState: 'init',
        states: states,
        transitions: transitions,
        setup: setup,
        teardown: teardown,
        data: data
    };
})();
