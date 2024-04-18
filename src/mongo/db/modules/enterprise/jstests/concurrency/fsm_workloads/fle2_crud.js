/**
 * fle2_crud.js
 *
 * Performs CRUD operations on both an encrypted and unencrypted collection.
 * Asserts that documents in the unencrypted collection match the
 * documents in the encrypted collection.
 *
 * @tags: [
 * requires_fcv_70,
 * # TODO SERVER-89503 Re-enable this test in suites with balancer once
 * # the relative issue is solved.
 * assumes_balancer_off,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

export const $config = (function() {
    // 'data' is passed (copied) to each of the worker threads.
    var data = {
        encryptedFields: {
            fields: [
                {path: "first", bsonType: "string", queries: {"queryType": "equality"}},
                {path: "ssn", bsonType: "string", queries: {"queryType": "equality"}},
                {path: "pin", bsonType: "int", queries: {"queryType": "equality"}}
            ]
        },
        opIterationsPerState: 5,
        shardKey: {count: "hashed"},
        getRandomDocument: function() {
            const firsts = ["bob", "linda", "louise", "gene", "tina"];
            const ssns = ["123222212", "578862589", "678235522", "123456789", "987654321"];
            const pins = [3423, 4453, 9736];
            function getRandomValue(list) {
                let idx = Math.floor(Math.random() * list.length);
                return list[idx];
            }
            return {
                first: getRandomValue(firsts),
                ssn: getRandomValue(ssns),
                pin: NumberInt(getRandomValue(pins))
            };
        },
        getRandomFindQuery: function(tid) {
            const doc = this.getRandomDocument();
            return {
                $and: [{$or: [{first: doc.first}, {ssn: doc.ssn}, {pin: doc.pin}]}, {tid: tid}]
            };
        }
    };

    // 'states' are the different functions callable by a worker
    // thread. The 'this' argument of any exposed function is
    // bound as '$config.data'.
    var states = {
        init: function init(db, collName) {
            this.conflictStats = {
                insert: 0,
                insertAttempts: 0,
                delete: 0,
                deleteAttempts: 0,
                update: 0,
                updateAttempts: 0,
                findAndModify: 0,
                findAndModifyAttempts: 0
            };
            this.iteration = 1;

            this.sessionOpts = db.getSession().getOptions().getRawOpts();
            Object.freeze(this.sessionOpts);

            this.count = 0;
            this.encryptedClient = new EncryptedClient(db.getMongo(), db.getName());

            const encryptedSession = this.encryptedClient.startSession(this.sessionOpts);
            this.edb = encryptedSession.getDatabase(db.getName());
        },

        insertDocs: function insertDocs(db, collName) {
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const insertDoc = this.getRandomDocument();
                insertDoc["tid"] = this.tid;
                insertDoc["count"] = this.count + 1;

                // Insert a document into encrypted collection; retry on write conflict error
                this.conflictStats.insertAttempts++;
                let res = encryptedColl.insert(insertDoc);

                while (res.hasWriteError()) {
                    assert.writeErrorWithCode(res,
                                              ErrorCodes.WriteConflict,
                                              "Insert did not fail with WriteConflict error");
                    this.conflictStats.insert++;
                    this.conflictStats.insertAttempts++;
                    res = encryptedColl.insert(insertDoc);
                }
                assert.writeOK(res);

                // Insert the same document in the unencrypted collection
                res = db[collName].insert(insertDoc);
                assert.writeOK(res);

                ++this.count;
            }
        },
        updateDocs: function updateDocs(db, collName) {
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const indexToUpdate = Math.floor(Math.random() * this.count) + 1;
                const updateDoc = {$set: this.getRandomDocument()};
                const queryDoc = {count: indexToUpdate, tid: this.tid};

                // Update a document; retry on write conflict error
                this.conflictStats.updateAttempts++;
                let res = encryptedColl.update(queryDoc, updateDoc);

                while (res.hasWriteError()) {
                    assert.writeErrorWithCode(res,
                                              ErrorCodes.WriteConflict,
                                              "Update did not fail with WriteConflict error");
                    this.conflictStats.update++;
                    this.conflictStats.updateAttempts++;
                    res = encryptedColl.update(queryDoc, updateDoc);
                }
                assert.writeOK(res);
                if (res.nModified === 0) {
                    // indexToUpdate was deleted
                    continue;
                }

                // Update same document in unencrypted collection
                res = db[collName].update(queryDoc, updateDoc);
                assert.commandWorked(res);
            }
        },
        findAndModifyDocs: function findAndModifyDocs(db, collName) {
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const indexToUpdate = Math.floor(Math.random() * this.count) + 1;
                const updateDoc = {$set: this.getRandomDocument()};
                const queryDoc = {count: indexToUpdate, tid: this.tid};

                this.conflictStats.findAndModifyAttempts++;
                let res = this.edb.runCommand({
                    findAndModify: this.encryptedCollName,
                    query: queryDoc,
                    update: updateDoc,
                    upsert: true
                });

                while (res.ok === 0 ||
                       (res.hasOwnProperty("writeErrors") && res.writeErrors.length > 0)) {
                    assert.commandFailedWithCode(
                        res,
                        ErrorCodes.WriteConflict,
                        "FindAndModify did not fail with WriteConflict error");
                    this.conflictStats.findAndModify++;
                    this.conflictStats.findAndModifyAttempts++;
                    res = this.edb.runCommand({
                        findAndModify: this.encryptedCollName,
                        query: queryDoc,
                        update: updateDoc,
                        upsert: true
                    });
                }
                assert.commandWorked(res);

                res = db.runCommand(
                    {findAndModify: collName, query: queryDoc, update: updateDoc, upsert: true});
                assert.commandWorked(res);
            }
        },
        readDocs: function readDocs(db, collName) {
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const query = this.getRandomFindQuery(this.tid);
                let encryptedRes = null;
                try {
                    encryptedRes = encryptedColl.findOne(query);
                } catch (e) {
                    print("Encrypted find threw exception: " + tojson(e));
                    // NoSuchTransaction or LockTimeout errors indicate high contention
                    // in internal transactions, so back-off and try again later.
                    assert(e.code == ErrorCodes.NoSuchTransaction ||
                           e.code == ErrorCodes.LockTimeout);
                    sleep(1000 + (Math.random() * 10000));
                    continue;
                }

                if (encryptedRes === null) {
                    continue;
                }
                const rawRes = db[collName].findOne({count: encryptedRes.count, tid: this.tid});
                assert(rawRes !== null);
                assert.eq(rawRes.first, encryptedRes.first);
                assert.eq(rawRes.ssn, encryptedRes.ssn);
                assert.eq(rawRes.pin, encryptedRes.pin);
            }
        },
        deleteDocs: function deleteDocs(db, collName) {
            const encryptedColl = this.edb[this.encryptedCollName];
            for (let i = 0; i < this.opIterationsPerState; i++) {
                const indexToDelete = Math.floor(Math.random() * this.count) + 1;
                const queryDoc = {count: indexToDelete, tid: this.tid};

                // Delete a random document
                this.conflictStats.deleteAttempts++;
                let res = this.edb.runCommand(
                    {delete: this.encryptedCollName, deletes: [{q: queryDoc, limit: 1}]});

                while (res.ok === 0 ||
                       (res.hasOwnProperty("writeErrors") && res.writeErrors.length > 0)) {
                    assert.commandFailedWithCode(res,
                                                 ErrorCodes.WriteConflict,
                                                 "Delete did not fail with WriteConflict error");
                    this.conflictStats.delete ++;
                    this.conflictStats.deleteAttempts++;
                    res = this.edb.runCommand(
                        {delete: this.encryptedCollName, deletes: [{q: queryDoc, limit: 1}]});
                }

                assert.commandWorked(res);

                if (res.n === 0) {
                    continue;
                }

                // Delete same document in unencrypted collection
                res = db[collName].deleteOne(queryDoc);
                assert.commandWorked(res);
                assert.eq(1, res.deletedCount);
            }
        },
    };

    // 'transitions' defines how the FSM should proceed from its
    // current state to the next state. The value associated with a
    // particular state represents the likelihood of that transition.
    // All state functions should appear as keys within 'transitions'.
    const transitions = {
        init: {insertDocs: 1},
        insertDocs: {
            insertDocs: 0.2,
            updateDocs: 0.2,
            findAndModifyDocs: 0.2,
            readDocs: 0.2,
            deleteDocs: 0.2
        },
        updateDocs: {
            insertDocs: 0.2,
            updateDocs: 0.2,
            findAndModifyDocs: 0.2,
            readDocs: 0.2,
            deleteDocs: 0.2
        },
        findAndModifyDocs: {
            insertDocs: 0.2,
            updateDocs: 0.2,
            findAndModifyDocs: 0.2,
            readDocs: 0.2,
            deleteDocs: 0.2
        },
        readDocs: {
            insertDocs: 0.2,
            updateDocs: 0.2,
            findAndModifyDocs: 0.2,
            readDocs: 0.2,
            deleteDocs: 0.2
        },
        deleteDocs: {
            insertDocs: 0.2,
            updateDocs: 0.2,
            findAndModifyDocs: 0.2,
            readDocs: 0.2,
            deleteDocs: 0.2
        },
    };

    function cleanupOnLastIteration(data, db, collName, func) {
        let lastIteration = ++data.iteration >= data.iterations;
        try {
            func();
        } catch (e) {
            lastIteration = true;
            throw e;
        } finally {
            if (lastIteration) {
                const pctStr = function(over, under) {
                    return ((100 * over) / under).toFixed(2) + "% (" + over + "/" + under + ")";
                };
                const stats = data.conflictStats;
                const pcts = {
                    insert: pctStr(stats.insert, stats.insertAttempts),
                    delete: pctStr(stats.delete, stats.deleteAttempts),
                    update: pctStr(stats.update, stats.updateAttempts),
                    findAndModify: pctStr(stats.findAndModify, stats.findAndModifyAttempts)
                };
                print("Write conflict stats: " + tojson(pcts));
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

    // 'setup' is run once by the parent thread after the cluster has
    // been initialized, but before the worker threads have been spawned.
    // The 'this' argument is bound as '$config.data'. 'cluster' is provided
    // to allow execution against all mongos and mongod nodes.
    function setup(db, collName, cluster) {
        this.encryptedCollName = collName + ".encrypted";
        const eclient = new EncryptedClient(db.getMongo(), db.getName());
        eclient.createEncryptionCollection(this.encryptedCollName,
                                           {encryptedFields: this.encryptedFields});
        db.createCollection(collName);
    }

    // 'teardown' is run once by the parent thread before the cluster
    // is destroyed, but after the worker threads have been reaped.
    // The 'this' argument is bound as '$config.data'. 'cluster' is provided
    // to allow execution against all mongos and mongod nodes.
    function teardown(db, collName, cluster) {
        const eclient = new EncryptedClient(db.getMongo(), db.getName());
        if (this.sessionOpts == null) {
            this.sessionOpts = {};
        }
        const encryptedSession = eclient.startSession(this.sessionOpts);
        const edb = encryptedSession.getDatabase(db.getName());

        const rawDocItr = db[collName].find();

        if (!rawDocItr.hasNext()) {
            print("Unencrypted collection is empty!");
        }

        while (rawDocItr.hasNext()) {
            const rawDoc = rawDocItr.next();
            const query = {count: rawDoc.count, tid: rawDoc.tid};
            const encDocs = edb[this.encryptedCollName].find(query).toArray();

            let errmsg = () => {
                let msg = "Raw doc does not match the encrypted doc.";
                msg += " Raw Document: " + tojson(rawDoc);
                msg += " Encrypted Document: " + tojson(encDocs[0]);
                return msg;
            };
            assert.gt(
                encDocs.length, 0, "No matching encrypted doc for raw doc: " + tojson(rawDoc));
            assert.eq(encDocs.length,
                      1,
                      "Found extra encrypted docs for raw doc: " + tojson(rawDoc) +
                          " encrypted docs: " + tojson(encDocs));
            assert.eq(encDocs[0].first, rawDoc.first, errmsg);
            assert.eq(encDocs[0].ssn, rawDoc.ssn, errmsg);
            assert.eq(encDocs[0].pin, rawDoc.pin, errmsg);
        }

        const encDocItr = edb[this.encryptedCollName].find();
        if (!encDocItr.hasNext()) {
            print("Encrypted collection is empty!");
        }
        assert.eq(edb[this.encryptedCollName].countDocuments({}), db[collName].countDocuments({}));
    }

    return {
        threadCount: 10,
        iterations: 100,
        startState: 'init',
        states: states,
        transitions: transitions,
        setup: setup,
        teardown: teardown,
        data: data
    };
})();
