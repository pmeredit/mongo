// Helper library for testing auditing infrastructure in Mongod

(function() {
    'use strict';

    /**
     * Parses a JSON filled audit file, and provides an interface for making assertions
     * about the audit events a server emitted into it.
     */
    class AuditSpooler {
        constructor(auditFile) {
            this._auditLine = 0;
            this.auditFile = auditFile;
        }

        /**
         * Return all lines in the audit log.
         */
        getAllLines() {
            return cat(this.auditFile).trim().split("\n");
        }

        /**
         * Skip forward in the log file to "now"
         * Call this prior to an audit producing command to
         * ensure that old entries don't cause false-positives.
         */
        fastForward() {
            this._auditLine = this.getAllLines().length;
        }

        /**
         * Block until an new entry is available and return it.
         */
        getNextEntry() {
            assert.soon(() => {
                return this.getAllLines().length > this._auditLine;
            }, "audit logfile should contain entry within default timeout");
            const line = this.getAllLines()[this._auditLine];
            this._auditLine += 1;
            return JSON.parse(line);
        }

        /**
         * Poll the audit logfile for a matching entry
         * beginning with the current line.
         */
        assertEntry(atype, param) {
            assert.soon(
                () => {
                    const log = this.getAllLines().slice(this._auditLine);
                    for (var idx in log) {
                        const entry = log[idx];
                        try {
                            var line = JSON.parse(entry);
                        } catch (e) {
                            continue;
                        }
                        if (line.atype !== atype) {
                            continue;
                        }

                        // Warning: Requires that the subkey/element orders match
                        if (JSON.stringify(line.param) === JSON.stringify(param)) {
                            this._auditLine += Number(idx) + 1;
                            return true;
                        }
                    }
                    return false;
                },
                "audit logfile should contain entry within default timeout.\n" +
                    "Search started on line number: " + this._auditLine + "\n" +
                    "Log File Contents\n==============================\n" + this.getAllLines() +
                    "\n==============================\n");
        }

        /**
         * Poll the audit logfile for a matching entry
         * beginning with the current line.
         * This method returns true if an entry in the audit file exists after "now" which has
         * the desired atype, and if every field in param is equal to a matching field in the entry.
         */
        assertEntryRelaxed(atype, param) {
            assert.soon(
                () => {
                    const log = this.getAllLines().slice(this._auditLine);
                    for (let idx in log) {
                        const entry = log[idx];
                        let line;
                        try {
                            line = JSON.parse(entry);
                        } catch (e) {
                            continue;
                        }
                        if (line.atype !== atype) {
                            continue;
                        }

                        let deepPartialEquals = function(target, source) {
                            print("Checking '" + JSON.stringify(target) + "' vs '" +
                                  JSON.stringify(source) + "'");
                            for (let property in source) {
                                if (source.hasOwnProperty(property)) {
                                    print(property);
                                    if (target[property] === undefined) {
                                        print("Target missing property: " + property);
                                        return false;
                                    }

                                    if (typeof source[property] !== "object") {
                                        const res = source[property] === target[property];
                                        if (!res) {
                                            print(property + " not equal. " + source[property] +
                                                  " != " + target[property]);
                                            return false;
                                        }
                                    } else {
                                        if (!deepPartialEquals(target[property],
                                                               source[property])) {
                                            return false;
                                        }
                                    }
                                }
                            }
                            return true;
                        };

                        if (deepPartialEquals(line.param, param)) {
                            this._auditLine += Number(idx) + 1;
                            return true;
                        }
                    }
                    return false;
                },
                "audit logfile should contain entry within default timeout.\n" +
                    "Search started on line number: " + this._auditLine + "\n" +
                    "Log File Contents\n==============================\n" + this.getAllLines() +
                    "\n==============================\n");
        }

        /**
         * Poll the audit logfile for a matching entry
         * beginning with the current line.
         * This method returns true if an entry in the audit file exists after "now" which has
         * the desired atype, and if every field in param is equal to a matching field in the entry.
         */
        assertEntryRelaxed(atype, param) {
            assert.soon(
                () => {
                    const log = this.getAllLines().slice(this._auditLine);
                    for (let idx in log) {
                        const entry = log[idx];
                        let line;
                        try {
                            line = JSON.parse(entry);
                        } catch (e) {
                            continue;
                        }
                        if (line.atype !== atype) {
                            continue;
                        }

                        let deepPartialEquals = function(target, source) {
                            print("Checking '" + JSON.stringify(target) + "' vs '" +
                                  JSON.stringify(source) + "'");
                            for (let property in source) {
                                if (source.hasOwnProperty(property)) {
                                    if (target[property] === undefined) {
                                        print("Target missing property: " + property);
                                        return false;
                                    }

                                    if (typeof source[property] !== "object") {
                                        const res = source[property] === target[property];
                                        if (!res) {
                                            print(property + " not equal. " + source[property] +
                                                  " != " + target[property]);
                                            return false;
                                        }
                                    } else {
                                        if (!deepPartialEquals(target[property],
                                                               source[property])) {
                                            return false;
                                        }
                                    }
                                }
                            }
                            return true;
                        };

                        if (deepPartialEquals(line.param, param)) {
                            this._auditLine += Number(idx) + 1;
                            return true;
                        }
                    }
                    return false;
                },
                "audit logfile should contain entry within default timeout.\n" +
                    "Search started on line number: " + this._auditLine + "\n" +
                    "Log File Contents\n==============================\n" + this.getAllLines() +
                    "\n==============================\n");
        }

        /**
         * Assert that no new audit events have been emitted, since the last event observed
         * via the Spooler.
         */
        assertNoNewEntries() {
            const log = this.getAllLines().slice(this._auditLine);
            assert.eq(log.length, 0, "Log contained new entries: " + tojson(log));
        }
    }

    /**
     * Instantiate a variant of MongoRunnner.runMongod(), which provides a custom assertion method
     * for tailing the audit log looking for particular atype entries with matching param objects.
     */
    MongoRunner.runMongodAuditLogger = function(opts) {
        opts = MongoRunner.mongodOptions(opts);
        opts.auditDestination = opts.auditDestination || "file";
        if (opts.auditDestination === "file") {
            opts.auditPath = opts.auditPath || (opts.dbpath + "/audit.log");
            opts.auditFormat = opts.auditFormat || "JSON";
        }
        let mongo = MongoRunner.runMongod(opts);

        /**
         * Produce a new Spooler object, which can be used to access the audit log events emitted
         * by the spawned mongod.
         */
        mongo.auditSpooler = function() {
            return new AuditSpooler(mongo.fullOptions.auditPath);
        };

        return mongo;
    };

})();
