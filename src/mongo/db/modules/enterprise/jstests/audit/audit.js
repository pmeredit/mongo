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
         * Skip forward in the log file to "now"
         * Call this prior to an audit producing command to
         * ensure that old entries don't cause false-positives.
         */
        fastForward() {
            this._auditLine = cat(this.auditFile).split("\n").length - 1;
        }

        /**
         * Poll the audit logfile for a matching entry
         * beginning with the current line.
         */
        assertEntry(atype, param) {
            assert.soon(() => {
                const log = cat(this.auditFile).split("\n").slice(this._auditLine);
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
            }, "audit logfile should contain entry within default timeout");
        }

        /**
         * Assert that no new audit events have been emitted, since the last event observed
         * via the Spooler.
         */
        assertNoNewEntries() {
            const log = cat(this.auditFile).split("\n").slice(this._auditLine);
            assert.eq(tojson([""]), tojson(log), "Log contained new entries: " + tojson(log));
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
