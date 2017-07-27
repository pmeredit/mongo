// Helper library for testing auditing infrastructure in Mongod

/**
 * Instantiate a MongoRunnn.runMongod()
 * and provide a custom assertion method for
 * tailing the audit log looking for particular
 * atype entries with matching param objects.
 */
MongoRunner.runMongodAuditLogger = function(opts) {
    opts = MongoRunner.mongodOptions(opts);
    opts.auditDestination = opts.auditDestination || "file";
    if (opts.auditDestination == "file") {
        opts.auditPath = opts.auditPath || (opts.dbpath + "/audit.log");
        opts.auditFormat = opts.auditFormat || "JSON";
    }
    mongo = MongoRunner.runMongod(opts);

    mongo.auditSpooler = function() {
        var spooler = function() {};
        spooler.auditFile = mongo.fullOptions.auditPath;
        spooler._auditLine = 0;

        /**
         * Skip forward in the log file to "now"
         * Call this prior to an audit producing command to
         * ensure that old entries don't cause false-positives.
         */
        spooler.fastForward = function() {
            spooler._auditLine = cat(spooler.auditFile).split("\n").length - 1;
        };

        /**
         * Poll the audit logfile for a matching entry
         * beginning with the current line.
         */
        spooler.assertEntry = function(atype, param) {
            assert.soon(function() {
                var log = cat(spooler.auditFile).split("\n").slice(spooler._auditLine);
                for (var idx in log) {
                    var entry = log[idx];
                    try {
                        line = JSON.parse(entry);
                    } catch (e) {
                        continue;
                    }
                    if (line.atype !== atype) {
                        continue;
                    }

                    // Warning: Requires that the subkey/element orders match
                    if (JSON.stringify(line.param) === JSON.stringify(param)) {
                        spooler._auditLine += Number(idx) + 1;
                        return true;
                    }
                }
                return false;
            }, 5 * 1000);
        };
        return spooler;
    };

    return mongo;
};
