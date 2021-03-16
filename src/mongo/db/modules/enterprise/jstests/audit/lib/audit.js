// Helper library for testing auditing infrastructure in Mongod

'use strict';

/**
 * Parses a JSON filled audit file, and provides an interface for making assertions
 * about the audit events a server emitted into it.
 */
class AuditSpooler {
    constructor(auditFile, isBSON = false) {
        this._auditLine = 0;
        this._isBSON = isBSON;
        this.auditFile = auditFile;
    }

    /**
     * Return all lines in the audit log.
     */
    getAllLines() {
        if (this._isBSON) {
            const auditFileContents = _readDumpFile(this.auditFile);
            return auditFileContents.map(JSON.stringify);
        } else {
            const lines = cat(this.auditFile).trim().split("\n");

            // When reading an empty file, cat reads in a single newline
            // which throws off the audit spooler when trying to fast forward.
            if (lines.length === 1 && lines[0] === "") {
                return [];
            }
            return lines;
        }
    }

    /**
     * Reset the AuditSpooler. Useful for rotating logs.
     */
    resetAuditLine() {
        this._auditLine = 0;
    }

    /**
     * Get the current audit line. Useful for rewinding a specified
     * amount.
     */
    getCurrentAuditLine() {
        return this._auditLine;
    }

    /**
     * Set the audit line to some specific value. Best to use with
     * getCurrentAuditLine().
     */
    setCurrentAuditLine(lineNum) {
        this._auditLine = lineNum;
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

    assertNoEntry(atype, param) {
        assert(!this.findEntry(this.getAllLines(), atype, param));
    }

    /**
     * Poll the audit logfile for a matching entry
     * beginning with the current line.
     */
    assertEntry(atype, param) {
        let line;
        assert.soon(
            () => {
                const log = this.getAllLines().slice(this._auditLine);
                line = this.findEntry(log, atype, param);
                return null !== line;
            },
            "audit logfile should contain entry within default timeout.\n" +
                "Search started on line number: " + this._auditLine + "\n" +
                "Log File Contents\n==============================\n" + this.getAllLines() +
                "\n==============================\n");

        // Success if we got here, return the matched record.
        return line;
    }

    /**
     *  This function is called by the functions above.
     *
     *  It takes a list of log lines, an audit type, and a search parameter and searches the log
     *  lines for the actionType / param combination. Returns the line if found, else returns null.
     */
    findEntry(log, atype, param) {
        let line;
        for (var idx in log) {
            const entry = log[idx];
            try {
                line = JSON.parse(entry);
            } catch (e) {
                continue;
            }
            if (line.atype !== atype) {
                continue;
            }

            // Warning: Requires that the subkey/element orders match
            if (param === undefined || JSON.stringify(line.param) === JSON.stringify(param)) {
                this._auditLine += Number(idx) + 1;
                return line;
            }
        }
        return null;
    }

    /**
     * Poll the audit logfile for a matching entry
     * beginning with the current line.
     * This method returns true if an entry in the audit file exists after "now" which has
     * the desired atype, and if every field in param is equal to a matching field in the entry.
     */
    assertEntryRelaxed(atype, param) {
        let line;
        assert.soon(
            () => {
                const log = this.getAllLines().slice(this._auditLine);
                for (let idx in log) {
                    const entry = log[idx];
                    try {
                        line = JSON.parse(entry);
                    } catch (e) {
                        continue;
                    }
                    if (line.atype !== atype) {
                        continue;
                    }

                    if (this._deepPartialEquals(line.param, param)) {
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

        // Success if we got here, return the matched record.
        return line;
    }

    /**
     * Assert that all entries in the audit log of action type @param atype have params containing
     * all of the fields and their corresponding values as specified in @param paramPartial.
     */

    assertAllAtypeEntriesRelaxed(atypes, paramPartial) {
        // If only one atype was provided as a string, convert it into an array.
        if (typeof atypes === 'string') {
            atypes = [atypes];
        }
        atypes.forEach((atype) => {
            // We want log to only contain the lines in the file that match atype but don't match
            // paramPartial.
            const log = this.getAllLines().filter(function(line) {
                let parsedLine = JSON.parse(line);
                if (atype === parsedLine.atype &&
                    !this._deepPartialEquals(parsedLine.param, paramPartial)) {
                    return true;
                }
                return false;
            }, this);
            assert.eq(log.length,
                      0,
                      "Log contained entries of atype " + atype + " whose params did not match " +
                          tojson(paramPartial) + " : " + tojson(log));
        });
    }

    /**
     * Poll the audit logfile for a matching entry beginning with the current line.
     * This method returns true if an entry in the audit file exists after "now" which has
     * the desired atype, and if every field in param.options is equal to a matching field
     * in the entry.
     */
    assertEntryOptionsRelaxed(atype, options) {
        let line;
        assert.soon(
            () => {
                const log = this.getAllLines().slice(this._auditLine);
                for (let idx in log) {
                    const entry = log[idx];
                    try {
                        line = JSON.parse(entry);
                    } catch (e) {
                        continue;
                    }
                    if (line.atype !== atype) {
                        continue;
                    }

                    if (line.param && line.param.options) {
                        if (this._deepPartialEquals(line.param.options, options)) {
                            return true;
                        }
                    }
                }
                return false;
            },
            "audit logfile should contain entry within default timeout.\n" +
                "Search started on line number: " + this._auditLine + "\n" +
                "Log File Contents\n==============================\n" + this.getAllLines() +
                "\n==============================\n");

        // Success if we got here, return the matched record.
        return line;
    }

    /**
     * Assert that no new audit events, optionally of a give type, have been emitted
     * since the last event observed via the Spooler.
     *
     * paramPartial is a partial match on the param field of the audit log. If the param field
     * in the log line contains all the keys from paramPartial and the values match the values
     * from param partial, then the log line fits the search and will be considered a new entry.
     */
    assertNoNewEntries(atype = undefined, paramPartial = undefined) {
        // We want lines to return false. True means there is a match which we do not want.
        const log = this.getAllLines().slice(this._auditLine).filter(function(line) {
            let parsedLine = JSON.parse(line);
            if (atype !== undefined && parsedLine.atype !== atype) {
                return false;
            }
            if (paramPartial !== undefined) {
                if (!this._deepPartialEquals(parsedLine.param, paramPartial)) {
                    return false;
                }
            }
            return true;
        }, this);
        assert.eq(log.length, 0, "Log contained new entries: " + tojson(log));
    }

    _deepPartialEquals(target, source) {
        print("Checking '" + JSON.stringify(target) + "' vs '" + JSON.stringify(source) + "'");
        for (let property in source) {
            if (source.hasOwnProperty(property)) {
                if (target[property] === undefined) {
                    print("Target missing property: " + property);
                    return false;
                }

                if (typeof source[property] === "function") {
                    /* { foo: (v) => (v > 5) } */
                    const res = source[property](target[property]);
                    if (!res) {
                        print(property + " does not satisfy callback: " + tojson(source[property]) +
                              "(" + tojson(target[property]) + ") == false");
                        return false;
                    }
                } else if (typeof source[property] !== "object") {
                    /* { foo: 'bar' } */
                    const res = source[property] === target[property];
                    if (!res) {
                        print(property + " not equal. " + source[property] +
                              " != " + target[property]);
                        return false;
                    }
                } else if (source[property] instanceof RegExp) {
                    /* { foo: /bar.*baz/ } */
                    const res = source[property].test(target[property]);
                    if (!res) {
                        print(property + " does not match pattern " + source[property].toString() +
                              " != " + target[property]);
                        return false;
                    }
                } else {
                    /* { foo: {bar: 'baz'} } */
                    if (!this._deepPartialEquals(target[property], source[property])) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}

function formatAuditOpts(opts, isBSON) {
    opts.auditDestination = opts.auditDestination || "file";
    if (opts.auditDestination === "file") {
        if (!opts.auditPath) {
            opts.auditPath =
                opts.auditPath || MongoRunner.dataPath + `mongodb-${opts.port}-audit.log`;
        }
        if (isBSON) {
            opts.auditFormat = opts.auditFormat || "BSON";
        } else {
            opts.auditFormat = opts.auditFormat || "JSON";
        }
    }

    return opts;
}

/**
 * Instantiate a variant of MongoRunnner.runMongod(), which provides a custom assertion method
 * for tailing the audit log looking for particular atype entries with matching param objects.
 */
MongoRunner.runMongodAuditLogger = function(opts, isBSON = false) {
    opts = formatAuditOpts(opts, isBSON);
    let mongo = MongoRunner.runMongod(opts);

    /**
     * Produce a new Spooler object, which can be used to access the audit log events emitted
     * by the spawned mongod.
     */
    mongo.auditSpooler = function() {
        return new AuditSpooler(mongo.fullOptions.auditPath, isBSON);
    };

    return mongo;
};

/**
 * Instantiate a variant of new ShardingTest(), which provides a custom assertion method
 * for tailing the audit log looking for particular atype entries with matching param objects.
 *
 * To use this, specify the number (n) of mongos, config servers, and shards by using the format:
 * {mongos: n, config: n, shards: n} and specifying the options for each of these members using
 * the format: {other: {configOptions: ..., shardOptions: ..., mongosOptions: ..., ... }}. The
 * defaults are defined at the top of the function.
 *
 * The baseOptions param is for things common for auditing such as  auditAuthorizationSuccess or
 * an audit filter, or for things like auth enabled.
 *
 * Returns the st object with an audit logger attached to all shards (and members of the replica
 * set for a single shard), mongos', and config servers.
 */
MongoRunner.runShardedClusterAuditLogger = function(opts = {}, baseOptions = {}, isBSON = false) {
    // We call mongo options for the mongos to allocate a port.
    let nestedOpts = formatAuditOpts(baseOptions, isBSON);

    const defaultOpts = {
        mongos: 1,
        config: 1,
        shards: 1,
        other: {
            mongosOptions: nestedOpts,
            configOptions: nestedOpts,
            shardOptions: nestedOpts,
        }
    };

    // Beware! This does not do a nested merge, so if your provided opts has an "other" field, it
    // will completely override defaultOpts.other.
    let clusterOptions = Object.merge(defaultOpts, opts);

    const st = new ShardingTest(clusterOptions);

    let attachAuditSpool = function(conn) {
        conn.auditSpooler = function() {
            if (conn.fullOptions.auditPath) {
                return new AuditSpooler(conn.fullOptions.auditPath, isBSON);
            }
            return null;
        };
    };

    for (const i in st._connections) {
        let connection = st._connections[i];
        if (connection.rs && connection.rs.nodes) {
            connection.rs.nodes.forEach(attachAuditSpool);
        } else {
            attachAuditSpool(connection);
        }
    }

    st.forEachMongos(attachAuditSpool);
    st._configServers.forEach(attachAuditSpool);

    return st;
};

function isImprovedAuditingEnabled(m) {
    return assert
        .commandWorked(
            m.getDB('admin').runCommand({getParameter: 1, featureFlagImprovedAuditing: 1}))
        .featureFlagImprovedAuditing.value;
}

function isRuntimeAuditEnabled(m) {
    return assert
        .commandWorked(
            m.getDB('admin').runCommand({getParameter: 1, featureFlagRuntimeAuditConfig: 1}))
        .featureFlagRuntimeAuditConfig.value;
}
