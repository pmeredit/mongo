// Helper library for testing auditing infrastructure in Mongod

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

/**
 * Parses a JSON filled audit file, and provides an interface for making assertions
 * about the audit events a server emitted into it.
 */
export class AuditSpooler {
    constructor(auditFile, format = "JSON") {
        this._auditLine = 0;
        this.schema = 'mongo';
        this.format = format;
        this.auditFile = auditFile;
    }

    /**
     * Return all lines in the audit log.
     */
    getAllLines() {
        if (this.format === "BSON") {
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
     * Prints all new lines in the audit log without advancing pointer
     * WARNING: this function does not wait, use it for debugging only
     */
    printAllNewLines() {
        const lines = this.getAllLines();
        for (let it = this._auditLine; it < lines.length; ++it) {
            print(lines[it]);
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
        assert.soon(() => { return this.getAllLines().length > this._auditLine; },
                    "audit logfile should contain entry within default timeout");
        const line = this.getAllLines()[this._auditLine];
        this._auditLine += 1;
        return JSON.parse(line);
    }

    /**
     * Run a function without moving the auditline position in the log.
     */
    noAdvance(func) {
        const pos = this._auditLine;
        const ret = func();
        this._auditLine = pos;
        return ret;
    }

    /**
     * Block until an new entry is available and return it without parsing it.
     */
    getNextEntryNoParsing() {
        assert.soon(() => { return this.getAllLines().length > this._auditLine; },
                    "audit logfile should contain entry within default timeout");
        const line = this.getAllLines()[this._auditLine];
        this._auditLine += 1;
        return line;
    }

    assertNoEntry(atype, param) {
        assert(!this.findEntry(this.getAllLines(), atype, param), this._makeErrorMessage());
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
            () => { return this._makeErrorMessage(); });

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
    assertEntryRelaxed(atype, param, timeout, interval, opts) {
        let line;
        assert.soon(() => {
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

                if (this.deepPartialEquals(line.param, param)) {
                    this._auditLine += Number(idx) + 1;
                    return true;
                }
            }
            return false;
        }, () => this._makeErrorMessage(), timeout, interval, opts);

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
                    !this.deepPartialEquals(parsedLine.param, paramPartial)) {
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
                        if (this.deepPartialEquals(line.param.options, options)) {
                            return true;
                        }
                    }
                }
                return false;
            },
            () => { return this._makeErrorMessage(); });

        // Success if we got here, return the matched record.
        return line;
    }

    /**
     * Assert that no new audit events matching an optional filter have been emitted
     * since the last event observed via the Spooler.
     *
     * By using this.deepPartialEquals, we get scalar comparison for free and any object
     * comparisons are implicitly performed using a partial equality match.
     */
    assertNoNewEntriesMatching(filter = {}) {
        // We want lines to return false. True means there is a match which we do not want.
        const log = this.getAllLines().slice(this._auditLine).filter(function(line) {
            const parsedLine = function() {
                try {
                    return JSON.parse(line);
                } catch (err) {
                    jsTest.log("Failed to parse line as JSON: " + line);
                    throw err;
                }
            }();

            return Object.keys(filter).every(
                (key) => this.deepPartialEquals(parsedLine[key], filter[key]));
        }, this);
        assert.eq(log.length, 0, "Log contained new entries: " + tojson(log));
    }

    /**
     * Legacy wrapper for assertNoNewEntriesMatching.
     * Maps separate parameters for `atype` and `param`.
     */
    assertNoNewEntries(atype = undefined, paramPartial = undefined) {
        const filter = {};
        if (atype !== undefined) {
            assert.eq(typeof atype, 'string');
            filter.atype = atype;
        }
        if (paramPartial !== undefined) {
            filter.param = paramPartial;
        }
        return this.assertNoNewEntriesMatching(filter);
    }

    _makeErrorMessage() {
        let msg = "audit logfile should contain entry within default timeout.\n" +
            "Search started on line number: " + this._auditLine + "\n" +
            "Log File Contents\n==============================\n";

        const log = this.getAllLines().slice(this._auditLine);

        for (const idx in log) {
            msg += log[idx] + "\n";
        }

        msg += "==============================\n";

        return msg;
    }

    // Extended JSON encodes longs (and sometimes other types) using objects.
    // This makes simple numeric comparison.... tricky.
    // Cast these types to Numbers which can be cleanly compared by Javascript.
    relaxJSON(input) {
        if ((typeof input === "object") && (Object.keys(input).length === 1)) {
            const kNumericKeys = ['$numberInt', '$numberLong', '$numberDouble', '$numberDecimal'];
            for (let typ of kNumericKeys) {
                if (input.hasOwnProperty(typ)) {
                    try {
                        return Number(input[typ]);
                    } catch (e) {
                        print(`Could not parse ${input[typ]} as a number`);
                        // Ignore failure to parse.
                    }
                }
            }
        }
        return input;
    }

    deepPartialEquals(target, source) {
        print("Checking '" + JSON.stringify(target) + "' vs '" + JSON.stringify(source) + "'");
        for (let property in source) {
            function debug(msg) {
                print(
                    msg + ': ' +
                    tojson(
                        {property: property, source: source[property], target: target[property]}));
            }

            if (source.hasOwnProperty(property)) {
                if (target[property] === undefined) {
                    print("Target missing property: " + property);
                    return false;
                }

                const sourceProp = this.relaxJSON(source[property]);
                const targetProp = this.relaxJSON(target[property]);

                if (typeof sourceProp === "function") {
                    /* { foo: (v) => (v > 5) } */
                    const res = sourceProp(targetProp);
                    if (!res) {
                        debug('Callback not satisfied');
                        return false;
                    }
                } else if (Array.isArray(sourceProp)) {
                    /* { foo: ['bar', 'baz', ...] } */
                    // Assert that all of the elements in source satisfy deepPartialEquals for at
                    // least one element in target.
                    if (!Array.isArray(targetProp)) {
                        debug('Target property is not an array');
                        return false;
                    }

                    const res = sourceProp.every(
                        (srcElem) => targetProp.some(
                            (targetElem) => this.deepPartialEquals(targetElem, srcElem)));
                    if (!res) {
                        debug('Source array contain element(s) not found in target');
                        return false;
                    }
                } else if (typeof sourceProp !== "object") {
                    /* { foo: 'bar' } */
                    const res = sourceProp === targetProp;
                    if (!res) {
                        debug('Targets are not trivially equal');
                        return false;
                    }
                } else if (sourceProp instanceof RegExp) {
                    /* { foo: /bar.*baz/ } */
                    const res = sourceProp.test(targetProp);
                    if (!res) {
                        debug('Regex pattern ' + sourceProp.toString() +
                              ' not satisfied by target');
                        return false;
                    }
                } else {
                    /* { foo: {bar: 'baz'} } */
                    if (!this.deepPartialEquals(targetProp, sourceProp)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}

export class AuditSpoolerOCSF extends AuditSpooler {
    constructor(auditFile, format = "JSON") {
        super(auditFile, format);
        this.schema = 'ocsf';
    }

    /**
     * Poll the audit logfile for a matching entry
     * beginning with the current line.
     */
    assertEntry(eventCategory, eventClass, activityId) {
        let line;
        assert.soon(
            () => {
                const log = this.getAllLines().slice(this._auditLine);
                line = this.findEntry(log, eventCategory, eventClass, activityId);
                return null !== line;
            },
            () => { return this._makeErrorMessage(); });

        // Success if we got here, return the matched record.
        return line;
    }

    /**
     * Checks to be sure no entries matching the Ids provided has been generated.
     */
    assertNoEntry(eventCategory, eventClass, activityId) {
        assert(this.findEntry(this.getAllLines().slice(this._auditLine),
                              eventCategory,
                              eventClass,
                              activityId) === null);
    }

    /**
     *  This function is called by the functions above.
     *
     *  It takes a list of log lines, an audit type, and a search parameter and searches the log
     *  lines for the actionType / param combination. Returns the line if found, else returns null.
     */
    findEntry(log, eventCategory, eventClass, activityId) {
        let line;
        for (var idx in log) {
            const entry = log[idx];
            try {
                line = JSON.parse(entry);
            } catch (e) {
                continue;
            }
            if (line.category_uid !== eventCategory || line.class_uid !== eventClass ||
                line.activity_id !== activityId) {
                jsTest.log(line);
                continue;
            }

            this._auditLine += Number(idx) + 1;
            return line;
        }
        return null;
    }

    /**
     *  This function is called by the functions above.
     *
     *  It takes a list of log lines, an audit type, and a search parameter and searches the log
     *  lines for the actionType / param combination. Returns the line if found, else returns null.
     */
    findAllEntries(eventCategory, eventClass, activityId) {
        let line;
        let return_set = [];

        const log = this.getAllLines().slice(this._auditLine);

        for (var idx in log) {
            const entry = log[idx];
            try {
                line = JSON.parse(entry);
            } catch (e) {
                continue;
            }
            if (line.category_uid !== eventCategory || line.class_uid !== eventClass ||
                line.activity_id !== activityId) {
                continue;
            }

            return_set.push(line);
        }
        return return_set;
    }
}

export class StandaloneFixture {
    constructor() {
        this.logPathMongod = MongoRunner.dataPath + "mongod.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess(opts = {}, format = "JSON", schema = "mongo") {
        this.opts = {
            logpath: this.logPathMongod,
            auth: "",
            setParameter: {},
            auditPath: this.auditPath,
        };
        if (!opts.auditRuntimeConfiguration) {
            this.opts.setParameter.auditAuthorizationSuccess = "true";
        }
        this.opts = mergeDeepObjects(this.opts, opts);

        const conn = MongoRunner.runMongodAuditLogger(this.opts, format, schema);

        this.audit = conn.auditSpooler();
        this.admin = conn.getDB("admin");
        this.format = format;
        this.schema = schema;

        this.conn = conn;
        this.logPath = this.conn.fullOptions.logpath;
        return {"conn": this.conn, "audit": this.audit, "admin": this.admin};
    }

    createUserAndAuth() {
        assert.commandWorked(this.admin.runCommand(
            {createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

        assert(this.admin.auth({user: "user1", pwd: "pwd"}));

        assert.commandWorked(this.admin.runCommand(
            {createUser: "user2", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
    }

    restartMainFixtureProcess(conn, options, uncleanStop) {
        jsTest.log('Restarting standalone fixture');
        if (uncleanStop) {
            MongoRunner.stopMongod(conn, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL});
        } else {
            MongoRunner.stopMongod(conn);
        }

        this.conn = MongoRunner.runMongod(options);
    }

    stopProcess() {
        MongoRunner.stopMongod(this.conn);
    }
}

// Used to test mongos in a sharded cluster
export class ShardingFixture {
    constructor() {
        this.logPathMongos = MongoRunner.dataPath + "mongos.log";
        this.auditPath = MongoRunner.dataPath + "audit.log";
    }

    startProcess(opts = {}, format = "JSON", schema = "mongo") {
        this.opts = {
            mongos: 1,
            config: 1,
            shards: 1,
            other: {
                mongosOptions: {
                    logpath: this.logPathMongos,
                    auth: null,
                    setParameter: {auditAuthorizationSuccess: "true"},
                    auditPath: this.auditPath,
                    auditDestination: "file",
                    auditFormat: "JSON",
                },
            }
        };
        this.opts = mergeDeepObjects(this.opts, opts);

        const st = MongoRunner.runShardedClusterAuditLogger(this.opts, {}, format, schema);

        this.audit = st.s0.auditSpooler();
        this.admin = st.s0.getDB("admin");
        this.format = format;
        this.schema = schema;

        this.st = st;
        this.logPath = this.st.s0.fullOptions.logpath;
        return {"conn": this.st.s0, "audit": this.audit, "admin": this.admin};
    }

    createUserAndAuth() {
        assert.commandWorked(this.admin.runCommand(
            {createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

        assert(this.admin.auth({user: "user1", pwd: "pwd"}));

        assert.commandWorked(this.admin.runCommand(
            {createUser: "user2", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
    }

    restartMainFixtureProcess(conn, options, uncleanStop) {
        jsTest.log('Restarting sharding');
        Object.keys(this.st._mongos).forEach((n) => this.st.restartMongos(n, options));
        this.conn = this.st.s0;
    }

    stopProcess() {
        this.st.stop();
    }
}

// Checks the logPath defined above for the specific ID. Does not use any system logs or joint
// logs.
export function ContainsLogWithId(id, fixture) {
    const logPath = fixture.logPath;
    return cat(logPath).trim().split("\n").some((line) => JSON.parse(line).id === id);
}

export function makeAuditOpts(sourceOpts, format = 'JSON', schema = 'mongo') {
    assert(sourceOpts.auditDestination === undefined || sourceOpts.auditDestination === "file",
           '"auditDestination" must either be unset or "file"');
    assert(schema == 'mongo' || schema == 'ocsf',
           '"schema" must be either "mongo" or "ocsf", got: ' + tojson(schema));
    assert(schema != 'ocsf' || format == 'JSON',
           '"format" must be "JSON" for schema "ocsf", got: ' + tojson(format));

    let auditOpts = {auditDestination: "file"};

    // It would be lovely if we could pre-assign a port and use it here. Sadly, we cannot because
    // ReplSetTest does not allow for pre-assigned ports.

    auditOpts.auditPath = `${MongoRunner.dataPath}audit-logs/mongodb-${UUID().hex()}-audit.log`;

    auditOpts.auditSchema = (schema == 'mongo') ? 'mongo' : 'OCSF';
    auditOpts.auditFormat = format;

    // Extend our local object with the properties from sourceOpts.
    return Object.extend(auditOpts, sourceOpts, /* deep = */ true);
}

export function makeReplSetAuditOpts(opts = {}, format = "JSON", schema = "mongo") {
    const setOpts = Object.assign({nodes: 1}, opts);
    if (Number.isInteger(setOpts.nodes)) {
        // e.g. {nodes: 3}
        const numNodes = setOpts.nodes;
        setOpts.nodes = [];
        for (let i = 0; i < numNodes; ++i) {
            setOpts.nodes[i] = makeAuditOpts({}, format, schema);
        }
    } else if ((typeof setOpts.nodes) == 'object') {
        // e.g. {nodes: { options for single node } }
        setOpts.nodes = makeAuditOpts(setOpts.nodes, format, schema);
    } else if (Array.isArray(setOpts.nodes)) {
        // e.g. {nodes: [{node1Opts}, {node2Opts}, ...]}
        setOpts.nodes = setOpts.nodes.map((node) => makeAuditOpts(node, format, schema));
    }
    return setOpts;
}

/**
 * Performs a deep merge of objects and returns a new object. Does not modify
 * objects (immutable) and merges arrays via concatenation.
 */
export function mergeDeepObjects(...objects) {
    const isObject = obj => obj && typeof obj === 'object';

    return objects.reduce((prev, obj) => {
        Object.keys(obj).forEach(key => {
            const pVal = prev[key];
            const oVal = obj[key];

            if (Array.isArray(pVal) && Array.isArray(oVal)) {
                prev[key] = pVal.concat(...oVal);
            } else if (isObject(pVal) && isObject(oVal)) {
                prev[key] = mergeDeepObjects(pVal, oVal);
            } else {
                prev[key] = oVal;
            }
        });

        return prev;
    }, {});
}

/**
 * Instantiate a variant of MongoRunnner.runMongod(), which provides a custom assertion method
 * for tailing the audit log looking for particular atype entries with matching param objects.
 */
MongoRunner.runMongodAuditLogger = function(opts, format = "JSON", schema = "mongo") {
    let mongo = MongoRunner.runMongod(makeAuditOpts(opts, format, schema));

    /**
     * Produce a new Spooler object, which can be used to access the audit log events emitted
     * by the spawned mongod.
     */
    mongo.auditSpooler = function() {
        if (schema === "mongo") {
            return new AuditSpooler(mongo.fullOptions.auditPath, format);
        } else {
            return new AuditSpoolerOCSF(mongo.fullOptions.auditPath, format);
        }
    };

    return mongo;
};

ReplSetTest.runReplSetAuditLogger = function(
    opts = {}, format = "JSON", schema = "mongo", expectPrimaryChange = false) {
    const rsOpts = makeReplSetAuditOpts(opts, format, schema);
    const rs = new ReplSetTest(rsOpts);
    rs.startSet();
    if (expectPrimaryChange) {
        rs.initiate(null, null, {initiateWithDefaultElectionTimeout: true});
    } else {
        rs.initiate();
    }
    rs.nodes.forEach(function(node) {
        if (!schema) {
            node.auditSpooler = (() => new AuditSpooler(node.fullOptions.auditPath, format));
        } else {
            node.auditSpooler = (() => new AuditSpoolerOCSF(node.fullOptions.auditPath, format));
        }
    });
    return rs;
};

/**
 * Instantiate a variant of new ShardingTest(), which provides a custom assertion method
 * for tailing the audit log looking for particular atype entries with matching param objects.
 *
 * To use this, specify the number (n) of mongos, config servers, and shards by using the format:
 * {mongos: n, config: n, shards: n} and specifying the options for each of these members using
 * the format: {other: {configOptions: ..., mongosOptions: ..., ... }}. The
 * defaults are defined at the top of the function.
 *
 * The baseOptions param is for things common for auditing such as  auditAuthorizationSuccess or
 * an audit filter, or for things like auth enabled.
 *
 * Returns the st object with an audit logger attached to all shards (and members of the replica
 * set for a single shard), mongos', and config servers.
 */
MongoRunner.runShardedClusterAuditLogger = function(
    opts = {}, baseOptions = {}, format = "JSON", schema = "mongo", expectPrimaryChange = false) {
    const defaultOpts = {
        mongos: [makeAuditOpts(baseOptions, format, schema)],
        config: [makeAuditOpts(baseOptions, format, schema)],
        shards: 1,
        rs0: makeReplSetAuditOpts(baseOptions, format, schema),
        initiateWithDefaultElectionTimeout: expectPrimaryChange
    };

    // Beware! This does not do a nested merge, so if your provided opts has an "other" field, it
    // will completely override defaultOpts.other.
    const clusterOptions = Object.merge(defaultOpts, opts);

    assert(clusterOptions.shards === 1,
           'only 1 shards is supported but found shard option: ' +
               tojsononeline(clusterOptions.shards));

    const st = new ShardingTest(clusterOptions);

    let attachAuditSpool = function(conn) {
        conn.auditSpooler = function() {
            if (conn.fullOptions.auditPath) {
                if (schema === "mongo") {
                    return new AuditSpooler(conn.fullOptions.auditPath, format);
                } else {
                    return new AuditSpoolerOCSF(conn.fullOptions.auditPath, format);
                }
            }
            return null;
        };
    };

    st.forEachConnection((conn) => conn.rs.nodes.forEach(attachAuditSpool));
    st.forEachMongos(attachAuditSpool);
    st.forEachConfigServer(attachAuditSpool);

    return st;
};
