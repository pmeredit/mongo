import {FlatCompat} from "@eslint/eslintrc";
import eslint from "@eslint/js";
import js from "@eslint/js";
import {default as mongodb_plugin} from "eslint-plugin-mongodb";
import globals from "globals";
import path from "node:path";
import {fileURLToPath} from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});

export default [
    ...compat
        .extends("eslint:recommended"),
                {
                    ignores: ["src/mongo/gotools/*", "**/*.tpl.js", "jstests/third_party/**/*.js"],
                },
                {
                    languageOptions: {
                        globals: {
                            ...globals.mongo,
                            TestData: true,
                            WriteError: true,
                            WriteCommandError: true,
                            BulkWriteError: true,
                            DB: true,
                            DBCollection: true,
                            DBQuery: true,
                            DBExplainQuery: true,
                            DBCommandCursor: true,
                            MongoBridge: true,
                            MongoURI: true,
                            WriteConcern: true,
                            SessionOptions: true,
                            CollInfos: true,
                            CountDownLatch: true,
                            BSONAwareMap: true,
                            latestFCV: true,
                            lastLTSFCV: true,
                            lastContinuousFCV: true,
                            checkFCV: true,
                            isFCVEqual: true,
                            binVersionToFCV: true,
                            numVersionsSinceLastLTS: true,
                            getFCVConstants: true,
                            removeFCVDocument: true,
                            targetFCV: true,
                            assert: true,
                            doassert: true,
                            authutil: true,
                            tojson: true,
                            tojsononeline: true,
                            tostrictjson: true,
                            tojsonObject: true,
                            toJsonForLog: true,
                            print: true,
                            printjson: true,
                            printjsononeline: true,
                            jsTest: true,
                            jsTestLog: true,
                            jsonTestLog: true,
                            ErrorCodes: true,
                            ErrorCodeStrings: true,
                            checkProgram: true,
                            Random: true,
                            checkLog: true,
                            sleep: true,
                            resetDbpath: true,
                            copyDbpath: true,
                            jsTestName: true,
                            startParallelShell: true,
                            buildInfo: true,
                            getBuildInfo: true,
                            jsTestOptions: true,
                            printShardingStatus: true,
                            _getErrorWithCode: true,
                            isNetworkError: true,
                            __magicNoPrint: true,
                            computeSHA256Block: true,
                            emit: true,
                            _awaitRSHostViaRSMonitor: true,
                            convertShardKeyToHashed: true,
                            benchRun: true,
                            benchRunSync: true,
                            gc: true,
                            DataConsistencyChecker: true,
                            isNumber: true,
                            isObject: true,
                            isString: true,
                            _createSecurityToken: true,
                            _createTenantToken: true,
                            _isAddressSanitizerActive: true,
                            _isLeakSanitizerActive: true,
                            _isThreadSanitizerActive: true,
                            _isUndefinedBehaviorSanitizerActive: true,
                            _isSpiderMonkeyDebugEnabled: true,
                            _optimizationsEnabled: true,
                            allocatePort: true,
                            allocatePorts: true,
                            resetAllocatedPorts: true,
                            bsonObjToArray: true,
                            _writeTestPipeObjects: true,
                            _writeTestPipe: true,
                            _writeTestPipeBsonFile: true,
                            _readTestPipes: true,
                            runFeatureFlagMultiversionTest: true,
                            isRetryableError: true,
                            numberDecimalsAlmostEqual: true,
                            numberDecimalsEqual: true,
                            debug: true,
                            bsonsize: true,
                            _DelegatingDriverSession: true,
                            _DummyDriverSession: true,
                            _ServerSession: true,
                            sortDoc: true,
                            executeNoThrowNetworkError: true,
                            _readDumpFile: true,
                            _openGoldenData: true,
                            _writeGoldenData: true,
                            _threadInject: true,
                            port: true,
                            _buildBsonObj: true,
                            convertTrafficRecordingToBSON: true,
                            _setShellFailPoint: true,
                            shellHelper: true,
                            _srand: true,
                            _shouldUseImplicitSessions: true,
                            testingReplication: true,
                            myPort: true,
                            retryOnNetworkError: true,
                            getJSHeapLimitMB: true,
                            _getEnv: true,
                            indentStr: true,
                            _forgetReplSet: true,
                            _fnvHashToHexString: true,
                            _resultSetsEqualUnordered: true,
                            getStringWidth: true,
                            _compareStringsWithCollation: true,
                            eventResumeTokenType: true,
                            highWaterMarkResumeTokenType: true,

                            // likely could be replaced with `path`
                            _copyFileRange: true,
                            appendFile: true,
                            copyFile: true,
                            writeFile: true,
                            fileExists: true,
                            pathExists: true,
                            umask: true,
                            getFileMode: true,
                            copyDir: true,

                            // likely could be replaced with `child_process`
                            MongoRunner: true,
                            run: true,
                            runProgram: true,
                            runMongoProgram: true,
                            runNonMongoProgram: true,
                            runNonMongoProgramQuietly: true,
                            _runMongoProgram: true,
                            _startMongoProgram: true,
                            startMongoProgram: true,
                            _stopMongoProgram: true,
                            stopMongoProgramByPid: true,
                            clearRawMongoProgramOutput: true,
                            rawMongoProgramOutput: true,
                            waitProgram: true,
                            waitMongoProgram: true,
                            _runningMongoChildProcessIds: true,
                            startMongoProgramNoConnect: true,

                            // shell-specific
                            shellPrintHelper: true,
                            shellAutocomplete: true,
                            __autocomplete__: true,
                            defaultPrompt: true,
                            ___it___: true,
                            __promptWrapper__: true,
                            passwordPrompt: true,
                            isInteractive: true,

                            // built-in BSON types and helpers
                            Code: true,
                            MaxKey: true,
                            MinKey: true,
                            HexData: true,
                            DBPointer: true,
                            DBRef: true,
                            BinData: true,
                            NumberLong: true,
                            NumberDecimal: true,
                            Timestamp: true,
                            MD5: true,
                            Geo: true,
                            decodeResumeToken: true,
                            bsonWoCompare: true,
                            bsonUnorderedFieldsCompare: true,
                            bsonBinaryEqual: true,
                            friendlyEqual: true,
                            timestampCmp: true,
                            decompressBSONColumn: true,

                            hex_md5: true,
                            QueryHelpers: true,
                            chatty: true,
                            DriverSession: true,
                            ToolTest: true,
                            uncheckedParallelShellPidsString: true,
                            _shouldRetryWrites: true,

                            // from_cpp:
                            __prompt__: true,
                            _replMonitorStats: true,

                            // explainable.js
                            Explainable: true,

                            // utils.js
                            _verboseShell: true,
                            __quiet: true,
                            printStackTrace: true,
                            setVerboseShell: true,
                            _barFormat: true,
                            compare: true,
                            compareOn: true,
                            shellPrint: true,
                            _originalPrint: true,
                            disablePrint: true,
                            enablePrint: true,
                            replSetMemberStatePrompt: true,
                            hasErrorCode: true,
                            helloStatePrompt: true,
                            _validateMemberIndex: true,
                            help: true,
                            retryOnRetryableError: true,
                        },

                        ecmaVersion: 2022,
                        sourceType: "module",
                    },

                    plugins: {
                        "mongodb": mongodb_plugin,
                    },

                    rules: {
                        // TODO SERVER-99571 : enable mongodb/* rules.
                        "mongodb/no-print-fn": 0,
                        "mongodb/no-printing-tojson": 0,

                        "no-prototype-builtins": 0,
                        "no-useless-escape": 0,
                        "no-irregular-whitespace": 0,
                        "no-inner-declarations": 0,

                        "no-unused-vars": [
                            0,
                            {
                                varsIgnorePattern: "^_",
                                args: "none",
                            }
                        ],

                        "no-empty": 0,
                        "no-redeclare": 0,
                        "no-constant-condition": 0,
                        "no-loss-of-precision": 0,
                        semi: 2,

                        "no-restricted-syntax": [
                            "error",
                            {
                                message:
                                    "Invalid load call. Please convert your library to a module and import it instead.",
                                selector: "CallExpression > Identifier[name=\"load\"]",
                            }
                        ],
                    },
                },
                {
                    // It's ok for golden tests to use print() and tojson() directly.
                    plugins: {
                        "mongodb": mongodb_plugin,
                    },
                    files: [
                        "jstests/libs/begin_golden_test.js",
                        "jstests/libs/golden_test.js",
                        "jstests/libs/override_methods/golden_overrides.js",
                        "jstests/libs/override_methods/sharded_golden_overrides.js",
                        "jstests/libs/query/golden_test_utils.js",
                        "jstests/libs/query_golden_sharding_utils.js",
                        "jstests/query_golden/**/*.js",
                        "jstests/query_golden_sharding/**/*.js",
                    ],
                    rules: {
                        "mongodb/no-print-fn": 0,
                        "mongodb/no-tojson-fn": 0,
                    }
                },
                {
                    // Don't run mongodb linter rules on src/
                    plugins: {
                        "mongodb": mongodb_plugin,
                    },
                    files: [
                        "src/**/*.js",
                    ],
                    rules: {
                        "mongodb/no-print-fn": 0,
                        "mongodb/no-tojson-fn": 0,
                    }
                }
];
