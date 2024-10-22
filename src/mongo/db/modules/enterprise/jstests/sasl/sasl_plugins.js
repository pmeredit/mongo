// Windows tests for Cyrus SASL plugin loading

(function() {
if (!_isWindows()) {
    quit();
}

const kSaslTestPluginFileName = 'cyrus_sasl_windows_test_plugin.dll';
const kSaslPluginLogOutputString = `Loaded ${kSaslTestPluginFileName}`;
const kSaslPluginFailureMessageTemplate = 'Unexpectedly loading Cyrus SASL plugin in ';

function shouldNotLoadCyrusSaslPluginInClient() {
    let out = runNonMongoProgram("mongo", "--eval", ";");

    rawMongoProgramOutput(".*").split("\n").forEach(function(line) {
        assert(-1 === line.indexOf(kSaslPluginLogOutputString),
               kSaslPluginFailureMessageTemplate + 'client');
    });
}

function shouldNotLoadCyrusSaslPluginInServer() {
    clearRawMongoProgramOutput();
    const m = MongoRunner.runMongod();

    try {
        rawMongoProgramOutput(".*").split("\n").forEach(function(line) {
            assert(-1 === line.indexOf(kSaslPluginLogOutputString),
                   kSaslPluginFailureMessageTemplate + 'server');
        });
    } finally {
        MongoRunner.stopMongod(m);
    }
}

shouldNotLoadCyrusSaslPluginInClient();
shouldNotLoadCyrusSaslPluginInServer();
})();
