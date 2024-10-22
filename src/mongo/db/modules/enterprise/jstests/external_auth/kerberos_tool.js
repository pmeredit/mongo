// Tests the Kerberos validation tool against several example configurations
// Excluded from AL2 Atlas Enterprise build variant since it depends on krb5/kinit, which
// is not installed on that variant.
// @tags: [incompatible_with_atlas_environment]

import {
    assetsPath,
    saslHostName
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

if (_isWindows()) {
    quit();
}
const kerbUser = "ldapz_kerberos1";
const kerbService = "mockservice";
const kerbRealm = "LDAPTEST.10GEN.CC";
const kerbPrinc = kerbUser + "@" + kerbRealm;
const defaultEnv = {
    KRB5_CONFIG: assetsPath + "krb5.conf",
    KRB5_KTNAME: assetsPath + "ldapz_kerberos1.keytab",
    KRB5_CLIENT_KTNAME: "jstests/libs/mockservice.keytab"
};

// assert that running mongokerberos with given arguments causes an exit with failure exit code
// return output of running program
function assertError(failureMessage, environment, ...args) {
    // clear, then populate the credentials cache
    if (!_isWindows()) {
        run("kdestroy", "-A");
        run("kinit", "-k", "-t", defaultEnv.KRB5_KTNAME, kerbPrinc);
    }
    const pid =
        _startMongoProgram({args: ["mongokerberos", "--debug"].concat(args), env: environment});
    return assert.neq("0", waitProgram(pid), failureMessage);
}

// assert that running mongokerberos with given arguments cause an exit with success exit code
// return output of running program
function assertSuccess(failureMessage, environment, ...args) {
    // clear, then populate the credentials cache
    if (!_isWindows()) {
        run("kdestroy", "-A");
        run("kinit", "-k", "-t", defaultEnv.KRB5_KTNAME, kerbPrinc);
    }
    const pid =
        _startMongoProgram({args: ["mongokerberos", "--debug"].concat(args), env: environment});
    return assert.eq("0", waitProgram(pid), failureMessage);
}

function assertCorrectErrorMessage(failureMessage, expectedErrorMessage, environment, ...args) {
    // clear, then populate the credentials cache
    if (!_isWindows()) {
        run("kdestroy", "-A");
        run("kinit", "-k", "-t", defaultEnv.KRB5_KTNAME, kerbPrinc);
    }
    const pid =
        _startMongoProgram({args: ["mongokerberos", "--debug"].concat(args), env: environment});
    waitProgram(pid);
    const output = rawMongoProgramOutput(".*");
    assert.neq(-1, output.search(expectedErrorMessage), failureMessage);
}

// Takes in a string representing actual or resolved config file contents,
// and removes any information that would differentiate the two, such as shell ID, whitespace, and
// comments. Finally, sorts the non-empty lines, since the config profile is not necessarily
// resolved in order.
function normalizeConfig(configContents) {
    return configContents
        // split lines into a list
        .split("\n")
        // remove all whitespace in every line
        .map(line => line.replace(/\s/g, ''))
        // remove all process identifiers from tool output
        .map(line => line.replace(/sh[0-9]+\|/g, ''))
        // remove all empty lines
        .filter(line => line !== "")
        // remove all comments
        .filter(line => !line.startsWith("#"))
        .sort();
}

jsTestLog("Running mongokerberos in basic client configuration");
assertSuccess("mongokerberos could not run successfully in client mode.",
              defaultEnv,
              "--client",
              "--host",
              saslHostName,
              "--username",
              kerbUser,
              "--gssapiServiceName",
              kerbService);

jsTestLog("Running mongokerberos in basic server configuration");
// make sure to use keytab for mockservice keytab (has service principals in it)
assertSuccess("mongokerberos could not run successfully in server mode.",
              Object.merge(defaultEnv, {KRB5_KTNAME: "jstests/libs/mockservice.keytab"}),
              "--server",
              "--gssapiServiceName",
              kerbService,
              "--gssapiHostName",
              saslHostName);

jsTestLog("Ensuring value of KRB5_CLIENT_KTNAME does not affect server mode");
assertSuccess("mongokerberos failed in server mode as a result of a bad client keytab.",
              Object.merge(defaultEnv, {
                  KRB5_KTNAME: "jstests/libs/mockservice.keytab",
                  KRB5_CLIENT_KTNAME: "/var/NotAKeytabFile"
              }),
              "--server",
              "--gssapiServiceName",
              kerbService,
              "--gssapiHostName",
              saslHostName);

// verify config file is printed
if (!_isWindows()) {
    jsTestLog("Checking config profile was resolved correctly");
    const output = rawMongoProgramOutput(".*");
    const configStartIndicator = "KRB5 config profile resolved as: \n";
    const configEndIndicator = "\\[OK\\] KRB5 config profile resolved without errors.";
    const configStart = output.search(configStartIndicator) + configStartIndicator.length;
    const configEnd = output.search(configEndIndicator);
    const configContentsResolved = output.substring(configStart, configEnd);
    const configContentsActual = cat(defaultEnv.KRB5_CONFIG);
    const linesResolved = normalizeConfig(configContentsResolved);
    const linesActual = normalizeConfig(configContentsActual);
    assert.eq(
        linesResolved, linesActual, "Config file was not resolved perfectly by mongokerberos.");
}

// TODO SERVER-45937 test to see what happens if we open a client against a service which doesn't
// exist, or with a user principal which isn't available

// verify correct error output if keytab doesn't exist
if (!_isWindows()) {
    jsTestLog("Testing mongokerberos in server mode with non-existent keytab");
    assertCorrectErrorMessage(
        "mongokerberos did not produce expected error output when keytab doesn't exist.",
        "KRB5 keytab either doesn't exist or contains no entries.",
        Object.merge(defaultEnv, {KRB5_KTNAME: "/var/NotAKeytabFile"}),
        "--server",
        "--gssapiHostName",
        saslHostName);
}

// verify correct error output if keytab has missing entries
if (!_isWindows()) {
    jsTestLog("Testing mongokerberos against keytab with missing entries");
    assertCorrectErrorMessage(
        "mongokerberos did not produce expected error output when keytab was missing entries.",
        "KRB5 keytab either doesn't exist or contains no entries.",
        Object.merge(defaultEnv, {KRB5_KTNAME: assetsPath + "empty.keytab"}),
        "--server",
        "--gssapiHostName",
        saslHostName);
}

// TODO SERVER-45678 Test RDNS, would require additional keytab entry and account in KDC.

// verify correct error output if misconfigured RDNS
jsTestLog("Testing output for misconfigured RDNS");
assertSuccess("mongokerberos did not produce expected error output when RDNS was misconfigured.",
              defaultEnv,
              "--client",
              "--host",
              saslHostName,
              "--username",
              kerbUser,
              "--gssapiServiceName",
              kerbService);
assert.neq(rawMongoProgramOutput(".*").search("could not find hostname in reverse lookup"),
           -1,
           "Running with misconfigured RDNS exited successfully, but an error was not printed.");

// not specifying --client or --server will result in an error
jsTestLog("Testing output for invocation without --client and --server");
assertError("mongokerberos ran successfully without specifying required arg --client|--server",
            defaultEnv,
            "--username",
            kerbPrinc);

// specifying --client and --server will result in an error
jsTestLog("Testing output for invocation with both --client and --server");
assertError(
    "mongokerberos ran successfully when specifying conflicting arguments --client and --server",
    defaultEnv,
    "--client",
    "--server",
    "--username",
    kerbPrinc);

// specifying --client without --username will result in an error
jsTestLog("Testing output for client-mode invocation without --username");
assertError("mongokerberos --client ran successfully without specifying required arg --username",
            defaultEnv,
            "--client");
