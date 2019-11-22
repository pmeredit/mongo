// Tests the Kerberos validation tool against several example configurations

(function() {
'use strict';

// assert that running mongokerberos with given arguments causes an exit with failure exit code
// return output of running program
function assertError(failureMessage, ...args) {
    return assert.neq(
        "0", runProgram.apply(null, ["mongokerberos", "--debug"].concat(args)), failureMessage);
}

// assert that running mongokerberos with given arguments cause an exit with success exit code
// return output of running program
function assertSuccess(failureMessage, ...args) {
    return assert.eq(
        "0", runProgram.apply(null, ["mongokerberos", "--debug"].concat(args)), failureMessage);
}

// not specifying --client or --server will result in an error
assertError("mongokerberos ran successfully without specifying required arg --client|--server",
            "--username",
            "ldapz_kerberos1@LDAPTEST.10GEN.CC");

// specifying --client and --server will result in an error
assertError(
    "mongokerberos ran successfully when specifying conflicting arguments --client and --server",
    "--client",
    "--server",
    "--username",
    "ldapz_kerberos1@LDAPTEST.10GEN.CC");

// specifying --client without --username will result in an error
assertError("mongokerberos --client ran successfully without specifying required arg --username",
            "--client");
})();
