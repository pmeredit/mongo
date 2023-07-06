// Run one or more tests inside a dns container

import {isRHEL8, isUbuntu1804} from "jstests/libs/os_helpers.js";
import {getPython3Binary} from "jstests/libs/python.js";

// Since the container is Ubuntu 18.04, it does not make sense to run binaries from other distros on
// it.
if (!isUbuntu1804() && !isRHEL8()) {
    quit();
}

const lib_dir = "src/mongo/db/modules/enterprise/jstests/external_auth/lib";
const ldap_container = `${lib_dir}/ldap_container.py`;

// Stop any previously running containers to ensure we are running from a clean state
const stop_ret = runMongoProgram(getPython3Binary(), "-u", ldap_container, "-v", "stop");
assert.eq(stop_ret, 0, "Could not stop containers");

// Build and start the container
const start_ret = runMongoProgram(getPython3Binary(), "-u", ldap_container, "-v", "start");
assert.eq(start_ret, 0, "Could not start containers");

try {
    // Run the given command in side the container
    // NOTE: the paths refer to paths inside the container
    //
    // TODO - add more containers here since we do not support running concurrent containers
    const runt_ret = runMongoProgram(
        getPython3Binary(),
        "-u",
        ldap_container,
        "-v",
        "run",
        "mongo",
        "-nodb",
        "/app/src/mongo/db/modules/enterprise/jstests/external_auth/dns/dns_test.js");
    assert.eq(runt_ret, 0, "Could not run command");
} finally {
    // Stop the container
    const stop2_ret = runMongoProgram(getPython3Binary(), "-u", ldap_container, "-v", "stop");
    assert.eq(stop2_ret, 0, "Could not stop containers");
}
