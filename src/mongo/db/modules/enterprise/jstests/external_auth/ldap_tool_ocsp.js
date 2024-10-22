// Check that OCSP verification works with mongoldap
// @tags: [
//   requires_http_client,
// ]

import {getPython3Binary} from "jstests/libs/python.js";
import {MockOCSPServer} from "jstests/ocsp/lib/mock_ocsp.js";
import {OCSP_CA_PEM, OCSP_SERVER_CERT} from "jstests/ocsp/lib/ocsp_helpers.js";
import {
    defaultUserDNSuffix
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

// TODO SERVER-61043
if (_isWindows()) {
    quit();
}

function runWithEnv(args, env) {
    const pid = _startMongoProgram({args: args, env: env});
    return waitProgram(pid);
}

let mock_ocsp = new MockOCSPServer("", 1);
mock_ocsp.start();

const proxyPort = allocatePort();
const proxyPath = "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapproxy.py";

clearRawMongoProgramOutput();

const pid = startMongoProgramNoConnect(getPython3Binary(),
                                       proxyPath,
                                       "--port",
                                       proxyPort,
                                       "--useTLSServer",
                                       "--serverCert",
                                       OCSP_SERVER_CERT,
                                       "--targetHost",
                                       "ldaptest.10gen.cc",
                                       "--targetPort",
                                       "389",
                                       "--delay",
                                       0);

assert(checkProgram(pid).alive);

assert.soon(function() {
    return rawMongoProgramOutput(".*").search("Starting factory") !== -1;
});

sleep(2000);

assert.eq("0",
          runWithEnv(
              [
                  "mongoldap",
                  "--debug",
                  "--color=false",
                  "--ldapServers",
                  "localhost:" + proxyPort,
                  "--ldapTransportSecurity",
                  "tls",
                  "--ldapServerCAFile",
                  OCSP_CA_PEM,
                  "--user",
                  "cn=ldapz_admin," + defaultUserDNSuffix,
                  "--password",
                  "Secret123"
              ],
              // Do not require OpenLDAP to do TLS peer certificate validation because it uses its
              // own custom CN validation which fails on RHEL8
              {LDAPTLS_REQCERT: "allow"}));

mock_ocsp.stop();
