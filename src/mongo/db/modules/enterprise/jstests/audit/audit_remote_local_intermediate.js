// Tests `local`, `remote` and `intermediates` fields are present in audit logs when connected with
// or without a load balancer.

import {ProxyProtocolServer} from "jstests/sharding/libs/proxy_protocol.js";
import {
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";
import {
    kSchemaMongo,
    runTest
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_remote_local_intermediate_lib.js";

{
    jsTest.log("Testing standalone");
    const fixture = new StandaloneFixture();
    runTest(fixture, kSchemaMongo);
}

{
    jsTest.log("Testing sharded cluster");
    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */);
}

// TODO: SERVER-100859: remove
// Proxy protocol server does not work on windows
if (_isWindows()) {
    quit();
}

{
    jsTest.log("Testing sharded cluster with load balanced connection with version 1");
    const ingressPort = allocatePort();
    const egressPort = allocatePort();

    const proxy_server = new ProxyProtocolServer(ingressPort, egressPort, 1);
    proxy_server.start();

    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */, proxy_server);

    proxy_server.stop();
}

{
    jsTest.log("Testing sharded cluster with load balanced connection with version 2");
    const ingressPort = allocatePort();
    const egressPort = allocatePort();

    const proxy_server = new ProxyProtocolServer(ingressPort, egressPort, 2);
    proxy_server.start();

    const fixture = new ShardingFixture();
    runTest(fixture, kSchemaMongo, true /* isSharded */, proxy_server);

    proxy_server.stop();
}
