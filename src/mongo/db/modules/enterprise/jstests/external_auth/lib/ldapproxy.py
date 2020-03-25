#!/usr/bin/env python3
"""
This is a simple LDAP proxy that lets you simulate slow LDAP servers and test connectivity
to LDAP servers for use in our LDAP tests.
"""

import platform
if platform.system() == "Windows":
    from twisted.internet import iocpreactor
    iocpreactor.install()


from ldaptor.protocols import pureldap
from ldaptor.protocols.ldap.ldapclient import LDAPClient
from ldaptor.protocols.ldap.ldapconnector import connectToLDAPEndpoint, LDAPClientCreator
from ldaptor.protocols.ldap.ldapsyntax import LDAPEntry
from ldaptor.protocols.ldap.proxybase import ProxyBase
from ldaptor.protocols.pureldap import LDAPSearchRequest, LDAPResult
from twisted.internet import defer, protocol, reactor, task
from twisted.internet.endpoints import serverFromString, clientFromString, connectProtocol
from twisted.python import log, usage
from functools import partial
import sys

class LDAPProxy(ProxyBase):
    def __init__(self, config):
        self.delay = float(config['delay'])
        self.msgID = 0
        self.port = config['port']
        self.unauthorizedRootDSE = config['unauthorizedRootDSE']
        ProxyBase.__init__(self)

    def handleBeforeForwardRequest(self, request, controls, reply):
        if self.unauthorizedRootDSE and isinstance(request, LDAPSearchRequest) and (not request.baseObject) and (request.attributes == [b'supportedSASLMechanisms']):
            log.msg("Proxy port {}: Failing on RootDSE query. Request was: {}".format(self.port, repr(request)))
            reply(LDAPResult(resultCode=50)) # 50 == LDAP_INSUFFICIENT_PRIVILEGES
            return None
        log.msg("Proxy port {}: got request for {}".format(self.port, repr(request)))
        return super().handleBeforeForwardRequest(request, controls, reply)

    def handleProxiedResponse(self, response, request, controls):
        myMsgID = self.msgID
        self.msgID += 1
        log.msg("Proxy port {}: Queing delayed response for {} seconds for msg id {}. Request was: {}".format(
            self.port, self.delay, myMsgID, repr(request)))

        def delayedResponse():
            log.msg("Proxy port {}: Sending delayed response for msg id {}: {}".format(
                self.port, myMsgID, repr(response)))
            return response

        return task.deferLater(reactor, self.delay, delayedResponse)

class Options(usage.Options):
    optFlags = [
        [ "testClient", "t", "Test connecting to an LDAP server and running a root DSE query" ],
        [ "useTLS", "s", "Whether to connect with SSL" ],
        [ "unauthorizedRootDSE", "D", "Return INSUFFICIENT_PRIVILEGES for RootDSE queries" ],
    ]

    optParameters = [
        [ "port", "p", 10389, "The port to listen on", int ],
        [ "targetHost", "t", "ldaptest.10gen.cc", "The host to proxy connections to", str ],
        [ "targetPort", "P", "389", "The port to proxy connections to", int ],
        [ "delay", "d", 3.5, "How long to delay requests in seconds", float ]
    ]

@defer.inlineCallbacks
def testClientConnect(config):
    clientCreator = LDAPClientCreator(reactor, LDAPClient)
    clientConfig = { "": (config['targetHost'], config['targetPort']) }
    print("Connecting to {}:{}".format(config['targetHost'], config['targetPort']))

    client = yield clientCreator.connect("", overrides=clientConfig)

    searchReq = LDAPEntry(client, "")
    results = yield searchReq.search(scope=pureldap.LDAP_SCOPE_baseObject,
                                     filterText='(objectClass=*)')

    print("Got root DSE!")

if __name__ == "__main__":
    print("Starting LDAP Proxy with {} reactor".format(type(reactor).__name__))
    config = Options()
    try:
        config.parseOptions()
    except usage.UsageError as errortext:
        print('{}: {}'.format(sys.argv[0], errortext))
        print('{}: Try --help for usage details'.format(sys.argv[0]))
        sys.exit(1)

    if (config['testClient']):
        test = testClientConnect(config)
        test.exitCode = 0
        def onError(err):
            test.exitCode = 1
            err.printDetailedTraceback(file=sys.stderr)

        test.addErrback(onError)
        test.addCallback(lambda _: reactor.stop())
        reactor.run()
        sys.exit(test.exitCode)

    log.startLogging(sys.stderr)

    factory = protocol.ServerFactory()
    proxiedEndpointStr = "tcp:host={}:port={}".format(config["targetHost"], config["targetPort"])
    clientConnector = partial(
        connectToLDAPEndpoint,
        reactor,
        proxiedEndpointStr,
        LDAPClient)

    def buildProtocol():
        proto = LDAPProxy(config)
        proto.clientConnector = clientConnector
        proto.use_tls = config['useTLS']
        return proto

    factory.protocol = buildProtocol
    reactor.listenTCP(int(config['port']), factory, interface="::1")
    reactor.listenTCP(int(config['port']), factory, interface="127.0.0.1")
    reactor.run()
