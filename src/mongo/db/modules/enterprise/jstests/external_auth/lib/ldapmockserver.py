#! /usr/bin/env python

import io
import sys

from ldaptor.inmemory import fromLDIFFile
from ldaptor.interfaces import IConnectedLDAPEntry
from ldaptor.protocols import pureldap
from ldaptor.protocols.ldap import ldaperrors
from ldaptor.protocols.ldap.ldapserver import LDAPServer
from ldaptor.protocols.ldap.ldifprotocol import LDIFTruncatedError
from ldaptor.protocols.pureber import (
    CLASS_APPLICATION,
    BEROctetString,
    BERSequence,
    berDecodeMultiple,
)
from twisted.application import service
from twisted.internet import task
from twisted.internet.endpoints import serverFromString
from twisted.internet.protocol import ServerFactory
from twisted.python import log, usage
from twisted.python.components import registerAdapter


class Options(usage.Options):
    optParameters = [
        ["port", "p", "10389", "The port to listen from as an LDAP server", int],
        ["dit-file", "f", "", "The LDIF file specifying the LDAP server's directory", str],
        ["referral-uri", "r", "", "The URI to refer search requests to", str],
        [
            "delay",
            "d",
            "0",
            "The number of seconds to delay bind and search responses back by",
            int,
        ],
    ]


class Tree:
    def __init__(self, ldif_file):
        if len(ldif_file) > 0:
            try:
                with open(ldif_file, "rb") as f:
                    self.f = io.BytesIO(f.read())
                    d = fromLDIFFile(self.f)
                    d.addCallback(self.ldifRead)
            except (IOError, OSError, LDIFTruncatedError) as err:
                print("Error reading file: ", ldif_file, "Reason: ", err)
                sys.exit(1)
        else:
            # If a LDIF file was not specified, then this LDAP server must be performing referrals
            # only. Since ldaptor does not natively support referrals, the server instantiates its
            # tree to None and manually constructs referral responses as needed during binds and
            # searches.
            self.db = None

    def ldifRead(self, result):
        self.f.close()
        self.db = result


class LDAPResultReferral(pureldap.LDAPProtocolResponse, BERSequence):
    tag = CLASS_APPLICATION | 0x13

    def __init__(self, uris=None):
        pureldap.LDAPProtocolResponse.__init__(self)
        BERSequence.__init__(self, value=[], tag=LDAPResultReferral.tag)
        assert uris is not None
        self.uris = uris

    @classmethod
    def fromBER(cls, tag, content, berdecoder=None):
        decoded = berDecodeMultiple(
            content,
            LDAPBERDecoderContext_LDAPSearchResultReference(fallback=berdecoder),
        )
        instance = cls(uris=decoded)
        return instance

    def toWire(self):
        return BERSequence(
            BERSequence([BEROctetString(uri) for uri in self.uris]), tag=LDAPResultReferral.tag
        ).toWire()

    def __repr__(self):
        return "{}(uris={}{})".format(
            self.__class__.__name__,
            repr([uri for uri in self.uris]),
            f", tag={self.tag}" if self.tag != self.__class__.tag else "",
        )


class MockLDAPServer(LDAPServer):
    """
    A mock LDAP server. It can operate in 2 modes depending on whether referral_uri is provided. The
    absence of referral_uri causes it to use the DIT loaded from the dit_file LDIF for all binds and searches.
    If referral_uri is specified, then it will return a referral to that URI for all non-liveness
    check searches and binds.
    Binds and searches may also be delayed by a certain interval as configured by the "delay"
    parameter.
    """

    def __init__(self, dit_file, referral_uri, delay):
        LDAPServer.__init__(self)
        self.dit_file = dit_file
        self.referral_uri = referral_uri
        self.delay = delay

    def handle_LDAPSearchRequest(self, request, controls, reply):
        response = pureldap.LDAPSearchResultDone(resultCode=ldaperrors.Success.resultCode)

        # RootDSE queries (used for liveness checks) are always handled locally.
        if request.attributes == [b"supportedSASLMechanisms"]:
            reply(
                pureldap.LDAPSearchResultEntry(
                    objectName="",
                    attributes=[
                        (b"supportedSASLMechanisms", [b"PLAIN"]),
                    ],
                )
            )

            response = pureldap.LDAPSearchResultDone(resultCode=ldaperrors.Success.resultCode)
            log.msg(
                "Handled rootDSE request: {} locally, reply is {}".format(
                    repr(request), repr(response)
                )
            )
        elif len(self.referral_uri) > 0:
            # If the mock server has been started with a referral URI, return a referral to that server.
            reply(
                LDAPResultReferral(
                    uris=[f"ldap://{self.referral_uri}/{request.baseObject.decode('ascii')}"]
                )
            )
            response = pureldap.LDAPSearchResultDone(resultCode=ldaperrors.Success.resultCode)
            log.msg(
                "Referred search request: {} to LDAP server at {}, reply is {}".format(
                    repr(request), self.referral_uri, repr(response)
                )
            )
        else:
            # Otherwise, default to the LDAPServer implementation to retrieve the entry from the DIT.
            response = super().handle_LDAPSearchRequest(request, controls, reply)
            log.msg(
                "Handled search query: {} locally, reply is {}".format(
                    repr(request), repr(response)
                )
            )

        # Delay the response as specified by the "delay" parameter.
        if self.delay > 0:
            log.msg(
                "Delaying search reply by {} seconds, reply is {}".format(
                    self.delay, repr(response)
                )
            )
            return task.deferLater(reactor, self.delay, lambda: response)

        return response

    def handle_LDAPBindRequest(self, request, controls, reply):
        # If the mock server has been started with a referral URI without a DIT, refer the bind
        # to the other server.
        response = pureldap.LDAPBindResponse(resultCode=ldaperrors.Success.resultCode)
        if len(self.referral_uri) > 0 and len(self.dit_file) == 0:
            reply(
                LDAPResultReferral(
                    uris=[f"ldap://{self.referral_uri}/{request.dn.decode('ascii')}"]
                )
            )
            response = pureldap.LDAPBindResponse(resultCode=ldaperrors.Success.resultCode)
            log.msg(
                "Referred bind request: {} to LDAP server at {}, reply is {}".format(
                    repr(request), self.referral_uri, repr(response)
                )
            )
        else:
            response = super().handle_LDAPBindRequest(request, controls, reply)
            log.msg(
                "Handled bind request: {} locally, reply is {}".format(
                    repr(request), repr(response)
                )
            )

        # Delay the response as specified by the "delay" parameter.
        if self.delay > 0:
            log.msg(
                "Delaying bind response by {} seconds, reply is {}".format(
                    self.delay, repr(response)
                )
            )
            return task.deferLater(reactor, self.delay, lambda: response)

        return response

    def handle_LDAPAbandonRequest(self, request, controls, reply):
        return None


class LDAPServerFactory(ServerFactory):
    protocol = MockLDAPServer

    def __init__(self, root, dit_file, referral_uri, delay):
        self.root = root
        self.dit_file = dit_file
        self.referral_uri = referral_uri
        self.delay = delay

    def buildProtocol(self, addr):
        proto = self.protocol(self.dit_file, self.referral_uri, self.delay)
        proto.debug = self.debug
        proto.factory = self
        return proto


if __name__ == "__main__":
    from twisted.internet import reactor

    config = Options()
    try:
        config.parseOptions()
    except usage.UsageError as errortext:
        print("{}: {}".format(sys.argv[0], errortext))
        print("{}: Try --help for usage details".format(sys.argv[0]))
        sys.exit(1)

    # First of all, to show logging info in stdout :
    log.startLogging(sys.stderr)

    # We initialize our tree
    tree = Tree(config["dit-file"])

    # When the LDAP Server protocol wants to manipulate the DIT, it invokes
    # `root = interfaces.IConnectedLDAPEntry(self.factory)` to get the root
    # of the DIT.  The factory that creates the protocol must therefore
    # be adapted to the IConnectedLDAPEntry interface.
    registerAdapter(lambda x: x.root, LDAPServerFactory, IConnectedLDAPEntry)
    factory = LDAPServerFactory(
        tree.db, config["dit-file"], config["referral-uri"], config["delay"]
    )
    factory.debug = True
    application = service.Application("ldaptor-server")
    myService = service.IServiceCollection(application)
    serverEndpointStr = f"tcp:{config['port']}"
    e = serverFromString(reactor, serverEndpointStr)
    d = e.listen(factory)
    reactor.run()
