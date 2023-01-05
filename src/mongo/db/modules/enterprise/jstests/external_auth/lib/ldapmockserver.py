#! /usr/bin/env python

import sys
import io

from twisted.application import service
from twisted.internet.endpoints import serverFromString
from twisted.internet.protocol import ServerFactory
from twisted.python.components import registerAdapter
from twisted.python import log, usage
from ldaptor.inmemory import fromLDIFFile
from ldaptor.interfaces import IConnectedLDAPEntry
from ldaptor.protocols.ldap import ldaperrors
from ldaptor.protocols.ldap.ldapserver import LDAPServer
from ldaptor.protocols import pureldap

from ldaptor.protocols.pureber import (
    BEROctetString,
    BERSequence,
    CLASS_APPLICATION,
    berDecodeMultiple,
)

LDIF = b"""\
dn: dc=cc
dc: cc
objectClass: dcObject

dn: dc=10gen,dc=cc
dc: 10gen
objectClass: dcObject
objectClass: organization

dn: ou=Users,dc=10gen,dc=cc
objectClass: organizationalUnit
ou: Users

dn: ou=Groups,dc=10gen,dc=cc
objectClass: organizationUnit
ou: Groups

dn: cn=ldapz_admin,ou=Users,dc=10gen,dc=cc
objectClass: user
userPassword: Secret123
cn: ldapz_admin
memberOf: cn=testWriter,ou=Groups,dc=10gen,dc=cc

dn: cn=ldapz_ldap_bind,ou=Users,dc=10gen,dc=cc
objectClass: user
userPassword: Admin001
cn: ldapz_ldap_bind
memberOf: cn=testWriter,ou=Groups,dc=10gen,dc=cc

dn: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc
objectClass: user
userPassword: Secret123
cn: ldapz_ldap1
memberOf: cn=testWriter,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupD,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupC,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupB,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupA,ou=Groups,dc=10gen,dc=cc

dn: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc
objectClass: user
userPassword: Secret123
uid: ldapz_ldap2
cn: ldapz_ldap2
memberOf: cn=testWriter,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupE,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupC,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupB,ou=Groups,dc=10gen,dc=cc
memberOf: cn=groupA,ou=Groups,dc=10gen,dc=cc

dn: cn=testWriter,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: testWriter
member: cn=ldapz_admin,ou=Users,dc=10gen,dc=cc
member: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc
member: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc

dn: cn=groupE,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: groupE
member: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc

dn: cn=groupD,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: groupD
member: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc

dn: cn=groupC,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: groupC
member: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc
member: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc

dn: cn=groupB,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: groupB
member: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc
member: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc

dn: cn=groupA,ou=Groups,dc=10gen,dc=cc
objectClass: groupOfNames
cn: groupA
member: cn=ldapz_ldap1,ou=Users,dc=10gen,dc=cc
member: cn=ldapz_ldap2,ou=Users,dc=10gen,dc=cc

"""
class Options(usage.Options):
    optParameters = [
        [ "port", "p", "10389", "The port to listen from as an LDAP server", int ],
        [ "referral-uri", "r", "", "The URI to refer search requests to", str ],
    ]


class Tree:
    def __init__(self):
        global LDIF
        self.f = io.BytesIO(LDIF)
        d = fromLDIFFile(self.f)
        d.addCallback(self.ldifRead)

    def ldifRead(self, result):
        self.f.close()
        self.db = result

class LDAPSearchResultReferral(pureldap.LDAPProtocolResponse, BERSequence):
    tag = CLASS_APPLICATION | 0x13

    def __init__(self, uris=None):
        pureldap.LDAPProtocolResponse.__init__(self)
        BERSequence.__init__(self, value=[], tag=LDAPSearchResultReferral.tag)
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
        return BERSequence(BERSequence([BEROctetString(uri) for uri in self.uris]), tag = LDAPSearchResultReferral.tag).toWire()

    def __repr__(self):
        return "{}(uris={}{})".format(
            self.__class__.__name__,
            repr([uri for uri in self.uris]),
            f", tag={self.tag}" if self.tag != self.__class__.tag else "",
        )

class MockLDAPServer(LDAPServer):
    """
    A mock LDAP server. It can operate in 2 modes depending on whether referral_uri is provided. The
    absence of referral_uri causes it to use the DIT specified in LDIF for all binds and searches.
    If referral_uri is specified, then it will return a referral to that URI for all non-liveness
    check search requests. Binds and rootDSE queries will be processed locally.
    """

    def __init__(self, referral_uri):
        LDAPServer.__init__(self)
        self.referral_uri = referral_uri

    def handle_LDAPSearchRequest(self, request, controls, reply):
        if (request.attributes == [b'supportedSASLMechanisms']):
            reply(
                pureldap.LDAPSearchResultEntry(
                    objectName="",
                    attributes=[
                        (b'supportedSASLMechanisms', [b'PLAIN']),
                    ],
                )
            )
            return pureldap.LDAPSearchResultDone(resultCode=ldaperrors.Success.resultCode)
        
        # If the mock server has been started with a referral URI, return a referral to that server.
        if (len(self.referral_uri) > 0):
            reply(LDAPSearchResultReferral(uris=[f"ldap://{self.referral_uri}/{request.baseObject.decode('ascii')}"]))
            return pureldap.LDAPSearchResultDone(resultCode=ldaperrors.Success.resultCode)
        
        # Otherwise, default to the LDAPServer implementation to retrieve the entry from the DIT.
        return super().handle_LDAPSearchRequest(request, controls, reply)

    def handle_LDAPAbandonRequest(self, request, controls, reply):
        return None

class LDAPServerFactory(ServerFactory):
    protocol = MockLDAPServer

    def __init__(self, root, referral_uri):
        self.root = root
        self.referral_uri = referral_uri

    def buildProtocol(self, addr):
        proto = self.protocol(self.referral_uri)
        proto.debug = self.debug
        proto.factory = self
        return proto


if __name__ == "__main__":
    from twisted.internet import reactor

    config = Options()
    try:
        config.parseOptions()
    except usage.UsageError as errortext:
        print('{}: {}'.format(sys.argv[0], errortext))
        print('{}: Try --help for usage details'.format(sys.argv[0]))
        sys.exit(1)
        
    # First of all, to show logging info in stdout :
    log.startLogging(sys.stderr)
    # We initialize our tree
    tree = Tree()
    # When the LDAP Server protocol wants to manipulate the DIT, it invokes
    # `root = interfaces.IConnectedLDAPEntry(self.factory)` to get the root
    # of the DIT.  The factory that creates the protocol must therefore
    # be adapted to the IConnectedLDAPEntry interface.
    registerAdapter(lambda x: x.root, LDAPServerFactory, IConnectedLDAPEntry)
    factory = LDAPServerFactory(tree.db, config['referral-uri'])
    factory.debug = True
    application = service.Application("ldaptor-server")
    myService = service.IServiceCollection(application)
    serverEndpointStr = f"tcp:{config['port']}"
    e = serverFromString(reactor, serverEndpointStr)
    d = e.listen(factory)
    reactor.run()
