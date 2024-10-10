#! /usr/bin/env python

import sys

from ldaptor import delta
from ldaptor.protocols import pureldap
from ldaptor.protocols.ldap.ldapclient import LDAPClient
from ldaptor.protocols.ldap.ldapsyntax import LDAPEntry
from twisted.internet import defer
from twisted.internet.endpoints import clientFromString, connectProtocol
from twisted.internet.task import react
from twisted.python import log, usage


class Options(usage.Options):
    optParameters = [
        ["targetHost", "t", "localhost", "The hostname of the local LDAP server", str],
        ["targetPort", "P", "10389", "The port to proxy connections to", int],
        ["group", "g", "", "The DN of group to modify", str],
        ["user", "u", "", "The DN of the user to add or delete to group", str],
        ["modifyAction", "m", "add", "If changetype is modify, set this to add or delete", str],
    ]


def options_to_modify_op(group, user, modify_action):
    """
    Given a group, a user, and a desired action (add or delete), this function returns
    a list of the LDAP modifications that need to be performed to add/delete the group from
    the user's memberOf attribute and the user from the group's member attribute.

    Returns: modifications
    """

    modifications = []
    if group is None or group == "":
        raise Exception("Input needs to include key, `group`!")
    if user is None or user == "":
        raise Exception("Input needs to include key, `user`!")
    if modify_action is None or modify_action == "":
        raise Exception("Input needs to include key, `modifyAction`!")

    if modify_action == "add":
        modifications.append(delta.Add("memberOf", [group]).asLDAP())
        modifications.append(delta.Add("member", [user]).asLDAP())
    elif modify_action == "delete":
        modifications.append(delta.Delete("memberOf", [group]).asLDAP())
        modifications.append(delta.Delete("member", [user]).asLDAP())

    return modifications


@defer.inlineCallbacks
def onConnect(client, group, user, modify_action):
    modifications = options_to_modify_op(group, user, modify_action)

    # Modify memberOf attribute in user entry.
    op = pureldap.LDAPModifyRequest(user, modification=[modifications[0]])
    response = yield client.send(op)
    if response.resultCode != 0:
        log.err("DIT reported error code {}: {}".format(response.resultCode, response.errorMessage))
        raise Exception("Failed to modify memberOf attribute in user entry")

    # Modify member attribute in group entry.
    op = pureldap.LDAPModifyRequest(group, modification=[modifications[1]])
    response = yield client.send(op)
    if response.resultCode != 0:
        log.err("DIT reported error code {}: {}".format(response.resultCode, response.errorMessage))
        raise Exception("Failed to modify member attribute in group entry")

    # Print new user and group.
    searchReq = LDAPEntry(client, "dc=10gen,dc=cc")
    user_cn = user.split(",")[0][3:]
    print("user_cn", user_cn)
    group_cn = group.split(",")[0][3:]
    print("group_cn", group_cn)
    results = yield searchReq.search(filterText="(|(cn={})(cn={}))".format(user_cn, group_cn))
    data = "".join([result.getLDIF() for result in results])
    print("User and group after modification:", data)


def onError(err, reactor):
    if reactor.running:
        log.err(err)
        reactor.stop()


def main(reactor):
    log.startLogging(sys.stdout)
    config = Options()
    try:
        config.parseOptions()
    except usage.UsageError as errortext:
        print("{}: {}".format(sys.argv[0], errortext))
        print("{}: Try --help for usage details".format(sys.argv[0]))
        sys.exit(1)

    endpoint_str = "tcp:host={}:port={}".format(config["targetHost"], config["targetPort"])
    e = clientFromString(reactor, endpoint_str)
    d = connectProtocol(e, LDAPClient())
    d.addCallback(onConnect, config["group"], config["user"], config["modifyAction"])
    d.addErrback(onError, reactor)
    return d


react(main)
