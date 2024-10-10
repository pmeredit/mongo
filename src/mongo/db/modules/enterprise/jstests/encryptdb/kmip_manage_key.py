import argparse
import logging
import sys

from kmip.core.enums import (
    CryptographicAlgorithm,
    KMIPVersion,
    RevocationReasonCode,
    State,
)
from kmip.pie.client import ProxyKmipClient

LOGGER = logging.getLogger(__name__)
STATE_ATTRIBUTE = "State"


def createClientConnection(kmip_port, kmip_version):
    client = ProxyKmipClient(
        hostname="127.0.0.1",
        port=kmip_port,
        key="jstests/libs/trusted-client.pem",
        cert="jstests/libs/trusted-client.pem",
        ssl_version="PROTOCOL_SSLv23",
        ca="jstests/libs/trusted-ca.pem",
        kmip_version=kmip_version,
    )
    return client


def getVersionEnum(versionString):
    if versionString == "1.0":
        return KMIPVersion.KMIP_1_0
    elif versionString == "1.2":
        return KMIPVersion.KMIP_1_2
    else:
        LOGGER.error(
            "We expect version to be 1.0 or 1.2, but got version string: %s", versionString
        )
        sys.exit(1)


def makeKey(args):
    LOGGER.info("Creating KMIP key")
    with createClientConnection(args.kmipPort, getVersionEnum(args.version)) as client:
        uid = client.create(CryptographicAlgorithm.AES, 256)
        LOGGER.info("Key created")
        LOGGER.info("UID=<" + uid + ">")
        LOGGER.info("KEY=<" + client.get(uid).value.hex() + ">")


def activateKey(args):
    LOGGER.info("Activate KMIP key")
    uid = args.uid
    with createClientConnection(args.kmipPort, getVersionEnum(args.version)) as client:
        client.activate(uid)
        LOGGER.info("Key activated")
        LOGGER.info("UID=<" + uid + ">")
        LOGGER.info("KEY=<" + client.get(uid).value.hex() + ">")
        LOGGER.info("Successfully activated KMIP Key")


def getStateAttribute(args):
    LOGGER.info("Getting 'State' attribute")
    with createClientConnection(args.kmipPort, getVersionEnum(args.version)) as client:
        objectUid, attributeList = client.get_attributes(str(args.uid), [STATE_ATTRIBUTE])
        LOGGER.info("Object UID(" + objectUid + ")")
        if len(attributeList) != 1:
            LOGGER.error("We are expecting 1 'State' attribute, %d returned.", len(attributeList))
            sys.exit(1)

        attribute = attributeList[0]
        LOGGER.info("Got attribute <" + str(attribute.attribute_name) + ">")
        LOGGER.info("Attribute Value <" + str(attribute.attribute_value) + ">")
        LOGGER.info("IS_ACTIVE=<" + str(str(attribute.attribute_value) == str(State.ACTIVE)) + ">")


def deactivateKMIPKey(args):
    LOGGER.info("Deactivating KMIP Key")
    with createClientConnection(args.kmipPort, getVersionEnum(args.version)) as client:
        client.revoke(RevocationReasonCode.CESSATION_OF_OPERATION, args.uid)
    LOGGER.info("Successfully Deactivated KMIP Key")


def main() -> None:
    logging.basicConfig(format="[%(levelname)s] %(name)s: %(message)s", level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="KMIP key manager.")
    parser.add_argument("--kmipPort", type=int, default=6666, help="KMIP server port")
    parser.add_argument("--version", type=str, default="1.2", help="KMIP version")
    sub = parser.add_subparsers(title="KMIP Manage Key subcommands", help="sub-command help")

    create_key_cmd = sub.add_parser("create_key", help="Create Key")
    create_key_cmd.set_defaults(func=makeKey)

    activate_key_cmd = sub.add_parser("activate_key", help="Create Key")
    activate_key_cmd.add_argument("--uid", required=True, type=str, help="Key UID")
    activate_key_cmd.set_defaults(func=activateKey)

    get_attributes_cmd = sub.add_parser("get_state_attribute", help="Get State Attribute")
    get_attributes_cmd.add_argument("--uid", required=True, type=str, help="Key UID")
    get_attributes_cmd.set_defaults(func=getStateAttribute)

    deactivate_cmd = sub.add_parser("deactivate_kmip_key", help="Deactivate KMIP Key")
    deactivate_cmd.add_argument("--uid", required=True, type=str, help="Key UID")
    deactivate_cmd.set_defaults(func=deactivateKMIPKey)

    args = parser.parse_args()
    args.func(args)

    LOGGER.info("Finished KMIP Actions")


if __name__ == "__main__":
    main()
