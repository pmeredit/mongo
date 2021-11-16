import argparse
import logging
import sys
from kmip.core.enums import CryptographicAlgorithm, State
from kmip.pie.client import ProxyKmipClient


LOGGER = logging.getLogger(__name__)
STATE_ATTRIBUTE = "State"

def createClientConnection(kmip_port):
    client = ProxyKmipClient(
        hostname="127.0.0.1",
        port=kmip_port,
        key="jstests/libs/trusted-client.pem",
        cert="jstests/libs/trusted-client.pem",
        ssl_version="PROTOCOL_SSLv23",
        ca="jstests/libs/trusted-ca.pem",
    )
    return client

def makeKey(args):
    LOGGER.info("Creating KMIP key")
    client = createClientConnection(args.kmipPort)
    with client:
        uid = client.create(CryptographicAlgorithm.AES, 256)
        client.activate(uid)
        LOGGER.info("Key created")
        LOGGER.info("UID=<" + uid + ">")
        LOGGER.info("KEY=<" + client.get(uid).value.hex() + ">")

def getStateAttribute(args):
    LOGGER.info("Getting 'State' attribute")
    client = createClientConnection(args.kmipPort)
    with client:
        objectUid, attributeList = client.get_attributes(str(args.uid), [STATE_ATTRIBUTE])
        LOGGER.info("Object UID(" + objectUid + ")")
        if (len(attributeList) != 1):
            LOGGER.error("We are expecting 1 'State' attribute, %d returned.", len(attributeList))
            sys.exit(1)

        attribute = attributeList[0]
        LOGGER.info("Got attribute <" + str(attribute.attribute_name) + ">")
        LOGGER.info("Attribute Value <" + str(attribute.attribute_value) + ">")
        LOGGER.info("IS_ACTIVE=<" + str(str(attribute.attribute_value) == str(State.ACTIVE)) + ">")

def main() -> None:
    logging.basicConfig(format="[%(levelname)s] %(name)s: %(message)s", level=logging.DEBUG)
    
    parser = argparse.ArgumentParser(description='KMIP key manager.')
    parser.add_argument('--kmipPort', type=int, default=6666, help="KMIP server port")
    sub = parser.add_subparsers(title="KMIP Manage Key subcommands", help="sub-command help")
    
    create_key_cmd = sub.add_parser('create_key', help='Create Key')
    create_key_cmd.set_defaults(func=makeKey)

    get_attributes_cmd = sub.add_parser('get_state_attribute', help='Get State Attribute')
    get_attributes_cmd.add_argument("--uid", required=True, type=str, help="Key UID")
    get_attributes_cmd.set_defaults(func=getStateAttribute)

    args = parser.parse_args()
    args.func(args)

    LOGGER.info("Finished KMIP Actions")

if __name__ == '__main__':
    main()
