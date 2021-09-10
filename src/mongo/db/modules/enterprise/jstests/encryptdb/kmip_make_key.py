import logging
import sys
from kmip.core.enums import CryptographicAlgorithm
from kmip.pie.client import ProxyKmipClient

def main():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': { 
            'standard': { 
                'format': '[%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': { 
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': { 
            '': {
                'handlers': ['default'],
                'level': 'DEBUG'
            }
        }
    })

    logger = logging.getLogger(__name__)

    kmip_port = 6666
    print(sys.argv)
    if len(sys.argv) >= 2:
        try:
            kmip_port = int(sys.argv[1])
        except ValueError:
            print("KMIP port must be an integer")
            sys.exit(1)

    client = ProxyKmipClient(
        hostname="127.0.0.1",
        port=kmip_port,
        key="jstests/libs/trusted-client.pem",
        cert="jstests/libs/trusted-client.pem",
        ssl_version="PROTOCOL_SSLv23",
        ca="jstests/libs/trusted-ca.pem",
    )

    logger.info("Creating KMIP key")
    with client:
        uid = client.create(CryptographicAlgorithm.AES, 256)
        logger.info("Key created")
        logger.info("UID=<" + uid + ">")
        logger.info("KEY=<" + client.get(uid).value.hex() + ">")

if __name__ == '__main__':
    main()
