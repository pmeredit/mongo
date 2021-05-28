# this file is a thin wrapper around the PyKMIP server
# which is required for some encrypted storage engine tests

import logging
import sys

from kmip.services.kmip_server import KMIPServer

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

    server = KMIPServer(
        host="127.0.0.1",
        port=kmip_port,
        keyfile="jstests/libs/trusted-server.pem",
        certfile="jstests/libs/trusted-server.pem",
        cert_reqs="CERT_REQUIRED",
        ssl_version="PROTOCOL_SSLv23",
        ca_certs="jstests/libs/trusted-ca.pem",
        do_handshake_on_connect=True,
        suppress_ragged_eofs=True)

    logger.info("Starting KMIP server")

    try:
        server.serve()
    except KeyboardInterrupt as e:
        logger.debug('Shutdown signal received')
    except Exception as e:
        logger.info('Exception received while serving: {0}'.format(e))
    finally:
        server.close()

    logger.info("Stopping KMIP server")


if __name__ == '__main__':
    main()
