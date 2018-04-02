# this file is a thin wrapper around the PyKMIP server
# which is required for some encrypted storage engine tests

import logging

from kmip.services.kmip_server import KMIPServer

def main():

    logger = logging.getLogger(__name__)

    server = KMIPServer(
        host="127.0.0.1",
        port=6666,
        keyfile="jstests/libs/server.pem",
        certfile="jstests/libs/server.pem",
        cert_reqs="CERT_REQUIRED",
        ssl_version="PROTOCOL_SSLv23",
        ca_certs="jstests/libs/ca.pem",
        do_handshake_on_connect=True,
        suppress_ragged_eofs=True)

    logger.info("Starting KMIP server")

    try:
        server.serve()
    except Exception as e:
        logger.info('Exception received while serving: {0}'.format(e))
    finally:
        server.close()

    logger.info("Stopping KMIP server")


if __name__ == '__main__':
    main()
