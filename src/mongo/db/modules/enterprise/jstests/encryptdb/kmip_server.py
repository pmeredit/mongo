# this file is a thin wrapper around the PyKMIP server
# which is required for some encrypted storage engine tests

import logging
import sys
import tempfile

from kmip.services.server import KmipServer

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

    with tempfile.TemporaryDirectory() as dbdirname:
        server = KmipServer(
            hostname="127.0.0.1",
            port=kmip_port,
            key_path="jstests/libs/trusted-server.pem",
            certificate_path="jstests/libs/trusted-server.pem",
            ca_path="jstests/libs/trusted-ca.pem",
            config_path=None,
            policy_path=dbdirname,
            log_path=dbdirname + '/log',
            logging_level='DEBUG',
            database_path=dbdirname + '/tmp.db',
            auth_suite='TLS1.2',
            enable_tls_client_auth=False,
        )

        logger.info(f"Starting KMIP server on port {kmip_port}")

        try:
            server.start()
            server.serve()
        except KeyboardInterrupt as e:
            logger.debug('Shutdown signal received')
        except Exception as e:
            logger.info('Exception received while serving: {0}'.format(e))
        finally:
            server.stop()

        logger.info("Stopping KMIP server")


if __name__ == '__main__':
    main()
