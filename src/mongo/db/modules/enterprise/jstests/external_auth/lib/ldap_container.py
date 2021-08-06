#!/usr/bin/env python3
"""
Script for testing custom DNS resolver in containers.
"""

import argparse
import logging
import os
import pprint
import subprocess
import sys
from typing import List

LOGGER = logging.getLogger(__name__)

############################################################################
# Default configuration settings
#

DEFAULT_IMAGE_NAME = "ldap_dns"

DEFAULT_CONTAINER_NAME = "ldap_dns_container"

DOCKER = "podman" if os.path.exists("/etc/fedora-release") else "docker"

############################################################################


def _run_process(params, cwd=None) -> int:
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, cwd=cwd)
    return ret.returncode


def _run_process_capture(params, cwd=None):
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, cwd=cwd, check=True, capture_output=True)
    assert ret == 0
    return ret.stdout


def _build_image(image_name: str) -> bool:
    # podman build -t image_name directory
    ret = _run_process([
        DOCKER, "build", "-t", image_name,
        os.path.join(os.getcwd(),
                     "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_dns")
    ])
    return ret == 0


def _list_containers() -> List[str]:
    # podman container ls --format=json
    containers = _run_process_capture([DOCKER, "container", "ls", "-a",
                                       "--format={{.Names}}"]).decode('utf-8').splitlines()

    pprint.pprint(containers)

    return containers


def _is_container_running(container: str) -> bool:
    containers = _list_containers()
    if [c for c in containers if container in c]:
        return True
    return False


def _start_args(args, extra) -> int:
    return _start(args.container, args.image_name)


def _start(container: str, image_name: str) -> int:
    if _is_container_running(container):
        print(f"Container '{container}' already running, not starting it")
        return 1

    if not _build_image(image_name):
        print("ERROR: faild to build image")
        return 1

    #podman run --rm  --dns=127.0.0.1 -v "$(pwd)":/app:z  --name dns_test --help  -it dns_client
    app_path = os.getcwd()
    _run_process([
        DOCKER, "run", "--rm", "--dns=127.0.0.1", "--volume", app_path + ":/app:z", "--name",
        container, "--detach", image_name
    ])

    return 0


def _stop_args(args, extra) -> int:
    return _stop(args.container)


def _stop(container: str) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not stopping it")
        return 0

    _run_process([DOCKER, "stop", container])

    _run_process([DOCKER, "rm", container])

    return 0


def _run_args(args, extra) -> int:
    return _run(args.container, extra)


def _run(container: str, cmd: List[str]) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not running command")
        return 1

    #podman exec -it dns_test  /bin/bash
    return _run_process([DOCKER, "exec", "-w", "/app", container] + cmd)


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description='LDAP container tester.')

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")
    parser.add_argument('-d', "--debug", action='store_true', help="Enable debug logging")

    sub = parser.add_subparsers(title="LDAP Tester subcommands", help="sub-command help")

    start_cmd = sub.add_parser('start', help='Start the LDAP server')
    start_cmd.add_argument("--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to start")
    start_cmd.add_argument("--image_name", type=str, default=DEFAULT_IMAGE_NAME, help="Image name to use for building")
    start_cmd.set_defaults(func=_start_args)

    stop_cmd = sub.add_parser('stop', help='Stop the LDAP server')
    stop_cmd.add_argument("--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to stop")
    stop_cmd.set_defaults(func=_stop_args)

    run_cmd = sub.add_parser('run', help='Run the command in the container')
    run_cmd.add_argument("--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to run")
    run_cmd.set_defaults(func=_run_args)

    (args, extra) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    sys.exit(args.func(args, extra))


if __name__ == "__main__":
    main()
