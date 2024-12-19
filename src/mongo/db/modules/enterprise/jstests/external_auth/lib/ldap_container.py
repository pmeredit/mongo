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

# RHEL 8 only has podman by default
DOCKER = (
    "podman"
    if os.path.exists("/etc/fedora-release") or os.path.exists("/etc/redhat-release")
    else "docker"
)

DOCKER_FILE_ROOT = "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_dns"

############################################################################


def _run_process(params, cwd=None) -> int:
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, cwd=cwd)
    return ret.returncode


def _run_process_capture(params, cwd=None):
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, cwd=cwd, check=True, capture_output=True)
    if ret != 0:
        print("RUN PROCESS FAILED: %s" % (ret))
        print("OUTPUT: %s\n%s" % (ret.stdout, ret.stderr))
        _run_process([DOCKER, "info"])

    return ret.stdout


def _build_image(image_name: str) -> bool:
    # podman build -t image_name directory
    retry = 0

    OS_DEFINE = "UBUNTU"
    if os.path.exists("/etc/fedora-release"):
        OS_DEFINE = "FEDORA"
    elif os.path.exists("/etc/redhat-release"):
        OS_DEFINE = "RHEL"

    subprocess.check_call(
        'grep -v "^# " %s/Dockerfile | cpp -D%s -E > %s/Dockerfile.pp'
        % (DOCKER_FILE_ROOT, OS_DEFINE, DOCKER_FILE_ROOT),
        shell=True,
    )

    # Retry the image build several times in case of network errors for image pulls
    # There is no way to know if a build fails because of an image pull issue or
    # simply a bug in the Dockerfile though
    while retry < 5:
        ret = _run_process(
            [
                DOCKER,
                "build",
                "-t",
                image_name,
                "-f",
                os.path.join(os.getcwd(), os.path.join(DOCKER_FILE_ROOT, "Dockerfile.pp")),
                os.path.join(os.getcwd(), DOCKER_FILE_ROOT),
            ]
        )

        if ret == 0:
            return True

        LOGGER.warning("Build image attempt: %i failed", retry + 1)
        retry += 1

    return False


def _list_containers() -> List[str]:
    # podman container ls --format=json
    containers = (
        _run_process_capture([DOCKER, "container", "ls", "-a", "--format={{.Names}}"])
        .decode("utf-8")
        .splitlines()
    )

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

    # podman run --rm  --dns=127.0.0.1 -v "$(pwd)":/app:z  --name dns_test --help  -it dns_client
    app_path = os.getcwd()
    _run_process(
        [
            # --rm with --detach is not supported for the RHEL 8 version of podman
            DOCKER,
            "run",
            "--cap-add",
            "SYS_PTRACE",
            "--log-driver",
            "json-file",
            "--dns=127.0.0.1",
            "--volume",
            app_path + ":/app:z",
            "--name",
            container,
            "--detach",
            image_name,
        ]
    )

    # Wait for container to start by running dig
    return _run_process(
        [
            DOCKER,
            "exec",
            "-w",
            "/app",
            container,
            "/usr/bin/dig",
            "@127.0.0.1",
            "foo.mock.mongodb.org",
            "+timeout=3",
            "+tries=10",
        ]
    )


def _stop_args(args, extra) -> int:
    return _stop(args.container)


def _stop(container: str) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not stopping it")
        return 0

    print("=====================================================================================")
    print("Dumping Docker Logs")
    print("=====================================================================================")
    _run_process([DOCKER, "logs", container])
    print("=====================================================================================")

    _run_process([DOCKER, "stop", container])

    _run_process([DOCKER, "rm", container])

    return 0


def _run_args(args, extra) -> int:
    return _run(args.container, extra)


def _run(container: str, cmd: List[str]) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not running command")
        return 1

    # podman exec -it dns_test  /bin/bash
    # For OpenLDAP, disable certificate validation and certificate SAN checks because the
    # certificate does not match our mock dns entries
    return _run_process(
        [
            DOCKER,
            "exec",
            "-e",
            "LDAPTLS_REQCERT=never",
            "-e",
            "LDAPTLS_REQSAN=never",
            "-w",
            "/app",
            container,
        ]
        + cmd
    )


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description="LDAP container tester.")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")

    sub = parser.add_subparsers(title="LDAP Tester subcommands", help="sub-command help")

    start_cmd = sub.add_parser("start", help="Start the LDAP server")
    start_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to start"
    )
    start_cmd.add_argument(
        "--image_name", type=str, default=DEFAULT_IMAGE_NAME, help="Image name to use for building"
    )
    start_cmd.set_defaults(func=_start_args)

    stop_cmd = sub.add_parser("stop", help="Stop the LDAP server")
    stop_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to stop"
    )
    stop_cmd.set_defaults(func=_stop_args)

    run_cmd = sub.add_parser("run", help="Run the command in the container")
    run_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to run"
    )
    run_cmd.set_defaults(func=_run_args)

    (args, extra) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    sys.exit(args.func(args, extra))


if __name__ == "__main__":
    main()
