#!/usr/bin/env python3
"""
Script for testing custom DNS resolver in containers.
"""

import argparse
import logging
import os
import platform
import pprint
import subprocess
import sys
from typing import List

LOGGER = logging.getLogger(__name__)

############################################################################
# Default configuration settings
#

DEFAULT_IMAGE_NAME = "lambda"

DEFAULT_CONTAINER_NAME = "lambda_container"

# RHEL 8 only has podman by default
DOCKER = (
    "podman"
    if os.path.exists("/etc/fedora-release") or os.path.exists("/etc/redhat-release")
    else "docker"
)

PLATFORM = (
    "linux/arm64"
    if platform.machine().startswith("arm") or platform.machine().startswith("aarch")
    else "linux/amd64"
)

DOCKER_FILE_ROOT = "src/mongo/db/modules/enterprise/jstests/streams/lambda_local"

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
                os.path.join(os.getcwd(), os.path.join(DOCKER_FILE_ROOT, "Dockerfile")),
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
    if container in containers:
        return True
    return False


def _start_args(args, extra) -> int:
    return _start(args.container, args.image_name, args.port)


def _start(container: str, image_name: str, port: int) -> int:
    if _is_container_running(container):
        print(f"Container '{container}' already running, not starting it")
        return 1

    if not _build_image(image_name):
        print("ERROR: faild to build image")
        return 1

    return _run_process(
        [
            DOCKER,
            "run",
            "--platform",
            PLATFORM,
            "-p",
            f"{port}:8080",
            "--name",
            container,
            "--detach",
            image_name,
        ]
    )


def _stop_args(args, extra) -> int:
    return _stop(args.container)


def _stop(container: str) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not stopping it")
        _run_process([DOCKER, "rm", container])
        return 0

    print("=====================================================================================")
    print("Dumping Docker Logs")
    print("=====================================================================================")
    _run_process([DOCKER, "logs", container])
    print("=====================================================================================")

    _run_process([DOCKER, "stop", container])

    _run_process([DOCKER, "rm", container])

    return 0


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description="Lambda container tester.")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "-p", "--port", action="store", type=int, help="Lambda Listen Port", default=9000
    )

    sub = parser.add_subparsers(title="Lambda Tester subcommands", help="sub-command help")

    start_cmd = sub.add_parser("start", help="Start the Lambda server")
    start_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to start"
    )
    start_cmd.add_argument(
        "--image_name", type=str, default=DEFAULT_IMAGE_NAME, help="Image name to use for building"
    )
    start_cmd.set_defaults(func=_start_args)

    stop_cmd = sub.add_parser("stop", help="Stop the Lambda server")
    stop_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to stop"
    )
    stop_cmd.set_defaults(func=_stop_args)

    (args, extra) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    sys.exit(args.func(args, extra))


if __name__ == "__main__":
    main()
