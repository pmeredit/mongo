#!/usr/bin/env python3
"""
Script for spinning up a MinIO container for S3 $emit testing
"""

import argparse
import logging
import os
import platform
import pprint
import subprocess
import sys
import time
from typing import List

LOGGER = logging.getLogger(__name__)

############################################################################
# Default configuration settings
#

IMAGE_NAME = "s3"

DEFAULT_CONTAINER_NAME = "s3_container"

DEFAULT_MINIO_ALIAS = "myminio"
DEFAULT_BUCKET = "jstest"
DEFAULT_NAMESPACE = f"{DEFAULT_MINIO_ALIAS}/{DEFAULT_BUCKET}"

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

DOCKER_FILE_ROOT = "src/mongo/db/modules/enterprise/jstests/streams/s3_local"

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
                os.path.join(os.getcwd(), DOCKER_FILE_ROOT, "Dockerfile"),
                os.path.join(os.getcwd(), DOCKER_FILE_ROOT),
            ]
        )

        if ret == 0:
            return True

        LOGGER.warning("Build image attempt: %i failed", retry + 1)
        time.sleep(1)
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
    return _start(args.container)


def _start(container: str) -> int:
    if _is_container_running(container):
        print(f"Container '{container}' already running, not starting it")
        return 1

    if not _build_image(IMAGE_NAME):
        print("ERROR: faild to build image")
        return 1

    ret = _run_process(
        [
            DOCKER,
            "run",
            "--platform",
            PLATFORM,
            "-p",
            "9000:9000",
            "-p",
            "9001:9001",
            "--name",
            container,
            "--detach",
            IMAGE_NAME,
        ]
    )
    if ret != 0:
        print("ERROR: Failed to run docker container")
        return ret

    _wait_until_container_ready(container)


READY_STRING = "MinIO admin setup complete!"


def _wait_until_container_ready(container):
    i = 0
    out = ""
    while i < 5:
        out = str(_run_process_capture([DOCKER, "logs", container]))
        if READY_STRING in out:
            return
        time.sleep(0.5)
        i += 1

    print("ERROR: docker container wasn't ready in time.")
    return 0


def _clean_bucket_args(args, extra) -> int:
    return _clean_bucket(args.container, args.bucket)


def _clean_bucket(container, bucket):
    _run_process([DOCKER, "exec", container, "mc", "rm", "--recursive", "--force", bucket])


def _stop_args(args, extra) -> int:
    return _stop(args.container)


def _stop(container: str) -> int:
    if not _is_container_running(container):
        print(f"Container '{container}' not running, not stopping it")
        _run_process([DOCKER, "rm", container])
        return 0

    print("==================================================================")
    print("Dumping Docker Logs")
    print("==================================================================")
    _run_process([DOCKER, "logs", container])
    print("==================================================================")

    _run_process([DOCKER, "stop", container])

    _run_process([DOCKER, "rm", container])

    return 0


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description="S3 container tester.")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    sub = parser.add_subparsers(title="S3 Tester subcommands", help="sub-command help")

    # start
    start_cmd = sub.add_parser("start", help="Start the S3 server")
    start_cmd.add_argument(
        "--container", type=str, default=DEFAULT_CONTAINER_NAME, help="Container to start"
    )
    start_cmd.set_defaults(func=_start_args)

    # clean_bucket
    clean_bucket_cmd = sub.add_parser("clean_bucket", help="Clean the specified S3 bucket")
    clean_bucket_cmd.add_argument(
        "--container",
        type=str,
        default=DEFAULT_CONTAINER_NAME,
        help="Container with bucket to clean",
    )
    clean_bucket_cmd.add_argument(
        "--bucket",
        type=str,
        default=DEFAULT_NAMESPACE,
        help="Bucket (<minio alias>/<bucket>) to clean",
    )
    clean_bucket_cmd.set_defaults(func=_clean_bucket_args)

    # stop
    stop_cmd = sub.add_parser("stop", help="Stop the S3 server")
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
