#!/usr/bin/env python3
"""
Script for setting up a local Kafka broker container using docker.
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
import time
import socket

LOGGER = logging.getLogger(__name__)
DOCKER = "docker"
if os.path.exists("/etc/fedora-release") or os.path.exists("/etc/redhat-release"):
    # podman is used on fedora and redhat machines.
    DOCKER = "podman"
KAFKA_CONTAINER_NAME = "streamskafkabroker"
KAFKA_CONTAINER_IMAGE = "confluentinc/cp-kafka:7.0.1"
KAFKA_PORT = 9092
ZOOKEEPER_CONTAINER_NAME = "streamszookeeper"
ZOOKEEPER_CONTAINER_IMAGE = "confluentinc/cp-zookeeper:7.0.1"
ZOOKEEPER_PORT = 2181
KAFKA_START_ARGS = [
    DOCKER,
    "run",
    "--network=host",
    "-e",
    "KAFKA_BROKER_ID=1",
    "-e",
    f"KAFKA_ZOOKEEPER_CONNECT=localhost:{ZOOKEEPER_PORT}",
    "-e",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT",
    "-e",
    f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{KAFKA_PORT},PLAINTEXT_INTERNAL://broker:29092",
    "-e",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "-e",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "-e",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
    "-d",
    f"--name={KAFKA_CONTAINER_NAME}",
    KAFKA_CONTAINER_IMAGE,
]
KAFKA_STOP_ARGS = [
    DOCKER,
    "stop",
    KAFKA_CONTAINER_NAME,
]
KAFKA_RM_ARGS = [
    DOCKER,
    "rm",
    KAFKA_CONTAINER_NAME,
]
ZOOKEEPER_START_ARGS = [
    DOCKER,
    "run",
    "--network=host",
    "-e",
    "ZOOKEEPER_CLIENT_PORT=2181",
    "-e",
    "ZOOKEEPER_TICK_TIME=2000",
    "-d",
    f"--name={ZOOKEEPER_CONTAINER_NAME}",
    ZOOKEEPER_CONTAINER_IMAGE,
]
ZOOKEEPER_STOP_ARGS = [
    DOCKER,
    "stop",
    ZOOKEEPER_CONTAINER_NAME,
]
ZOOKEEPER_RM_ARGS = [
    DOCKER,
    "rm",
    ZOOKEEPER_CONTAINER_NAME,
]


def _run_process(params, cwd=None) -> int:
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, cwd=cwd)
    return ret.returncode


def _wait_for_port(port, timeout_secs=10):
    start = time.time()
    while time.time() - start <= timeout_secs:
        try:
            serv = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            serv.bind(("localhost", port))
            return
        except Exception as exception:
            print(f'Error connecting to port {port}: {exception}')
            time.sleep(1)
        finally:
            serv.close()
    if start - time.time() > timeout_secs:
        raise f'Timeout elapsed while waiting for port {port}'


def _start() -> int:
    # Start zookeeper
    ret = _run_process(ZOOKEEPER_START_ARGS)
    if ret != 0:
        raise RuntimeError('Failed to start zookeeper with error code: {ret}')
    _wait_for_port(ZOOKEEPER_PORT)
    # Start kafka
    ret = _run_process(KAFKA_START_ARGS)
    if ret != 0:
        raise RuntimeError('Failed to start kafka with error code: {ret}')
    _wait_for_port(KAFKA_PORT)


def _stop() -> int:
    ret = _run_process(KAFKA_STOP_ARGS)
    if ret != 0:
        print(f'Failed to stop kafka with error code: {ret}')
    ret = _run_process(ZOOKEEPER_STOP_ARGS)
    if ret != 0:
        print(f'Failed to stop zookeeper with error code: {ret}')
    ret = _run_process(KAFKA_RM_ARGS)
    if ret != 0:
        print(f'Failed to rm kafka with error code: {ret}')
    ret= _run_process(ZOOKEEPER_RM_ARGS)
    if ret != 0:
        print(f'Failed to rm zookeeper with error code: {ret}')


def main() -> None:
    """Execute Main entry point."""

    path = Path(__file__)
    os.chdir(path.parent.absolute())

    parser = argparse.ArgumentParser(description='Kafka container tester.')

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")
    parser.add_argument('-d', "--debug", action='store_true', help="Enable debug logging")

    sub = parser.add_subparsers(title="Kafka container subcommands", help="sub-command help")

    start_cmd = sub.add_parser('start', help='Start the Kafka broker')
    start_cmd.set_defaults(func=_start)

    stop_cmd = sub.add_parser('stop', help='Stop the Kafka broker')
    stop_cmd.set_defaults(func=_stop)

    (args, _) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    sys.exit(args.func())


if __name__ == "__main__":
    main()
