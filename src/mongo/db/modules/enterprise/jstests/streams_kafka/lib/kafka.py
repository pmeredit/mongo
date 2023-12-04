#!/usr/bin/env python3
"""
Script for setting up a local Kafka broker container using docker.
"""

import argparse
import csv
from io import StringIO
import json
import logging
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

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
BITNAMI_KAFKA_CONTAINER_IMAGE = "bitnami/kafka"
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
KAFKA_UTILITY_ARGS = [
    DOCKER,
    "run",
    "--network=host",
    BITNAMI_KAFKA_CONTAINER_IMAGE
]

def _get_kafka_start_args(partition_count):
    return [
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
        "-e",
        f"KAFKA_NUM_PARTITIONS={partition_count}",
        "-d",
        f"--name={KAFKA_CONTAINER_NAME}",
        KAFKA_CONTAINER_IMAGE,
    ]


def _run_process_base(params: List[str], **kwargs: Any) -> Any:
    LOGGER.info("RUNNING COMMAND: %s", params)
    ret = subprocess.run(params, **kwargs)
    return ret

def _run_process(params: List[str], **kwargs: Any) -> Any:
    ret = _run_process_base(params, **kwargs)
    return ret.returncode

def _run_process_capture(params: List[str], **kwargs: Any) -> Any:
    ret = _run_process_base(params, universal_newlines=True, capture_output=True, **kwargs)
    return ret

def _wait_for_port(port: int, timeout_secs: int = 10):
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


def start(args) -> int:
    # Start zookeeper
    ret = _run_process(ZOOKEEPER_START_ARGS)
    if ret != 0:
        raise RuntimeError('Failed to start zookeeper with error code: {ret}')
    _wait_for_port(ZOOKEEPER_PORT)
    # Start kafka
    ret = _run_process(_get_kafka_start_args(args.partitions))
    if ret != 0:
        raise RuntimeError('Failed to start kafka with error code: {ret}')
    _wait_for_port(KAFKA_PORT)


def stop(args) -> int:
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

def get_consumer_group(args) -> List[Any]:
    cmd: List[str] = KAFKA_UTILITY_ARGS.copy()
    cmd.extend(["--", "kafka-consumer-groups.sh", "--bootstrap-server", f"localhost:{KAFKA_PORT}", "--describe", "--group", args.group_id])
    ret = _run_process_capture(cmd)
    if ret != 0:
        print(f'Failed to run kafka-consumer-groups.sh with error code: {ret}')

    # Transform fixed width table from stdout into JSON.
    out: str = re.sub("[^\S\r\n]+", " ", ret.stdout)
    lines: List[str] = [line.strip() for line in out.split("\n") if len(line) > 0]
    normalized: str = "\n".join(lines)
    reader = csv.reader(StringIO(normalized), delimiter=" ")
    rawRows: List[List[str]] = list(reader)

    # Transform header names into snakecase.
    headers = [name.lower().replace("-", "_") for name in rawRows[0]]
    rows: List[Dict[str, Any]] = []
    for row in rawRows[1:]:
        obj: Dict[str, Any] = {headers[idx]: value for idx, value in enumerate(row)}
        rows.append(obj)

    print(json.dumps(rows))

def main() -> None:
    """Execute Main entry point."""

    path = Path(__file__)
    os.chdir(path.parent.absolute())

    parser = argparse.ArgumentParser(description='Kafka container tester.')

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")
    parser.add_argument('-d', "--debug", action='store_true', help="Enable debug logging")
    parser.add_argument('-p', "--partitions", help="Partition count used by the broker.")

    sub = parser.add_subparsers(title="Kafka container subcommands", help="sub-command help")

    start_cmd = sub.add_parser('start', help='Start the Kafka broker')
    start_cmd.set_defaults(func=start)

    stop_cmd = sub.add_parser('stop', help='Stop the Kafka broker')
    stop_cmd.set_defaults(func=stop)

    get_consumer_group_cmd = sub.add_parser('get-consumer-group', help='Gets the state for the input consumer group from the kafka broker')
    get_consumer_group_cmd.add_argument(
        "--group-id",
        required=True,
        type=str,
        help="Consumer group ID to fetch"
    )
    get_consumer_group_cmd.set_defaults(func=get_consumer_group)

    (args, _) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
