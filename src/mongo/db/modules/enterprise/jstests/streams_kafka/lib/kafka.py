#!/usr/bin/env python3
"""
Script for setting up a local Kafka broker container using docker.
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import socket
import subprocess
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List

LOGGER = logging.getLogger(__name__)
DOCKER = "docker"
if os.path.exists("/etc/fedora-release") or os.path.exists("/etc/redhat-release"):
    # podman is used on fedora and redhat machines.
    DOCKER = "podman"

# Build path to the directory storing certificates to map into docker containers
CERT_DIRECTORY_PATH = os.path.join(
    os.getcwd(), "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs"
)

# If we can't locate the certs, Kafka won't start, so identify this early.
if os.path.exists(CERT_DIRECTORY_PATH):
    print(f"Using certificate directory: {CERT_DIRECTORY_PATH}")
else:
    raise RuntimeError(f"Failed to locate certificate directory: {CERT_DIRECTORY_PATH}")

KAFKA_CONTAINER_NAME = "streamskafkabroker"
KAFKA_CONTAINER_IMAGE = "confluentinc/cp-kafka:7.0.1"
KAFKA_PORT = 9092
ZOOKEEPER_CONTAINER_NAME = "streamszookeeper"
ZOOKEEPER_CONTAINER_IMAGE = "confluentinc/cp-zookeeper:7.0.1"
ZOOKEEPER_PORT = 2181
BITNAMI_KAFKA_CONTAINER_IMAGE = "bitnami/kafka:3.5.2-debian-11-r24"
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
    "--volume={}:/etc/kafka/secrets:ro".format(CERT_DIRECTORY_PATH),
    "-e",
    "KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/zookeeper_server_jaas.conf -Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider -Dzookeeper.allowSaslFailedClients=false -Dzookeeper.requireClientAuthScheme=sasl",
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
KAFKA_UTILITY_ARGS = [DOCKER, "run", "--network=host", BITNAMI_KAFKA_CONTAINER_IMAGE]


def _get_kafka_start_args(partition_count, broker_config_overrides):
    kafka_start_args = [
        DOCKER,
        "run",
        "--network=host",
        "--volume={}:/etc/kafka/secrets:ro".format(CERT_DIRECTORY_PATH),
        "-e",
        f"KAFKA_NUM_PARTITIONS={partition_count}",
    ]

    kafka_broker_config = {
        "KAFKA_BROKER_ID": "1",
        "KAFKA_ZOOKEEPER_CONNECT": f"localhost:{ZOOKEEPER_PORT}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,SASL_SSL:SASL_SSL",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{KAFKA_PORT},PLAINTEXT_INTERNAL://broker:29092,SASL_SSL://localhost:9093",
        "KAFKA_SSL_KEYSTORE_FILENAME": "kafka.keystore.pkcs12",
        "KAFKA_SSL_KEYSTORE_CREDENTIALS": "kafka_keystore_creds",
        "KAFKA_SSL_KEY_CREDENTIALS": "kafka_sslkey_creds",
        "KAFKA_OPTS": "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf",
        "KAFKA_LISTENER_NAME_SASL_SSL_SASL_ENABLED_MECHANISMS": "PLAIN",
        "KAFKA_SASL_ENABLED_MECHANISMS": "PLAIN",
        "ZOOKEEPER_SASL_ENABLED": "false",
        "ZOOKEEPER_SASL_CLIENT": "false",
        "KAFKA_ZOOKEEPER_SET_ACL": "false",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
    }

    # Override default broker configurations
    for config_param in broker_config_overrides:
        kafka_broker_config[config_param[0]] = config_param[1]

    # Add broker configurations to start args
    for param, value in kafka_broker_config.items():
        kafka_start_args.append("-e")
        kafka_start_args.append(f"{param}={value}")

    kafka_start_args += [
        "-d",
        f"--name={KAFKA_CONTAINER_NAME}",
        KAFKA_CONTAINER_IMAGE,
    ]

    return kafka_start_args


def _get_kafka_ready(port):
    return [
        DOCKER,
        "exec",
        "streamskafkabroker",
        "kafka-cluster",
        "cluster-id",
        "--bootstrap-server",
        "localhost:" + str(port),
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
            serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            serv.bind(("localhost", port))
            return
        except Exception as exception:
            print(f"Error connecting to port {port}: {exception}")
            time.sleep(1)
        finally:
            serv.close()
    if start - time.time() > timeout_secs:
        raise f"Timeout elapsed while waiting for port {port}"


def _wait_for_kafka_ready(port, timeout_secs=30):
    start = time.time()
    print("Waiting for Kafka to become ready to service queries")

    while time.time() - start <= timeout_secs:
        ret = _run_process(_get_kafka_ready(port))
        if ret == 0:
            print("Kafka is ready!")
            return
        else:
            print("Kafka is still not ready")

        if start - time.time() > timeout_secs:
            raise "Timeout elapsed while waiting for kafka to become ready"

        time.sleep(1)


def start(args) -> int:
    def _start():
        # Start zookeeper
        ret = _run_process(ZOOKEEPER_START_ARGS)
        if ret != 0:
            raise RuntimeError("Failed to start zookeeper with error code: {ret}")
        _wait_for_port(ZOOKEEPER_PORT)
        # Start kafka
        ret = _run_process(_get_kafka_start_args(args.partitions, args.kafka_arg))
        if ret != 0:
            raise RuntimeError("Failed to start kafka with error code: {ret}")
        _wait_for_port(KAFKA_PORT)
        _wait_for_kafka_ready(KAFKA_PORT)

    # https://jira.mongodb.org/browse/BF-33895:
    # On rare ocassion in evergreen, the containers fail to start with an
    # error like: "fatal error: concurrent map writes".
    # Looks like this is fixed in newer versions of docker:
    # https://github.com/docker/compose/issues/10150.
    # Here we add some retry logic to work around this issue.
    max_retries = 2
    retries = 0
    last_exception = None
    while retries < max_retries:
        try:
            _start()
            return 0
        except Exception as e:
            print(f"Start containers failed with ${e}")
            retries += 1
            if retries < max_retries:
                sleep_secs = random.randint(1, 60)
                time.sleep(sleep_secs)
    raise last_exception


def stop(args) -> int:
    ret = _run_process(KAFKA_STOP_ARGS)
    if ret != 0:
        print(f"Failed to stop kafka with error code: {ret}")
    ret = _run_process(ZOOKEEPER_STOP_ARGS)
    if ret != 0:
        print(f"Failed to stop zookeeper with error code: {ret}")
    ret = _run_process(KAFKA_RM_ARGS)
    if ret != 0:
        print(f"Failed to rm kafka with error code: {ret}")
    ret = _run_process(ZOOKEEPER_RM_ARGS)
    if ret != 0:
        print(f"Failed to rm zookeeper with error code: {ret}")


def _kafka_utility_output_to_json(out) -> str:
    # Transform fixed width table from stdout into JSON.
    out: str = re.sub("[^\S\r\n]+", " ", out)
    lines: List[str] = [line.strip() for line in out.split("\n") if len(line) > 0]
    normalized: str = "\n".join(lines)
    reader = csv.reader(StringIO(normalized), delimiter=" ")
    rawRows: List[List[str]] = list(reader)
    if len(rawRows) == 0:
        return []

    # Transform header names into snakecase.
    headers = [name.lower().replace("-", "_") for name in rawRows[0]]
    rows: List[Dict[str, Any]] = []
    for row in rawRows[1:]:
        obj: Dict[str, Any] = {headers[idx]: value for idx, value in enumerate(row)}
        rows.append(obj)

    return rows


def list_consumer_group_members(args):
    cmd: List[str] = KAFKA_UTILITY_ARGS.copy()
    cmd.extend(
        [
            "--",
            "kafka-consumer-groups.sh",
            "--bootstrap-server",
            f"localhost:{KAFKA_PORT}",
            "--describe",
            "--members",
            "--group",
            args.group_id,
        ]
    )
    ret = _run_process_capture(cmd)
    if ret != 0:
        print(f"Failed to run kafka-consumer-groups.sh with error code: {ret}")
    print(json.dumps(_kafka_utility_output_to_json(ret.stdout)))


def get_consumer_group(args) -> List[Any]:
    cmd: List[str] = KAFKA_UTILITY_ARGS.copy()
    cmd.extend(
        [
            "--",
            "kafka-consumer-groups.sh",
            "--bootstrap-server",
            f"localhost:{KAFKA_PORT}",
            "--describe",
            "--group",
            args.group_id,
        ]
    )
    ret = _run_process_capture(cmd)
    if ret != 0:
        print(f"Failed to run kafka-consumer-groups.sh with error code: {ret}")

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


def get_compress_codec_details(args):
    cmd = [
        DOCKER,
        "exec",
        "-it",
        "streamskafkabroker",
        "/bin/kafka-run-class",
        "kafka.tools.DumpLogSegments",
        "--files",
        f"/var/lib/kafka/data/{args.topic}-{args.partition}/00000000000000000000.log",
        "--print-data-log",
    ]
    ret = _run_process_capture(cmd)
    pattern = re.compile(r"compresscodec:\s([^\s]+)")
    for line in ret.stdout.splitlines():
        match = pattern.search(line)
        if match:
            print(match.group(1))


def main() -> None:
    """Execute Main entry point."""

    path = Path(__file__)
    os.chdir(path.parent.absolute())

    parser = argparse.ArgumentParser(description="Kafka container tester.")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-p", "--partitions", help="Partition count used by the broker.", default=1)

    sub = parser.add_subparsers(title="Kafka container subcommands", help="sub-command help")

    start_cmd = sub.add_parser("start", help="Start the Kafka broker")
    start_cmd.add_argument(
        "-ka", "--kafka-arg", required=False, action="append", nargs=2, default=[]
    )
    start_cmd.set_defaults(func=start)

    stop_cmd = sub.add_parser("stop", help="Stop the Kafka broker")
    stop_cmd.set_defaults(func=stop)

    get_consumer_group_cmd = sub.add_parser(
        "get-consumer-group",
        help="Gets the state for the input consumer group from the kafka broker",
    )
    get_consumer_group_cmd.add_argument(
        "--group-id", required=True, type=str, help="Consumer group ID to fetch"
    )
    get_consumer_group_cmd.set_defaults(func=get_consumer_group)

    list_consumer_group_members_cmd = sub.add_parser(
        "list-consumer-group-members", help="Prints a list of active members of a consumer group."
    )
    list_consumer_group_members_cmd.add_argument(
        "--group-id", required=True, type=str, help="Consumer group ID to fetch members"
    )
    list_consumer_group_members_cmd.set_defaults(func=list_consumer_group_members)

    get_compress_codec_details_cmd = sub.add_parser(
        "get-compress-codec-details",
        help="Gets compress codec details in the kafka logs",
    )
    get_compress_codec_details_cmd.add_argument(
        "--topic", required=True, type=str, help="topic to fetch data from"
    )
    get_compress_codec_details_cmd.add_argument(
        "--partition", required=True, type=str, help="partition to fetch data from"
    )
    get_compress_codec_details_cmd.set_defaults(func=get_compress_codec_details)

    (args, _) = parser.parse_known_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
