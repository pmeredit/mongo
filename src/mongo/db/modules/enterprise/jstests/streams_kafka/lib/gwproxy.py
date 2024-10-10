#!/usr/bin/env python3
"""
Script for setting up a local GWProxy container using docker.
"""

import argparse
import base64
import logging
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List

import boto3
from botocore.credentials import InstanceMetadataFetcher, InstanceMetadataProvider

LOGGER = logging.getLogger(__name__)
DOCKER = "docker"
if os.path.exists("/etc/fedora-release") or os.path.exists("/etc/redhat-release"):
    # podman is used on fedora and redhat machines.
    DOCKER = "podman"


# Add some custom exceptions to handle shell calls to docker/podman.
class GWProxyException(Exception):
    pass


class FailToStartException(GWProxyException):
    pass


class FailToStopException(GWProxyException):
    pass


class FailToRemoveException(GWProxyException):
    pass


class FailToLoginException(GWProxyException):
    pass


class FailToPullException(GWProxyException):
    pass


class FailToConnectException(GWProxyException):
    pass


@dataclass
class AWSCredentials:
    """AWS Credentials."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str

    @classmethod
    def FromPayload(cls, credentials):
        return cls(
            credentials["AccessKeyId"], credentials["SecretAccessKey"], credentials["SessionToken"]
        )

    @classmethod
    def FromAPI(cls, credentials):
        return cls(credentials.access_key, credentials.secret_key, credentials.token)

    @classmethod
    def FromEnv(cls):
        key_id = os.getenv("AWS_ACCESS_KEY_ID")
        key = os.getenv("AWS_SECRET_ACCESS_KEY")
        token = os.getenv("AWS_SESSION_TOKEN")

        if key_id and key and token:
            return cls(key_id, key, token)

        return None


class GWProxyManager(object):
    # Non-routable/RFC1918 IPv4 address to bind GWProxy to (to avoid conflicting with Kafka)
    SERVER_IP = "172.20.100.10"

    # Build path to the directory storing certificates to map into docker containers
    GWPROXY_CONFIG_PATH = os.path.join(
        os.getcwd(), "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/gwproxy_configs"
    )

    # In production, an iptables catch-all dnat rule redirects to this.
    GWPROXY_PORT = 30000
    GWPROXY_CONTAINER_NAME = "gwproxytest"

    # Configurables to connect to SRE ECR repo.
    ECR_REGISTRY_ID = "664315256653"
    ECR_REGION = "us-east-1"
    ECR_REPOSITORY_NAME = "sre/gwproxy"
    ECR_NAME = "{}.dkr.ecr.{}.amazonaws.com".format(ECR_REGISTRY_ID, ECR_REGION)

    # This ARN assumes that the test is running on an evergreen node in the mongo server project,
    # so has access to assume this role.
    ECR_ROLE_ARN = "arn:aws:iam::664315256653:role/streams-evergreen-ecr-access-ro"

    def __init__(self):
        self.ecr_client = None
        self.ecr_registry = None

    def start_gwproxy(self) -> bool:
        """Start a local GWProxy server."""
        # Add interface alias
        self.add_interface_alias()

        # Create a client connection if we don't already have one.
        if self.ecr_client is None:
            self.login_to_ecr()

        # Pull latest image.
        image = self.get_latest_gwproxy_image()

        # Run GWProxy.
        print("using volume path: {}".format(self.GWPROXY_CONFIG_PATH))
        ret = self._run_process(
            [
                DOCKER,
                "run",
                "--network=host",
                "--volume={}:/config:ro".format(self.GWPROXY_CONFIG_PATH),
                "-d",
                f"--name={self.GWPROXY_CONTAINER_NAME}",
                image,
                "--config",
                "/config/gwproxy.yml",
            ]
        )

        if ret != 0:
            raise FailToStartException(f"Failed to start GWProxy container, returned {ret}.")

        return self._wait_for_port(self.GWPROXY_PORT)

    def stop_gwproxy(self) -> bool:
        """Stop running GWProxy containers and remove them."""
        ret = self._run_process([DOCKER, "stop", self.GWPROXY_CONTAINER_NAME])

        if ret != 0:
            raise FailToStopException(f"Failed to stop GWProxy container, returned {ret}.")

        # also remove container.
        ret = self._run_process([DOCKER, "rm", self.GWPROXY_CONTAINER_NAME])

        if ret != 0:
            raise FailToRemoveException(f"Failed to remove GWProxy container, returned {ret}.")

        return True

    def add_interface_alias(self) -> int:
        """This adds an additional non-routable RFC1918 IP to the lo loopback interface
        to allow us to bind gwproxy to the Kafka port, without colliding with the
        Kafka docker containers (which use --net=host).
        """
        ret = self._run_process(["sudo", "ip", "addr", "add", self.SERVER_IP + "/32", "dev", "lo"])

        if ret == 0:
            # success
            print("Added IP alias to loopback.")
        elif ret == 2:
            # RTNETLINK answers: File exists
            print("IP alias already exists on loopback, moving on.")

        return ret

    def set_credentials(self) -> None:
        """Populate the credentials cache, either from environment variables or sts assume."""
        # Allow users to provide their own keys (instead of assuming a role) if testing locally, etc.
        self.credentials = AWSCredentials.FromEnv()
        if self.credentials:
            print("AWS: Found user credentials in environment, using those.")
            return

        # No keys provided - create STS client and attempt to assume role.
        print("AWS: Attempting to assume role with instance profile.")
        provider = InstanceMetadataProvider(
            iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2)
        )
        sts_credentials = AWSCredentials.FromAPI(provider.load().get_frozen_credentials())

        sts_client = boto3.client(
            "sts",
            region_name=self.ECR_REGION,
            aws_access_key_id=sts_credentials.aws_access_key_id,
            aws_secret_access_key=sts_credentials.aws_secret_access_key,
            aws_session_token=sts_credentials.aws_session_token,
        )

        # assume our read-only streams ECR role.  note: this will throw a
        # botocore.exceptions.ClientError if we can't assume the role.
        assumed_role_object = sts_client.assume_role(
            RoleArn=self.ECR_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
        )

        # get temporary credentials.
        credentials = assumed_role_object["Credentials"]

        self.credentials = AWSCredentials.FromPayload(credentials)

    def ensure_ecr_session(self) -> None:
        """Create a new ECR Client if one does not exist."""
        if not self.ecr_client:
            # populate credentials cache.
            self.set_credentials()

            # configure ECR client, and get an authorization token.
            self.ecr_client = boto3.client(
                "ecr",
                aws_access_key_id=self.credentials.aws_access_key_id,
                aws_secret_access_key=self.credentials.aws_secret_access_key,
                aws_session_token=self.credentials.aws_session_token,
                region_name=self.ECR_REGION,
            )

    def login_to_ecr(self) -> None:
        """Ensure we have a current ECR session."""
        self.ensure_ecr_session()

        res = self.ecr_client.get_authorization_token(registryIds=[self.ECR_REGISTRY_ID])

        # extract a username and password that we can pass to docker/podman to login with.
        username, password = (
            base64.b64decode(res["authorizationData"][0]["authorizationToken"]).decode().split(":")
        )
        registry = res["authorizationData"][0]["proxyEndpoint"]

        # podman doesn't support --password-stdin (with the version we are running), so we have
        # to pass the password here.
        proc = subprocess.Popen(
            [DOCKER, "login", "--username", username, "--password", password, registry],
            stdout=subprocess.PIPE,
            text=True,
        )
        docker_stdout, ret = proc.communicate()

        if proc.returncode != 0:
            # login failed
            raise FailToLoginException(
                f"Failed to log in to ECR docker repository (return code {ret}): {docker_stdout}"
            )

    def get_latest_gwproxy_image(self) -> str:
        """Find latest GWProxy image, and download it."""
        self.ensure_ecr_session()

        # Get a list of all available gwproxy images
        response = self.ecr_client.describe_images(
            registryId=self.ECR_REGISTRY_ID, repositoryName=self.ECR_REPOSITORY_NAME
        )

        image_record = sorted(response["imageDetails"], key=lambda d: d["imagePushedAt"])[-1]
        image = "{}/{}@{}".format(
            self.ECR_NAME, self.ECR_REPOSITORY_NAME, image_record["imageDigest"]
        )

        print(f"downloading image: {image}")

        ret = self._run_process([DOCKER, "pull", image])
        if ret != 0:
            raise FailToPullException(f"Failed to pull gwproxy docker image with error code: {ret}")

        return image

    @staticmethod
    def _run_process_base(params: List[str], **kwargs: Any) -> Any:
        LOGGER.info("RUNNING COMMAND: %s", params)
        ret = subprocess.run(params, **kwargs)
        return ret

    @staticmethod
    def _run_process(params: List[str], **kwargs: Any) -> Any:
        ret = __class__._run_process_base(params, **kwargs)
        return ret.returncode

    def _wait_for_port(self, port: int, timeout_secs: int = 10) -> bool:
        """Loop and wait for the server to start listening on the specified port."""
        start = time.time()
        while time.time() - start <= timeout_secs:
            try:
                print(f"trying port {port}")
                serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                serv.connect((self.SERVER_IP, port))
                print("connected")
                return True
            except Exception as exception:
                print(f"Unable to connect to port {port}: {exception}")
                time.sleep(1)
            finally:
                serv.close()
        if start - time.time() > timeout_secs:
            raise FailToConnectException(f"Timeout elapsed while waiting for port {port}")


def start(args) -> int:
    """Start gwproxy."""
    mgr = GWProxyManager()

    try:
        mgr.start_gwproxy()
    except GWProxyException as ex:
        print(f"Failed to start gwproxy: {ex}")
        sys.exit(1)


def stop(args) -> int:
    """Shut down test environment."""
    mgr = GWProxyManager()

    try:
        mgr.stop_gwproxy()
    except GWProxyException as ex:
        print(f"Failed to stop gwproxy (may have already been stopped): {ex}")

    sys.exit(0)


def main() -> None:
    """Execute Main entry point."""

    path = Path(__file__)
    os.chdir(path.parent.absolute())

    parser = argparse.ArgumentParser(description="GWProxy server.")

    sub = parser.add_subparsers(title="GWProxy container subcommands", help="sub-command help")

    start_cmd = sub.add_parser("start", help="Start the GWProxy server")
    start_cmd.set_defaults(func=start)

    stop_cmd = sub.add_parser("stop", help="Stop the GWProxy server")
    stop_cmd.set_defaults(func=stop)

    (args, _) = parser.parse_known_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
