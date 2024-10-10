#!/usr/bin/env python3
"""
Script for assuming an aws role.
"""

import argparse
import logging
import os
import sys
import uuid

import boto3
import botocore.session

LOGGER = logging.getLogger(__name__)

STS_DEFAULT_ROLE_NAME = "arn:aws:iam::579766882180:role/mark.benvenuto"


def _assume_role(role_name):
    if sys.platform == "win32" or sys.platform == "cygwin":
        # These overriden values can be found here
        # https://github.com/boto/botocore/blob/13468bc9d8923eccd0816ce2dd9cd8de5a6f6e0e/botocore/configprovider.py#L49C7-L49C7
        # This is due to the backwards breaking changed python introduced https://bugs.python.org/issue36264
        botocore_session = botocore.session.Session(
            session_vars={
                "config_file": (
                    None,
                    "AWS_CONFIG_FILE",
                    os.path.join(os.environ["HOME"], ".aws", "config"),
                    None,
                ),
                "credentials_file": (
                    None,
                    "AWS_SHARED_CREDENTIALS_FILE",
                    os.path.join(os.environ["HOME"], ".aws", "credentials"),
                    None,
                ),
            }
        )
        boto3.setup_default_session(botocore_session=botocore_session)

    sts_client = boto3.client("sts")

    response = sts_client.assume_role(
        RoleArn=role_name, RoleSessionName=str(uuid.uuid4()), DurationSeconds=900
    )

    creds = response["Credentials"]

    print(f"""{{
  "AccessKeyId" : "{creds["AccessKeyId"]}",
  "SecretAccessKey" : "{creds["SecretAccessKey"]}",
  "SessionToken" : "{creds["SessionToken"]}",
  "Expiration" : "{str(creds["Expiration"])}"
}}""")


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description="Assume Role frontend.")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")

    parser.add_argument(
        "--role_name", type=str, default=STS_DEFAULT_ROLE_NAME, help="Role to assume"
    )

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    _assume_role(args.role_name)


if __name__ == "__main__":
    main()
