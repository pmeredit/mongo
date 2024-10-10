#!/usr/bin/env python3
"""
Script for retrieving an ID token from a GCE VM.

Requires ssh on local and remote hosts.
"""

import argparse
import json
import re
import subprocess
import sys

import jwt


def main():
    arg_parser = argparse.ArgumentParser(prog="GCP VM ID Token Acquisition Script")
    arg_parser.add_argument("--config_file", required=True)
    arg_parser.add_argument("--output_file", required=True)
    arg_parser.add_argument("--ssh_key_file", required=True)
    arg_parser.add_argument("--vm_info_file", required=True)

    args = arg_parser.parse_args()

    # Parse the config JSON file to get a map of settings needed to contact the metadata endpoint.
    config = {}
    with open(args.config_file) as config_file:
        config = json.load(config_file)

    # Need to know the GCP VM's external IP that we will connect to to get our token.
    # This was output when we created the VM as part of the setup script.
    gce_vm_info = {}
    with open(args.vm_info_file) as vm_info_file:
        gce_vm_info = json.load(vm_info_file)

    vm_external_ip = gce_vm_info.get("external_ip")
    if not vm_external_ip:
        print("Could not retrieve GCE VM external IP from file at path args.vm_info_file!")
        sys.exit(1)

    user = "evergreen"
    audience = config.get("audience")

    print("Getting token from {}@{}, audience={}".format(user, vm_external_ip, audience))

    ssh_key_file_name = args.ssh_key_file
    with open(ssh_key_file_name, "r", encoding="utf-8") as ssh_key_file:
        contents = ssh_key_file.read()
        if not re.search("^-----BEGIN (RSA|OPENSSH) PRIVATE KEY-----", contents):
            print("ERROR: Did not get a valid key file!!! File contents : [{}]".format(contents))
            sys.exit(1)

    process = subprocess.run(
        [
            "ssh",
            "-v",
            "-v",
            "-v",
            "-i",
            ssh_key_file_name,
            "-o",
            "StrictHostKeyChecking=no",
            "{}@{}".format(user, vm_external_ip),
            "curl",
            "-s",
            "-H",
            '"Metadata-Flavor: Google"',
            '"http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience={}"'.format(
                audience
            ),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Try parsing the output of the ssh command as a JWT. If this succeeds, then the contents of
    # stdout is known to represent a token. Otherwise, populate the error field with the exception
    # for diagnostics.
    token_output = {}
    try:
        token_str = process.stdout.decode()
        _ = jwt.decode(
            jwt=token_str,
            options={"verify_signature": False, "require": ["exp", "iss", "sub", "aud"]},
        )
        token_output["access_token"] = token_str
    except Exception as e:
        # Got an ill-formatted JWT, grab whatever text is in stdout/stderr and write it into a safe
        # json format.
        token_output["error"] = str(e)
        token_output["stdout"] = process.stdout.decode()
        token_output["stderr"] = process.stderr.decode()

    with open(args.output_file, "w+", encoding="utf-8") as out_file:
        out_file.write(json.dumps(token_output))


if __name__ == "__main__":
    sys.exit(main())
