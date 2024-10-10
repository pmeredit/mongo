import argparse
import json
import subprocess
import sys

CONST_REMOTE_URL = "127.0.0.1"
CONST_REMOTE_PORT = "45678"
CONST_ENCODING = "utf-8"


def main():
    arg_parser = argparse.ArgumentParser(prog="Azure Container Token-Getter Script")
    arg_parser.add_argument("--config_file", required=True)
    arg_parser.add_argument("--output_file", required=True)
    arg_parser.add_argument("--key_file", required=True)
    arg_parser.add_argument("--hostname_file", required=True)

    args = arg_parser.parse_args()

    # Parse the config json file to get a json map of all our settings
    config = {}
    with open(args.config_file) as config_file:
        config = json.load(config_file)

    # Need to know the azure hostname that we will connect to to get our token - this was output when we enabled ingress for the Azure Container App
    hostname = ""
    with open(args.hostname_file) as hostname_file:
        hostname = hostname_file.read().strip()
        hostname_file.close()

    if not hostname:
        print("Could not find hostname file for the app container at path args.hostname_file!")
        sys.exit(1)

    user = "ubuntu"

    key_file = args.key_file
    resource = config.get("oidc_azure_resource_name")
    object_id = config.get("oidc_azure_object_id")
    api_version = config.get("oidc_azure_managed_identity_api_version")
    port = config.get("oidc_azure_container_port")

    print(
        "Getting token from {}@{}:{}, object_id={}, api_version={}, resource={}".format(
            user, hostname, str(port), object_id, api_version, resource
        )
    )

    with open(key_file, "r", encoding=CONST_ENCODING) as keyfile:
        contents = keyfile.read()
        if not contents.startswith("-----BEGIN OPENSSH PRIVATE KEY-----\n"):
            print("ERROR: Did not get a valid key file!!! File contents : [{}]".format(contents))
            sys.exit(1)

    process = subprocess.run(
        [
            "ssh",
            "-i",
            key_file,
            "-o",
            "StrictHostKeyChecking=no",
            "-p",
            port,
            "{}@{}".format(user, hostname),
            "curl",
            "-s",
            "{}:{}?resource={}\&object_id={}\&api-version={}".format(
                CONST_REMOTE_URL, CONST_REMOTE_PORT, resource, object_id, api_version
            ),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    token_output = {}

    try:
        token_output = json.loads(process.stdout.decode(), strict=False)
    except Exception as e:
        # Got some bad json, grab whatever text is in stdout/stderr and write it into a safe json format
        token_output["error"] = str(e)
        token_output["stdout"] = process.stdout.decode()
        token_output["stderr"] = process.stderr.decode()

    with open(args.output_file, "w+", encoding=CONST_ENCODING) as out_file:
        out_file.write(json.dumps(token_output))
        out_file.close()


if __name__ == "__main__":
    sys.exit(main())
