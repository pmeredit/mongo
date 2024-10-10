"""
This script creates, deletes, and reaps Google Compute Engine virtual machines used in integration
testing with OIDC.

The external_auth_oidc_gcp_setup.sh script invokes this script in "create" mode, which causes it to
create a new VM for the forthcoming test to use. After the test has completed, the
external_auth_oidc_gcp_teardown.sh script invokes this script in "delete" mode, which causes it to
delete the VM whose details were written to the gce_vm_info_file.

All VMs created by this file should follow an instance template that ensures that they are auto-reaped
in 2 hours in the event that they are not properly deleted by the teardown script.

"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from collections import defaultdict

from google.cloud import compute_v1


# Reads config from the file and makes assertions that the expected fields are provided.
def read_config_from_file(config_file_name):
    # Expected to have the project ID, project zone, and test cluster audience.
    config = {}
    try:
        with open(config_file_name) as config_file:
            config = json.load(config_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"Config file could not be read: {str(e)}")
        raise e

    if "audience" not in config:
        logging.warning("Test cluster audience not specified in config")

    if "projectID" not in config:
        logging.warning("Project ID not specified as part of config")
        sys.exit(1)

    if "zone" not in config:
        logging.warning("Project zone not specified as part of config")
        sys.exit(1)

    if "instance_template_url" not in config:
        logging.warning("Instance template URL not specified as part of config")
        sys.exit(1)

    return config


def get_instances(instance_client, project_id):
    request = compute_v1.AggregatedListInstancesRequest()
    request.project = project_id
    request.max_results = 50

    agg_list = instance_client.aggregated_list(request=request)

    # The provided output is reformatted into a dictionary mapping zones with lists of instances.
    # Each instance's name and external IP address is printed.
    all_instances = defaultdict(list)
    for zone, response in agg_list:
        if response.instances:
            all_instances[zone].extend(response.instances)
            print(f"{zone}:")
            for instance in response.instances:
                print(
                    f" - {instance.name} ({instance.network_interfaces[0].access_configs[0].nat_i_p})"
                )

    return all_instances


def create_instance(instance_client, project_id, zone, instance_template, gce_vm_info_file_name):
    # Each VM name includes a UUID to minimize instance name collisions.
    instance_name = "gcp-oidc-testing-vm-" + str(uuid.uuid4())

    # Construct the request to create a new instance and wait for it to complete. The timeout is
    # is set to 5 minutes. The URL specified by 'instance-template' points to a predefined set of
    # configuration options that GCE will use to build the new instance.
    request = compute_v1.InsertInstanceRequest()
    request.zone = zone
    request.project = project_id
    request.source_instance_template = instance_template
    request.instance_resource.name = instance_name

    logging.info(f"Creating the {instance_name} instance in {zone}...")

    operation = instance_client.insert(request=request)
    operation.result(timeout=300)
    if operation.error_code:
        logging.warn(
            f"Error when creating the VM instance {instance_name}: [Code: {operation.error_code}] - {operation.error_message}"
        )
        sys.exit(1)

    logging.info(
        f"Instance {instance_name} created, writing host information to {gce_vm_info_file_name}"
    )
    new_instance = instance_client.get(project=project_id, zone=zone, instance=instance_name)
    gce_vm_info = {
        "name": new_instance.name,
        "external_ip": new_instance.network_interfaces[0].access_configs[0].nat_i_p,
    }
    with open(gce_vm_info_file_name, "w+", encoding="utf-8") as gce_vm_info_file:
        gce_vm_info_file.write(json.dumps(gce_vm_info))

    return new_instance


def delete_instance(instance_client, gce_vm_info_file_name, project_id, zone):
    # Read the GCE VM instance name from the output file - it should have been written to
    # during VM creation.
    gce_vm_info = {}
    try:
        with open(gce_vm_info_file_name) as gce_vm_info_file:
            gce_vm_info = json.load(gce_vm_info_file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"GCE VM info file could not be read: {str(e)}")
        raise e

    instance_name = gce_vm_info["name"]
    instance_external_ip = gce_vm_info["external_ip"]
    logging.info(f"Deleting the {instance_name}({instance_external_ip}) instance...")

    # Send the request to delete the instance, providing it with a 5 minute timeout window.
    operation = instance_client.delete(project=project_id, zone=zone, instance=gce_vm_info["name"])
    operation.result(timeout=300)
    if operation.error_code:
        logging.warn(
            f"Error when deleting the VM instance {gce_vm_info['name']}: [Code: {operation.error_code}] - {operation.error_message}"
        )
        sys.exit(1)

    logging.info(
        f"Instance {instance_name} deleted, now removing GCE VM info file ({gce_vm_info_file_name})"
    )
    os.remove(gce_vm_info_file_name)


def main():
    arg_parser = argparse.ArgumentParser(prog="GCP Compute Engine VM Manager")
    arg_parser.add_argument("mode", choices=["create", "delete"])
    arg_parser.add_argument("--config_file", type=str, required=True)
    arg_parser.add_argument("--service_account_key_file", type=str, required=True)
    arg_parser.add_argument("--output_file", type=str, required=True)
    arg_parser.add_argument("--debug", action="store_true")

    args = arg_parser.parse_args()

    # Set log level based on the startup args.
    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO))

    # Load in the config options from the supplied config file.
    config = read_config_from_file(args.config_file)

    # Set the $GOOGLE_APPLICATION_CREDENTIALS environment variable to the appropriate path.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.service_account_key_file

    # Create an instance client and perform the requested operation.
    instance_client = compute_v1.InstancesClient()
    if args.mode == "create":
        create_instance(
            instance_client,
            config["projectID"],
            config["zone"],
            config["instance_template_url"],
            args.output_file,
        )
        get_instances(instance_client, config["projectID"])
    else:
        delete_instance(instance_client, args.output_file, config["projectID"], config["zone"])
        get_instances(instance_client, config["projectID"])


if __name__ == "__main__":
    sys.exit(main())
