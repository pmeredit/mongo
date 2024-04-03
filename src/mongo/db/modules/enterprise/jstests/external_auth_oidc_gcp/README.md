# MongoDB OIDC External Auth for GCP README

## Overview

The `external_auth_oidc_gcp` test suite provides end-to-end test coverage of the MONGODB-OIDC authentication mechanism with Google Cloud identity providers. The suite currently is composed of a sole test, oidc_e2e_gcp_machine.js, which ensures that GCP service accounts can authenticate to MongoDB using an ID token. It runs on the Enterprise Ubuntu 20.04 variant on the mongodb-mongo-master-nightly project.

GCP resources (VMs, containers, Kubernetes pods, etc.) can be associated with service accounts so that they can attest their identities to other services both inside and outside of GCP. Some MongoDB clients are applications that will want to authenticate to the database as themselves rather than a user, so on GCP it would make sense for them to authenticate as a service account. This end-to-end test simulates a Google Compute Engine (GCE) virtual machine (VM) authenticating to the database as a service account that represents itself.

## Overview of Evergreen Execution

Execution of the `external_auth_oidc_gcp` suite on Evergreen involves three steps:

1. After performing general task setup, Evergreen runs the `external_auth_gcp_setup.sh` script. The script does the following:
   - a. Creates a new configuration file at `$HOME/gce_vm_config.json`. The file specifies the expected token audience, GCP project ID, GCP project zone, and the GCE VM instance template URL. The script sets these values based on Evergreen environment variables that were pre-loaded into the Evergreen projects that this suite is expected to run in.
   - b. Creates a SSH key file at `$HOME/gcp_ssh_key`. The key contents are derived from yet another preloaded Evergreen environment variable. Note that it may be easier to wrap the PEM formatted key in another layer of base64 encoding in order to preserve line breaks.
   - c. Creates a service account key file at the path specified by `$GOOGLE_APPLICATION_CREDENTIALS`. The key contents are derived from a preloaded Evergreen environment variable. Note that GCP service account keys are typically JSON, so it may also be worth base64 encoding the JSON and storing that representation in the environment variable.
   - d. Launches a Python script called `gce_vm_manager.py` that lives in the enterprise repository in "create" mode. The Python script creates a new GCP VM using the configuration file and the service account keyfile to authenticate to GCP. After creating the VM, the Python script writes the name and public IP address of the instance to `$HOME/gce_vm_info.json`.
2. Now, the test is ready to be executed. It is relatively straightforward:
   - a. First, the test configures the server for OIDC authentication with Google Identity as the IdP. The issuer URI is https://accounts.google.com, while the audience is determined by reading the config file created by the setup script.
   - b. After starting the MongoDB cluster, the test creates a user corresponding to the expected principal of the service account's ID token. This principal is specified in the ID token's sub claim and maps to the service account's unique identifier. Since the test is using internal authorization, the user is also assigned roles during user creation.
   - c. The test invokes a separate Python script, `get_id_token.py`. This script contains all the logic to retrieve the token from the Google Compute Engine VM that is now running after the setup script completed. The Python script uses the VM info file to determine the public IP address of the VM and the SSH key file to establish an SSH connection with the VM. It then runs a curl command within the VM to the GCP metadata server to request an ID token, supplying it with the audience written to the config file. It finally attempts to parse the output to determine whether or not it is a JWT before writing it to a configured output file.
   - d. The test reads the contents of the output file, converts it to BSON via a temp file, and supplies this as the payload to a saslStart command against the cluster.
   - e. After asserting that authentication succeeded, the test runs `connectionStatus` to determine the user's roles and compares it with the ones assigned to it via internal authorization, asserting equality.
   - f. The test shuts down the MongoDB cluster and the Python script before terminating.
3. Finally, Evergreen runs the `external_auth_gcp_teardown.sh` script as part of the post-command tasks. The script does the following:
   - a. If the task name doesn't match `external_auth_oidc_gcp`, it terminates immediately.
   - b. Otherwise, it runs the `gce_vm_manager.py` script, this time in "delete" mode. The script uses the config file and VM info file created during setup to determine the GCP project ID, zone, and VM name to delete and then proceeds to delete it.
   - c. After successful deletion, the teardown script deletes the `gcp_ssh_key` and service account key files.
   - d. Note that if this teardown script does not fire after a test for some reason, the all created VMs are configured to self-delete after 2 hours.
