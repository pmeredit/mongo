# MongoDB OIDC External Auth for Azure README

## Overview

These tests automate the retrieval of tokens from both interactive (human-user) and non-interactive (machine / managed identity) IdP configurations in Azure

### Interactive

Interactive tests are defined in oidc_e2e_azure.js . These tests exercise the ability of a human user to log in to Azure AD via public outlook.com accounts that are configured with basic access permissions in Azure AD. This is not an exhaustive set of test cases and is focused primarily on basic token acquisition and refresh flows via the defined test accounts.

### Non-Interactive

Non-interactive tests are defined in oidc_e2e_azure_machine.js . The tests defined in this file are of much smaller scope than the interactive tests, as the focus is on token format compatibility rather than token acquisition.

The tests and the associated evergreen setup and teardown scripts in Community (evergreen/external_auth_azure_teardown.sh and evergreen/external_auth_azure_setup.sh) assume and require that an Azure Container App instance is pre-configured and deployed in Azure - the Dockerfile and associated images are not managed by the test setup or evergreen, rather they are managed manually per further steps below.

After configuring an Azure managed-identity enabled token source via Azure Container Apps, we obtain a local token with appropriate scope and provide this to the test server. If the token is accepted and the user is logged in to the server, we consider this to be a successful test. Refresh flows and other token-specific workflows are not included as part of the test scope.

The setup for the non-interactive test is extensive and infrastructure-heavy, and is described in futher detal below.

## Non-Interactive Test Management

Management and maintenance of the non-interactive test harness in oidc_e2e_azure_machine.js requires the following components to work in coordination:

- An instance of a managed identity service running on [Azure Container App Service](https://azure.microsoft.com/en-us/products/container-apps)
- A Docker image (docker/azure_ubuntu_2204_oidc) of a simple token-acquiring service deployed to Azure Container App service via [Azure Container Registry](https://azure.microsoft.com/en-us/products/container-registry)
- A python script (toggle_ingress.py) responsible for enabling (and then disabling) ingress to the aforementioned container instance from evergreen by getting the evergreen egress IP address of the host running the test
- A python script (get_token.py) that is responsible for contacting the managed identity service and extracting the token for testing

Container App Deployed --> ingress enabled --> get_token.py to local file --> jstest accesses local file --> submit to test mongod --> ingress disabled (regardless of test outcome)

### Docker Image + Azure Container Registry

The construction of the docker image is straightforward, requiring the public key file contents to be provided as a build-time argument to the image, and tagged with the appropriate tag - in this case, the address of the ACR repo:

`docker build azure_ubuntu_2204_oidc --build-arg PUBKEY="<ssh-keygen file contents>" -t serversecurity.azurecr.io/oidc/ubuntu_lts_oidc_base:latest`

This container can be run locally, but make sure to map the exposed port 22 to something that does not conflict with your local SSH service.

To push this image remotely, you will need to authenticate with ACR via docker login with appropriate credentials:

`docker login serversecurity.azurecr.io`

### Azure Container Apps + Managed Identity Configuration

When a new image is pushed to ACR, it is necessary to go and update the Container App revision for the service, in order to update the running image (it will not refresh automatically even with a new image pushed to the latest tag)

1. Container Apps
2. Click on the Container App (e.g. oidc-machine-flow-test)
3. Click on "Create new revision"
4. Click on the selected "Container Image" and update the "Name" field to a unique name (e.g. oidc-machine-flow-test03), and if necessary, the Identity field to the appropriate managed identity
5. Click on "Create"
6. A new revision should be created in the "Revisions" tab. Once it is running, the test can be re-run with a container from the updated docker image
