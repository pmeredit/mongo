"""
This script controls the enabling / disabling of network ingress to the Azure Container App
used in integration testing with OIDC. Notable points is that enabling ingress is specific
to the host that this script is run on, as it is visible to the outside world
"""

import argparse
import json
import logging
import sys
import time
from enum import Enum

import requests


class AzureBlobHeaders(str, Enum):
    CLIENT_REQ_ID = "x-ms-client-request-id"
    DATE = "x-ms-date"
    LEASE_ACTION = "x-ms-lease-action"
    LEASE_DURATION = "x-ms-lease-duration"
    LEASE_ID = "x-ms-lease-id"
    VERSION = "x-ms-version"


class AzureBlobActions(str, Enum):
    ACQUIRE = "acquire"
    RELEASE = "release"
    RENEW = "renew"


class ContainerAppFields(str, Enum):
    CONFIGURATION = "configuration"
    CONFIGURATION_SECRETS = "secrets"
    INGRESS = "ingress"
    INGRESS_EXTERNAL = "external"
    INGRESS_TARGET_PORT = "targetPort"
    INGRESS_ALLOW_INSECURE = "allowInsecure"
    INGRESS_TRANSPORT = "transport"
    INGRESS_EXPOSED_PORT = "exposedPort"
    INGRESS_IP_SECURITY_RESTRICTIONS = "ipSecurityRestrictions"
    INGRESS_IP_ADDRESS_RANGE = "ipAddressRange"
    INGRESS_FQDN = "fqdn"
    PROPERTIES = "properties"


class ConfigFields(str, Enum):
    BASE_URI = "base_uri"
    CONTAINER_APP_API_VERSION = "oidc_azure_api_version"
    CONTAINER_APP_NAME = "oidc_azure_container_app_name"
    GROUP_NAME = "oidc_azure_group_name"
    SUBSCRIPTION_ID = "oidc_azure_subscription_id"
    CLIENT_ID = "oidc_azure_client_id"
    CLIENT_SECRET_VAL = "oidc_azure_client_secret_val"
    TENANT_ID = "oidc_azure_tenant_id"
    CONTAINER_PORT = "oidc_azure_container_port"


class BlobServiceFields(str, Enum):
    STORAGE_ACCOUNT = "d63id7ot5zs97syawjl64pnm"
    CONTAINER_NAME = "oidc-evergreen-test"
    FILE_NAME = "file.txt"


def valid_json_response_or_throw(response, expected_code=requests.codes.OK):
    """
    Takes a requests response and converts it into a json formatted body if the
    response status code matches expected_code, otherwise throws an exception
    """

    if response.status_code != expected_code:
        logging.warning(
            "Error calling %s with unexpected status code %s : %s",
            response.url,
            response.status_code,
            str(response.content),
        )
        raise Exception

    content = response.json() if response.content else {}

    return content


def get_my_ip():
    """
    Returns the external-visible IP of the current host this script is running on via a lookup on
    checkip.amazonaws.com
    """

    check_ip_url = "https://checkip.amazonaws.com"

    r = requests.get(check_ip_url)

    if r.status_code != requests.codes.OK:
        raise Exception(
            "Could not get external IP for ingress, returned error code " + r.status_code
        )

    external_ip = r.text.strip()

    logging.debug("Got externally-visible IP from %s : %s", check_ip_url, external_ip)

    return external_ip


def validate_container_ingress_enabled(container_app_expected, container_app_response):
    """
    Validates that ingress has been enabled for container_app_response, using container_app_expected
    to determine expected values
    """

    allowed_ips = container_app_response.ip_security_restrictions()
    expected_allowed_ips = container_app_expected.ip_security_restrictions()

    # We expect that allowed_ips always returns exactly 1 when validating and matches the
    # IP of the VM running this test - the IP allowlist rules are not additive in the event
    # that e.g. somehow a previous test failed to teardown and left the allowlist configured
    # with its IP
    if (
        not allowed_ips
        or len(allowed_ips) != 1
        or allowed_ips[0][ContainerAppFields.INGRESS_IP_ADDRESS_RANGE.value]
        != expected_allowed_ips[0][ContainerAppFields.INGRESS_IP_ADDRESS_RANGE.value]
    ):
        logging.debug(
            "Allowed IPs requested have not been updated yet. Sent %s but received %s after patch: ",
            str(container_app_expected.ip_security_restrictions()),
            str(allowed_ips),
        )

        return False

    if not container_app_response.exposed_port() or int(
        container_app_response.exposed_port()
    ) != int(container_app_expected.exposed_port()):
        logging.debug(
            "Remote ingress port was not updated yet. Expected %s but got %s",
            container_app_expected.exposed_port(),
            container_app_response.exposed_port(),
        )

        return False

    logging.debug(
        "Ingress enabled on app container, listening at %s:%s",
        container_app_response.hostname(),
        container_app_response.exposed_port(),
    )

    return True


def validate_container_ingress_disabled(container_app_expected, container_app_response):
    """
    Validates that ingress has been disabled for container_app_response
    to determine expected values
    """

    if container_app_response.ingress():
        logging.warning(
            "Ingress rules are still defined for the container app after disabling. App still open on %s",
            str(container_app_response.ingress()),
        )
        return False

    return True


class Token:
    """
    A representation of an HTTP access token, consisting of the token type and token body
    """

    def __init__(self, json):
        self.token_type = json["token_type"]
        self.body = json["access_token"]

    def to_http_header(self):
        return {"Authorization": "{} {}".format(self.token_type, self.body)}


class TokenService:
    """
    TokenService is responsible for handling obtaining and operating on access tokens
    """

    def __init__(self, config, scope):
        self.config = config
        self.stored_token_json = None
        self.scope = scope

    def get_auth_token(self):
        if self.stored_token_json:
            return Token(self.stored_token_json)

        payload = {
            "client_id": self.config[ConfigFields.CLIENT_ID.value],
            "scope": self.scope,
            "client_secret": self.config[ConfigFields.CLIENT_SECRET_VAL.value],
            "grant_type": "client_credentials",
        }

        r = requests.post(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(
                self.config[ConfigFields.TENANT_ID]
            ),
            data=payload,
        )

        r_json = valid_json_response_or_throw(r)
        self.stored_token_json = r_json

        return Token(r_json)


class ContainerApp:
    """
    A deserialized representation of a (subset of) JSON Azure Container App status,
    reflecting the object structure of the HTTP JSON body
    obtained via GET / PATCH, along with a few utility methods
    """

    def __init__(self, json, resource_uri):
        self.json = json
        self.resource_uri = resource_uri

    def _configuration(self):
        return self.json[ContainerAppFields.PROPERTIES.value][
            ContainerAppFields.CONFIGURATION.value
        ]

    def enable_ingress(self, for_ip, for_port):
        self._configuration()[ContainerAppFields.INGRESS.value] = {
            ContainerAppFields.INGRESS_EXTERNAL.value: True,
            ContainerAppFields.INGRESS_TARGET_PORT.value: 22,
            ContainerAppFields.INGRESS_ALLOW_INSECURE.value: False,
            ContainerAppFields.INGRESS_TRANSPORT.value: "Tcp",
            ContainerAppFields.INGRESS_EXPOSED_PORT.value: for_port,
            ContainerAppFields.INGRESS_IP_SECURITY_RESTRICTIONS.value: [
                {
                    ContainerAppFields.INGRESS_IP_ADDRESS_RANGE.value: "{}/32".format(for_ip),
                    "action": "Allow",
                }
            ],
        }

        self._configuration()[ContainerAppFields.CONFIGURATION_SECRETS.value] = None

    def disable_ingress(self):
        self._configuration()[ContainerAppFields.INGRESS.value] = None
        self._configuration()[ContainerAppFields.CONFIGURATION_SECRETS.value] = None

    def exposed_port(self):
        ingress = self.ingress()
        return ingress.get(ContainerAppFields.INGRESS_EXPOSED_PORT.value) if ingress else None

    def ingress(self):
        return self._configuration().get(ContainerAppFields.INGRESS.value)

    def ip_security_restrictions(self):
        ingress = self.ingress()
        return (
            ingress.get(ContainerAppFields.INGRESS_IP_SECURITY_RESTRICTIONS.value, [])
            if ingress
            else None
        )

    def hostname(self):
        ingress = self.ingress()
        return ingress.get(ContainerAppFields.INGRESS_FQDN.value) if ingress else None


class ContainerAppService:
    """
    ContainerAppService encapsulates actions around reading and writing to the Azure Container App API
    """

    def __init__(self, token_service, resource_formatter):
        self.token_service = token_service
        self.resource_formatter = resource_formatter

    def get_container_app(self, resource):
        """
        Retrieves the remote Container App for the given resource, deserializing the JSON
        into a ContainerApp object.
        """

        headers = self.token_service.get_auth_token().to_http_header()
        container_r = requests.get(resource, headers=headers)

        container_json = valid_json_response_or_throw(container_r)

        logging.debug(str(container_json))
        return ContainerApp(container_json, resource)

    def update_container_app(self, container_app, validator_fn):
        """Takes a ContainerApp object and converts this into the necessary HTTP PATCH required to
        update the remote Azure container app.
        """

        patch_headers = self.token_service.get_auth_token().to_http_header()
        patch_headers["Content-Type"] = "application/json"

        patch_response = None
        retries = 0

        while (not patch_response) and retries < 3:
            retries += 1
            patch_response = requests.patch(
                container_app.resource_uri,
                data=json.dumps(container_app.json),
                headers=patch_headers,
            )

            try:
                valid_json_response_or_throw(patch_response, requests.codes.ACCEPTED)
            except Exception:
                patch_response = None
                time.sleep(3)

        if not patch_response:
            raise Exception("Could not update the container app for the test!")

        time.sleep(5)  # give it 5 seconds to apply on the Azure side

        container_app_r = self.get_container_app(container_app.resource_uri)

        retries = 0
        while True:
            if validator_fn(container_app, container_app_r):
                break

            retries += 1
            if retries >= 12:
                raise Exception(
                    "Did not get updated container app state within {} retries".format(str(retries))
                )

            logging.warning("Waiting 5 seconds for request to update... retry %s/12", str(retries))

            time.sleep(5)

            container_app_r = self.get_container_app(container_app.resource_uri)

        return container_app_r


class ResourceFormatter:
    """
    ResourceFormatter is a utility class containing resource (e.g. HTTP URI) builder functions
    """

    def __init__(self, config):
        self.config = config

    def format_container_app_resource_uri(self):
        return "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}?api-version={}".format(
            self.config[ConfigFields.BASE_URI.value],
            self.config[ConfigFields.SUBSCRIPTION_ID.value],
            self.config[ConfigFields.GROUP_NAME.value],
            self.config[ConfigFields.CONTAINER_APP_NAME.value],
            self.config[ConfigFields.CONTAINER_APP_API_VERSION.value],
        )


class BlobLeaseLock:
    def __init__(self, config, blob_token_service, lease_id=None):
        self.lease_id = lease_id
        self.token_service = blob_token_service
        self.config = config
        self.lease_uri = "https://{}.blob.core.windows.net/{}/{}?comp=lease".format(
            BlobServiceFields.STORAGE_ACCOUNT.value,
            BlobServiceFields.CONTAINER_NAME.value,
            BlobServiceFields.FILE_NAME.value,
        )

    def _append_standard_headers(self, header_dict):
        header_dict[AzureBlobHeaders.DATE.value] = time.strftime(
            "%a, %d %b %Y %H:%M:%S GMT", time.gmtime()
        )
        header_dict[AzureBlobHeaders.CLIENT_REQ_ID.value] = "mongodb_oidc_azure_external_auth"
        header_dict[AzureBlobHeaders.VERSION.value] = "2019-12-12"
        header_dict[AzureBlobHeaders.LEASE_DURATION.value] = "60"

    def acquire(self):
        headers = self.token_service.get_auth_token().to_http_header()

        self._append_standard_headers(headers)
        headers[AzureBlobHeaders.LEASE_ACTION.value] = AzureBlobActions.ACQUIRE.value

        response = requests.put(self.lease_uri, headers=headers)

        # If we get a response code back that is not 201 (created) then likely another test has already locked,
        # wait and retry
        while response.status_code == requests.codes.CONFLICT:
            time.sleep(10)
            self._append_standard_headers(headers)
            response = requests.put(self.lease_uri, headers=headers)

        if response.status_code != requests.codes.CREATED:
            logging.error(
                "Error calling %s with unexpected status code %s : %s",
                AzureBlobActions.ACQUIRE.value,
                response.status_code,
                str(response.content),
            )

            raise Exception("Could not acquire lease to run test")

        self.lease_id = response.headers[AzureBlobHeaders.LEASE_ID.value]

        return self.lease_id

    def renew(self):
        if not self.lease_id:
            raise Exception("Attempted to renew blob lease without setting lease_id")

        headers = self.token_service.get_auth_token().to_http_header()

        self._append_standard_headers(headers)
        headers[AzureBlobHeaders.LEASE_ACTION.value] = AzureBlobActions.RENEW.value
        headers[AzureBlobHeaders.LEASE_ID.value] = self.lease_id

        response = requests.put(self.lease_uri, headers=headers)

        if response.status_code != requests.codes.OK:
            logging.error(
                "Error calling %s with unexpected status code %s : %s",
                AzureBlobActions.RENEW.value,
                response.status_code,
                str(response.content),
            )

            raise Exception("Could not renew lease while running test")

    def release(self):
        if not self.lease_id:
            raise Exception("Attempted to release blob lease without setting lease_id")

        headers = self.token_service.get_auth_token().to_http_header()

        self._append_standard_headers(headers)
        headers[AzureBlobHeaders.LEASE_ACTION.value] = AzureBlobActions.RELEASE.value
        headers[AzureBlobHeaders.LEASE_ID.value] = self.lease_id

        response = requests.put(self.lease_uri, headers=headers)

        # If we couldn't release, not the end of the world, the lease will expire
        # after 60 seconds - maybe it already has
        if response.status_code != requests.codes.OK:
            logging.warning(
                "Error calling %s with unexpected status code %s : %s",
                AzureBlobActions.RELEASE.value,
                response.status_code,
                str(response.content),
            )
            # someone else has acquire the lease - could be we timed out and somebody
            # else got it
            if response.status_code != requests.codes.CONFLICT:
                raise Exception("unexpected error while releasing the lease")


def parse_config(args):
    config = {}
    with open(args.config_file) as config_file:
        config = json.load(config_file)
        config_file.close()

    config[ConfigFields.BASE_URI.value] = "https://management.azure.com"

    return config


def enable_ingress(
    container_app_service, token_service, resource_formatter, container_app, exposed_port
):
    container_app.enable_ingress(get_my_ip(), exposed_port)

    container_app = container_app_service.update_container_app(
        container_app, validate_container_ingress_enabled
    )

    return container_app


def disable_ingress(container_app_service, container_app):
    container_app.disable_ingress()
    container_app_service.update_container_app(container_app, validate_container_ingress_disabled)


def main():
    arg_parser = argparse.ArgumentParser(prog="Azure Container Manager Script")

    arg_parser.add_argument("mode", choices=["enable", "disable"])
    arg_parser.add_argument("--config_file", type=str, required=True)
    arg_parser.add_argument("--debug", action="store_true")
    arg_parser.add_argument("--lock_file", type=str, required=True)

    args = arg_parser.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.WARN))

    config = parse_config(args)
    resource_formatter = ResourceFormatter(config)

    token_service_containerapp = TokenService(
        config, "https://management.core.windows.net/.default"
    )
    container_app_service = ContainerAppService(token_service_containerapp, resource_formatter)

    container_app = container_app_service.get_container_app(
        resource_formatter.format_container_app_resource_uri()
    )

    blob_lease_token_service = TokenService(config, "https://storage.azure.com/.default")

    if args.mode == "enable":
        with open(args.lock_file, "w+") as lock_file:
            blob_lease_lock = BlobLeaseLock(config, blob_lease_token_service)
            lease_id = blob_lease_lock.acquire()
            lock_file.write(lease_id)
            lock_file.flush()

        container_app = enable_ingress(
            container_app_service,
            token_service_containerapp,
            resource_formatter,
            container_app,
            config[ConfigFields.CONTAINER_PORT.value],
        )

        blob_lease_lock.renew()
    else:
        lease_id = ""

        with open(args.lock_file, "r") as lock_file:
            lease_id = lock_file.read()

        blob_lease_lock = BlobLeaseLock(config, blob_lease_token_service, lease_id=lease_id)

        if not lease_id:
            blob_lease_lock.acquire()
        else:
            blob_lease_lock.renew()

        disable_ingress(container_app_service, container_app)

        blob_lease_lock.release()

    # Output the targeted container app hostname to stdout
    # so this does not have to be hardcoded in subsequent tasks / variables
    print(container_app.hostname() if container_app.hostname() else "")

    return 0


if __name__ == "__main__":
    sys.exit(main())
