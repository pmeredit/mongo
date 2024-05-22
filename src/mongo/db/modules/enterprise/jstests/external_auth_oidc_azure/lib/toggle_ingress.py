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

import requests


def valid_json_response_or_throw(response, expected_code=200):
    """
    Takes a requests response and converts it into a json formatted body if the
    response status code matches expected_code, otherwise throws an exception
    """

    content = response.json() if response.content else {}

    if response.status_code != expected_code:
        logging.warn(
            "Error calling %s with unexpected status code %s : %s",
            response.url,
            response.status_code,
            str(content),
        )
        raise Exception

    return content


def get_my_ip():
    """
    Returns the external-visible IP of the current host this script is running on via a lookup on
    checkip.amazonaws.com
    """

    r = requests.get("https://checkip.amazonaws.com")

    if r.status_code != 200:
        raise Exception(
            "Could not get external IP for ingress, returned error code " + r.status_code
        )

    external_ip = r.text.strip()

    logging.debug("Got externally-visible IP from https://checkip.amazonaws.com : %s", external_ip)

    return external_ip


def validate_container_ingress_enabled(container_app_expected, container_app_response):
    """
    Validates that ingress has been enabled for container_app_response, using container_app_expected
    to determine expected values
    """

    allowed_ips = container_app_response.ip_security_restrictions()
    expected_allowed_ips = container_app_expected.ip_security_restrictions()
    if (
        len(allowed_ips) != 1
        or allowed_ips[0]["ipAddressRange"] != expected_allowed_ips[0]["ipAddressRange"]
    ):
        logging.warning(
            "Allowed IPs requested have not been updated! Sent %s but received %s after patch: ",
            str(container_app_expected.ip_security_restrictions()),
            str(allowed_ips),
        )
        sys.exit(1)

    if int(container_app_response.exposed_port()) != int(container_app_expected.exposed_port()):
        logging.warning(
            "Remote ingress port was not updated! Expected %s but got %s",
            container_app_response.exposed_port(),
            container_app_expected.exposed_port(),
        )
        sys.exit(1)

    logging.debug(
        "Ingress enabled on app container, listening at %s:%s",
        container_app_response.hostname(),
        container_app_response.exposed_port(),
    )


def validate_container_ingress_disabled(container_app_expected, container_app_response):
    """
    Validates that ingress has been disabled for container_app_response, using container_app_expected if necessary
    to determine expected values
    """

    if container_app_response.ingress() is not None:
        logging.warning(
            "Ingress rules are still defined for the container app after disabling! Please check the configuration! App still open for %s",
            str(container_app_expected.ingress()),
        )
        sys.exit(1)


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

    def __init__(self, config):
        self.config = config
        self.stored_token_json = None

    def get_auth_token(self):
        if self.stored_token_json:
            return Token(self.stored_token_json)

        payload = {
            "client_id": self.config["oidc_azure_client_id"],
            "scope": "https://management.core.windows.net/.default",
            "client_secret": self.config["oidc_azure_client_secret_val"],
            "grant_type": "client_credentials",
        }

        r = requests.post(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(
                self.config["oidc_azure_tenant_id"]
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
        return self.json["properties"]["configuration"]

    def enable_ingress(self, for_ip, for_port):
        self._configuration()["ingress"] = {
            "external": True,
            "targetPort": 22,
            "allowInsecure": False,
            "transport": "Tcp",
            "exposedPort": for_port,
            "ipSecurityRestrictions": [
                {"ipAddressRange": "{}/32".format(for_ip), "action": "Allow"}
            ],
        }

        self._configuration()["secrets"] = None

    def disable_ingress(self):
        self._configuration()["ingress"] = None
        self._configuration()["secrets"] = None

    def exposed_port(self):
        ingress = self.ingress()
        return ingress.get("exposedPort") if ingress else None

    def ingress(self):
        return self._configuration().get("ingress")

    def ip_security_restrictions(self):
        ingress = self.ingress()
        return ingress.get("ipSecurityRestrictions", []) if ingress else None

    def hostname(self):
        ingress = self.ingress()
        return ingress.get("fqdn") if ingress else None


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

        patch_response = requests.patch(
            container_app.resource_uri, data=json.dumps(container_app.json), headers=patch_headers
        )

        valid_json_response_or_throw(patch_response, 202)

        # Note: in practice, this sleep is unnecessary (the update on the previous line will become visible in the time it
        # takes to receive the HTTP response and send the follow-up GET request below), but since we're not overly
        # worried about runtime for this test suite, we'll wait the 5 seconds just to be extra sure.
        time.sleep(5)

        container_app_r = self.get_container_app(container_app.resource_uri)

        if validator_fn:
            validator_fn(container_app, container_app_r)

        return container_app_r


class ContainerAppRevisionService:
    """ "
    ContainerAppRevisionService encapsulates actions around reading from and acting against
    the Azure Container App APIs specifically around the concept of a "revision"
    """

    def __init__(self, token_service, resource_formatter):
        self.token_service = token_service
        self.resource_formatter = resource_formatter

    def get_active_revision_name(self, resource):
        headers = self.token_service.get_auth_token().to_http_header()

        container_revision_r = requests.get(resource, headers=headers)

        revision_json = valid_json_response_or_throw(container_revision_r)

        logging.debug(str(revision_json))

        active_revisions = list(
            filter(lambda v: v["properties"]["active"] is True, revision_json["value"])
        )
        return active_revisions[0]["name"]

    def restart_revision(self, resource):
        headers = self.token_service.get_auth_token().to_http_header()

        revision_restart_r = requests.post(resource, headers=headers)
        valid_json_response_or_throw(revision_restart_r)


class ResourceFormatter:
    """
    ResourceFormatter is a utility class containing resource (e.g. HTTP URI) builder functions
    """

    def __init__(self, config):
        self.config = config

    def format_container_app_resource_uri(self):
        return "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}?api-version={}".format(
            self.config["base_uri"],
            self.config["oidc_azure_subscription_id"],
            self.config["oidc_azure_group_name"],
            self.config["oidc_azure_container_app_name"],
            self.config["oidc_azure_api_version"],
        )

    def format_revision_restart_resource_uri(self, revision_name):
        return "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}/revisions/{}/restart?api-version={}".format(
            self.config["base_uri"],
            self.config["oidc_azure_subscription_id"],
            self.config["oidc_azure_group_name"],
            self.config["oidc_azure_container_app_name"],
            revision_name,
            self.config["oidc_azure_api_version"],
        )

    def format_revision_list_resource_uri(self):
        return "{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}/revisions?api-version={}".format(
            self.config["base_uri"],
            self.config["oidc_azure_subscription_id"],
            self.config["oidc_azure_group_name"],
            self.config["oidc_azure_container_app_name"],
            self.config["oidc_azure_api_version"],
        )


def parse_config(args):
    config = {}
    with open(args.config_file) as config_file:
        config = json.load(config_file)
        config_file.close()

    config["base_uri"] = "https://management.azure.com"

    return config


def enable_ingress(
    container_app_service, token_service, resource_formatter, container_app, exposed_port
):
    container_app.enable_ingress(get_my_ip(), exposed_port)
    container_app = container_app_service.update_container_app(
        container_app, validate_container_ingress_enabled
    )

    con_app_revision_service = ContainerAppRevisionService(token_service, resource_formatter)

    active_revision = con_app_revision_service.get_active_revision_name(
        resource_formatter.format_revision_list_resource_uri()
    )
    restart_resource_uri = resource_formatter.format_revision_restart_resource_uri(active_revision)
    con_app_revision_service.restart_revision(restart_resource_uri)

    return container_app


def disable_ingress(container_app_service, container_app):
    container_app.disable_ingress()
    container_app_service.update_container_app(container_app, validate_container_ingress_disabled)


def main():
    arg_parser = argparse.ArgumentParser(prog="Azure Container Manager Script")

    arg_parser.add_argument("mode", choices=["enable", "disable"])
    arg_parser.add_argument("--config_file", type=str, required=True)
    arg_parser.add_argument("--debug", action="store_true")

    args = arg_parser.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.WARN))

    config = parse_config(args)

    token_service = TokenService(config)
    resource_formatter = ResourceFormatter(config)
    container_app_service = ContainerAppService(token_service, resource_formatter)

    container_app_resource = resource_formatter.format_container_app_resource_uri()
    container_app = container_app_service.get_container_app(container_app_resource)

    if args.mode == "enable":
        container_app = enable_ingress(
            container_app_service,
            token_service,
            resource_formatter,
            container_app,
            config["oidc_azure_container_port"],
        )
    else:
        disable_ingress(container_app_service, container_app)

    # Output the targeted container app hostname to stdout
    # so this does not have to be hardcoded in subsequent tasks / variables
    print(container_app.hostname() if container_app.hostname() else "")

    return 0


if __name__ == "__main__":
    sys.exit(main())
