import sys
import requests
import time
import json
import argparse
import os
import logging


class Token:
    def __init__(self, token_type, body):
        self.token_type = token_type
        self.body = body

    def to_http_header(self):
        return {'Authorization': '{} {}'.format(self.token_type, self.body)}


class ContainerApp:
    """ A deserialized representation of a (subset of) JSON Azure Container App status, reflecting the object structure of the HTTP JSON body
    obtained via GET / PATCH, along with a few getters / setters.
    """

    def __init__(self, json):
        self.json = json

    def _getConfiguration(self):
        return self.json['properties']['configuration']

    def exposed_port(self):
        ingress = self.ingress()

        return ingress.get('exposedPort') if ingress else None

    def set_exposed_port(self, port):
        self.ingress()['exposedPort'] = port

    def set_secrets(self, secrets):
        if secrets is None:
            del self._getConfiguration()['secrets']
        else:
            self._getConfiguration()['secrets'] = secrets

    def secrets(self):
        return self._getConfiguration().get('secrets')

    def disableIngress(self):
        self._getConfiguration()['ingress'] = None

    def enableIngress(self):
        self._getConfiguration()['ingress'] = {
            'external': True,
            'targetPort': 22,
            'allowInsecure': False,
            'transport': 'Tcp'
        }

    def ingress(self):
        return self._getConfiguration().get('ingress')

    def ipSecurityRestriction(self):
        return self.ingress().get('ipSecurityRestrictions', [])

    def setIpSecurityRestriction(self, ip):
        security_range = None
        if ip:
            security_range = {
                'ipAddressRange': ip,
                'action': 'Allow'
            }

        self.ingress()['ipSecurityRestrictions'] = [security_range]

    def hostname(self):
        return self.ingress().get('fqdn')


def get_my_ip():
    r = requests.get('https://checkip.amazonaws.com')

    if r.status_code != 200:
        raise Exception(
            'Could not get external IP for ingress, returned error code ' + r.status_code)

    external_ip = r.text.strip()

    logging.debug(
        'Got externally-visible IP from https://checkip.amazonaws.com : %s', external_ip)

    return external_ip


def valid_json_response_or_throw(response, expected_code=200):

    content = response.json() if response.content else {}

    if response.status_code != expected_code:
        logging.warn('Error caling %s with unexpected status code %s : %s',
                     response.url, response.status_code, str(content))
        raise Exception

    return content


def get_auth_token(client_id, client_secret, t_id):
    payload = {'client_id': client_id,
               'scope': 'https://management.core.windows.net/.default',
               'client_secret': client_secret,
               'grant_type': 'client_credentials'}

    r = requests.post(
        'https://login.microsoftonline.com/{}/oauth2/v2.0/token'.format(t_id), data=payload)
    r_json = valid_json_response_or_throw(r)

    return Token(r_json['token_type'], r_json['access_token'])


def build_resource_url(subscription_id, resource_group, containerapp_name, api_version):
    return '{}/subscriptions/{}/resourceGroups/{}/providers/Microsoft.App/containerApps/{}?api-version={}'.format(
        'https://management.azure.com',
        subscription_id,
        resource_group,
        containerapp_name,
        api_version)


def get_container_remote(resource, token):
    """ Retrieves the remote Container App for the given resource, deserializing the JSON into a ContainerApp object.
        """

    headers = token.to_http_header()
    container_r = requests.get(resource, headers=headers)

    container_json = valid_json_response_or_throw(container_r)

    logging.debug(str(container_json))
    return ContainerApp(container_json)


def update_container_remote(resource, token, container_app):
    """ Takes a ContainerApp object and an authorization token, and converts this into the necessary HTTP PATCH required to
    update the remote Azure container app.
    """

    patch_headers = token.to_http_header()
    patch_headers['Content-Type'] = 'application/json'

    patch_response = requests.patch(resource,
                                    data=json.dumps(container_app.json),
                                    headers=patch_headers)

    valid_json_response_or_throw(patch_response, 202)


def enable_ingress(target_resource, port, token):
    """ Given a target resource:
     1. Retrieves a JSON body representing it in Azure
     2. Converts it into a ContainerApp object
     3. Performs the configuration necessary to enable ingress for this resource: enables TCP ingress on the specified port
        and configures a single IP that is allowed to perform this ingress to the local port that of the host this script is running on
     4. PATCHes a request to Azure Container App service with the updated Container App configuration
     5. Validates that ingress has been enabled as expected
    """

    logging.debug('Enabling ingress on app container...')

    my_ip_cidr = '{}/32'.format(get_my_ip())
    container_app = get_container_remote(target_resource, token)
    container_app.enableIngress()

    container_app.set_exposed_port(port)
    container_app.set_secrets(None)
    container_app.setIpSecurityRestriction(my_ip_cidr)

    update_container_remote(target_resource, token, container_app)

    # Note: in practice, this sleep is unnecessary (the update on the previous line will become visible in the time it
    # takes to receive the HTTP response and send the follow-up GET request below), but since we're not overly
    # worried about runtime for this test suite, we'll wait the 5 seconds just to be extra sure.
    logging.debug(
        'Sleeping for 5 seconds to allow the background update job to complete...')
    time.sleep(5)

    container_app = get_container_remote(target_resource, token)
    allowed_ips = container_app.ipSecurityRestriction()

    if len(allowed_ips) != 1 or allowed_ips[0]['ipAddressRange'] != my_ip_cidr:
        logging.warning(
            'Allowed IPs requested have not been updated! Received after patch: %s', str(allowed_ips))
        sys.exit(1)

    if int(container_app.exposed_port()) != int(port):
        logging.warning('Remote ingress port was not updated! Expected %s but got %s',
                        container_app.exposed_port(), port)
        sys.exit(1)

    logging.debug('Ingress enabled on app container, listening at %s:%s',
                  container_app.hostname(), container_app.exposed_port())
    return container_app


def disable_ingress(target_resource, token):
    logging.debug('Disabling ingress on app container...')

    container_app = get_container_remote(target_resource, token)
    container_app.disableIngress()
    container_app.set_secrets(None)

    update_container_remote(target_resource, token, container_app)

    # Note: in practice, this sleep is unnecessary (the update on the previous line will become visible in the time it
    # takes to receive the HTTP response and send the follow-up GET request below), but since we're not overly
    # worried about runtime for this test suite, we'll wait the 5 seconds just to be extra sure.
    time.sleep(5)

    container_app = get_container_remote(target_resource, token)

    if container_app.ingress() is not None:
        logging.warning('Ingress rules are still defined for the container app after disabling! Please check the configuration! App still open at %s', str(
            container_app.ingress()))
        sys.exit(1)

    logging.debug('Ingress on app container disabled.')
    return container_app


def main():

    arg_parser = argparse.ArgumentParser(prog='Azure Container Manager Script')

    arg_parser.add_argument('mode', choices=['enable', 'disable'])
    arg_parser.add_argument('--config_file', type=str, required=True)
    arg_parser.add_argument('--debug', action='store_true')

    args = arg_parser.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.WARN))

    config = {}
    with open(args.config_file) as config_file:
        config = json.load(config_file)
        config_file.close()

    resource_url = build_resource_url(config['oidc_azure_subscription_id'],
                                      config['oidc_azure_group_name'],
                                      config['oidc_azure_container_app_name'],
                                      config['oidc_azure_api_version'])

    token = get_auth_token(config['oidc_azure_client_id'],
                           config['oidc_azure_client_secret_val'], config['oidc_azure_tenant_id'])

    if args.mode == 'enable':
        container_app = enable_ingress(
            resource_url, config['oidc_azure_container_port'], token)

        # If we are enabling ingress, output the now-acessible container app hostname to stdout
        # so this does not have to be hardcoded in subsequent tasks / variables
        print(container_app.hostname())
    else:
        disable_ingress(resource_url, token)

    return 0


if __name__ == '__main__':
    sys.exit(main())
