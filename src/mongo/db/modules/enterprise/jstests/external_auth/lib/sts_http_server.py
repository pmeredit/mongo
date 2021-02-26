#! /usr/bin/env python3
"""Mock AWS STS Endpoint."""

import argparse
import base64
import collections
import http.server
import json
import logging
import socketserver
import sys
import urllib.parse
import ssl

from botocore.auth import SigV4Auth, S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

import aws_common

fault_type = None

"""Fault which causes sts::getCallerIdentity to return 403"""
FAULT_403 = "fault_403"

# List of supported fault types
SUPPORTED_FAULT_TYPES = [
    FAULT_403,
]


def get_dict_subset(headers, subset):
    ret = {}
    for header in headers.keys():
        if header.lower() in subset.lower():
            ret[header] = headers[header]
    return ret

class AwsStsHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests from AWS STS Monitoring and test commands
    """
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        """Serve a POST request."""
        parts = urllib.parse.urlsplit(self.path)
        path = parts[2]

        if path == "/":
            self._do_post()
        else:
            msg = "Unknown URL".encode()
            self.send_response(http.HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "text/plain");
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def _send_reply(self, data, status=http.HTTPStatus.OK):
        print("Sending Response: " + data.decode())

        self.send_response(status)
        self.send_header("content-type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

        self.wfile.write(data)

    def _do_post(self):
        clen = int(self.headers.get('content-length'))

        raw_input = self.rfile.read(clen)

        print("RAW INPUT: " + str(raw_input))

        if not self.headers["Host"] == "localhost":
            data = "Unexpected host"
            self._send_reply(data.encode("utf-8"))

        if not self._validate_signature(self.headers, raw_input):
            data = "Bad Signature"
            self._send_reply(data.encode("utf-8"))
            return

        # X-Amz-Target: TrentService.Encrypt
        if raw_input.decode("utf-8")  == "Action=GetCallerIdentity&Version=2011-06-15":
            self._do_get_caller_identity(self.headers["Authorization"])
        else:
            print("UNKNOWN AWS OPERATION: |%s|" % (str(raw_input)))
            data = "Unknown AWS Operation"
            self._send_reply(data.encode("utf-8"))


    def _validate_signature(self, headers, raw_input):
        auth_header = headers["Authorization"]
        signed_headers_start = auth_header.find("SignedHeaders")
        signed_headers = auth_header[signed_headers_start:auth_header.find(",", signed_headers_start)]
        signed_headers_dict = get_dict_subset(headers, signed_headers)
        print("HEADERS: " + str(headers))
        print("DIC: " + str(signed_headers_dict))

        request = AWSRequest(method="POST", url="/", data=raw_input, headers=signed_headers_dict)
        # SigV4Auth assumes this header exists even though it is not required by the algorithm
        request.context['timestamp'] = headers['X-Amz-Date']

        is_temporary = False
        if aws_common.MOCK_AWS_ACCOUNT_ID in auth_header:
            account = aws_common.MOCK_AWS_ACCOUNT_ID
            secret = aws_common.MOCK_AWS_ACCOUNT_SECRET_KEY
        elif aws_common.MOCK_AWS_TEMP_ACCOUNT_ID in auth_header:
            account = aws_common.MOCK_AWS_TEMP_ACCOUNT_ID
            secret = aws_common.MOCK_AWS_TEMP_ACCOUNT_SECRET_KEY
            is_temporary = True
        elif aws_common.MOCK_AWS_ACCOUNT_ASSUME_ROLE_ID in auth_header:
            account = aws_common.MOCK_AWS_ACCOUNT_ASSUME_ROLE_ID
            secret = aws_common.MOCK_AWS_ACCOUNT_ASSUME_ROLE_SECRET_KEY
            is_temporary = True
        else:
            raise ValueError("Unkown user in auth header: " + auth_header)

        if is_temporary and "x-amz-security-token" not in auth_header:
            print("BAD SIGNATURE - missing x-amz-security-token")
            return False
        elif (not is_temporary) and "x-amz-security-token" in auth_header:
            print("BAD SIGNATURE - extraneous x-amz-security-token")
            return False

        credentials = Credentials(account, secret)

        credential_prefix = "Credential=%s/" % (account)
        region_start = auth_header.find(credential_prefix) + len(credential_prefix + "YYYYMMDD/")
        region = auth_header[region_start:auth_header.find("/", region_start)]

        auth = SigV4Auth(credentials, "sts", region)
        print("CANN: %s" % (str(auth.canonical_request(request))))
        string_to_sign = auth.string_to_sign(request, auth.canonical_request(request))
        expected_signature = auth.signature(string_to_sign, request)

        signature_headers_start = auth_header.find("Signature=") + len("Signature=")
        actual_signature = auth_header[signature_headers_start:]

        if expected_signature != actual_signature:
            print("Actual: %s" % (actual_signature))
            print("Expected: %s,,,,%s" % (expected_signature, string_to_sign))
            return False

        return True

    def _do_get_caller_identity(self, auth_header):

        if aws_common.MOCK_AWS_ACCOUNT_ID in auth_header:
            arn = aws_common.MOCK_AWS_ACCOUNT_ARN
        elif aws_common.MOCK_AWS_TEMP_ACCOUNT_ID in auth_header:
            arn = aws_common.MOCK_AWS_TEMP_ACCOUNT_ARN
        elif aws_common.MOCK_AWS_ACCOUNT_ASSUME_ROLE_ID in auth_header:
            arn = aws_common.MOCK_AWS_ACCOUNT_ASSUME_ROLE_ARN
        else:
            raise ValueError("Unkown user in auth header: " + auth_header)

        if fault_type == FAULT_403:
            return self._do_get_caller_identity_faults()

        response = """<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
   <Arn>%s</Arn>
    <UserId>AIDACKCEVSQ6C2EXAMPLE</UserId>
    <Account>123456789012</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>01234567-89ab-cdef-0123-456789abcdef</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>""" % (arn)

        self._send_reply(response.encode("utf-8"))

    def _do_get_caller_identity_faults(self):
        if fault_type == FAULT_403:
            self._send_reply("Not allowed.".encode(), http.HTTPStatus.FORBIDDEN)
            return

        raise ValueError("Unknown Fault Type: %s" % (fault_type))

    def _send_header(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("content-type", "application/octet-stream")
        self.end_headers()

def run(port, server_class=http.server.HTTPServer, handler_class=AwsStsHandler):
    """Run web server."""
    server_address = ('', port)

    httpd = server_class(server_address, handler_class)

    print("Mock STS Web Server Listening on %s" % (str(server_address)))

    httpd.serve_forever()


def main():
    """Main Method."""
    global fault_type

    parser = argparse.ArgumentParser(description='MongoDB Mock AWS STS Endpoint.')

    parser.add_argument('-p', '--port', type=int, default=8000, help="Port to listen on")

    parser.add_argument('-v', '--verbose', action='count', help="Enable verbose tracing")

    parser.add_argument('--fault', type=str, help="Type of fault to inject")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if args.fault:
        if args.fault not in SUPPORTED_FAULT_TYPES:
            print("Unsupported fault type %s, supports types are %s" % (args.fault, SUPPORTED_FAULT_TYPES))
            sys.exit(1)

        fault_type = args.fault

    run(args.port)


if __name__ == '__main__':

    main()
