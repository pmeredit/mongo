#! /usr/bin/env python3
"""Mock AWS ECS Metadata Endpoint."""

import argparse
import collections
import base64
import http.server
import json
import logging
import socketserver
import sys
import urllib.parse

import aws_common

fault_type = None

MOCK_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI = "/v2/credentials/e619b4a8-9c02-47ac-b941-52f3b6cf5d06"

"""Fault which causes encrypt to return 500."""
FAULT_500 = "fault_500"

# List of supported fault types
SUPPORTED_FAULT_TYPES = [
    FAULT_500,
]


class AwsECSMetadataHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests from AWS ECS Metadata Monitoring and test commands
    """

    # HTTP 1.1 requires us to always send the length back
    # protocol_version = "HTTP/1.1"

    def do_GET(self):
        """Serve a Test GET request."""
        parts = urllib.parse.urlsplit(self.path)
        path = parts[2]

        if path == MOCK_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI:
            self._do_security_credentials_mock_role()
        else:
            self.send_response(http.HTTPStatus.NOT_FOUND)
            self.end_headers()
            self.wfile.write("Unknown URL".encode())

    def _send_reply(self, data, status=http.HTTPStatus.OK):
        print("Sending Response: " + data.decode())

        self.send_response(status)
        self.send_header("content-type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

        self.wfile.write(data)

    def _send_header(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("content-type", "application/octet-stream")
        self.end_headers()

    def _do_security_credentials_mock_role(self):
        if fault_type == FAULT_500:
            return self._do_security_credentials_mock_role_faults()

        self._send_header()

        user = aws_common.get_users()["tempUser"]
        str1 = f"""{{
  "RoleArn" : "arn:aws:iam::1234567890:role/ecsTaskExecutionRole",
  "AccessKeyId" : "{user['id']}",
  "SecretAccessKey" : "{user['secretKey']}",
  "Token" : "{user['sessionToken']}",
  "Expiration" : "2019-11-20T23:37:45Z"
}}"""

        self.wfile.write(str1.encode("utf-8"))

    def _do_security_credentials_mock_role_faults(self):
        if fault_type == FAULT_500:
            self._send_reply("Fake Internal Error.".encode(), http.HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        raise ValueError("Unknown Fault Type: %s" % (fault_type))


def run(port, server_class=http.server.HTTPServer, handler_class=AwsECSMetadataHandler):
    """Run web server."""
    server_address = ("", port)

    httpd = server_class(server_address, handler_class)

    print("Mock ECS Instance Metadata Web Server Listening on %s" % (str(server_address)))

    httpd.serve_forever()


def main():
    """Main Method."""
    global fault_type

    parser = argparse.ArgumentParser(description="MongoDB Mock AWS ECS Metadata Endpoint.")

    parser.add_argument("-p", "--port", type=int, default=8000, help="Port to listen on")

    parser.add_argument("-v", "--verbose", action="count", help="Enable verbose tracing")

    parser.add_argument("--fault", type=str, help="Type of fault to inject")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if args.fault:
        if args.fault not in SUPPORTED_FAULT_TYPES:
            print(
                "Unsupported fault type %s, supports types are %s"
                % (args.fault, SUPPORTED_FAULT_TYPES)
            )
            sys.exit(1)

        fault_type = args.fault

    run(args.port)


if __name__ == "__main__":
    main()
