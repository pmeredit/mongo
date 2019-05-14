#! /usr/bin/env python3
"""Mock AWS KMS Endpoint."""

import argparse
import collections
import base64
import http.server
import json
import logging
import socketserver
import sys
import urllib.parse
import ssl


import bson
from bson.codec_options import CodecOptions
from bson.json_util import dumps
import mock_http_common

SECRET_PREFIX = "00SECRET"

# Pass this data out of band instead of storing it in FreeMonHandler since the
# BaseHTTPRequestHandler does not call the methods as object methods but as class methods. This
# means there is not self.
stats = mock_http_common.Stats()

"""Fault which causes metrics to trigger resentRegistration once."""
FAULT_RESEND_REGISTRATION_ONCE = "resend_registration_once"

# List of supported fault types
SUPPORTED_FAULT_TYPES = [
    FAULT_RESEND_REGISTRATION_ONCE,
]

class FreeMonHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests from Free Monitoring and test commands
    """
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        """Serve a Test GET request."""
        parts = urllib.parse.urlsplit(self.path)
        path = parts[2]

        if path == mock_http_common.URL_PATH_STATS:
            self._do_stats()
        elif path == mock_http_common.URL_PATH_LAST_REGISTER:
            self._do_last_register()
        elif path == mock_http_common.URL_PATH_LAST_METRICS:
            self._do_last_metrics()
        elif path == mock_http_common.URL_DISABLE_FAULTS:
            self._do_disable_faults()
        elif path == mock_http_common.URL_ENABLE_FAULTS:
            self._do_enable_faults()
        else:
            self.send_response(http.HTTPStatus.NOT_FOUND)
            self.end_headers()
            self.wfile.write("Unknown URL".encode())

    def do_POST(self):
        """Serve a Free Monitoring POST request."""
        parts = urllib.parse.urlsplit(self.path)
        path = parts[2]

        if path == "/":
            self._do_post()
        else:
            self.send_response(http.HTTPStatus.NOT_FOUND)
            self.end_headers()
            self.wfile.write("Unknown URL".encode())

    def _send_reply(self, data):
        self.send_response(http.HTTPStatus.OK)
        self.send_header("content-type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

        self.wfile.write(data)

    def _do_post(self):
        global stats
        clen = int(self.headers.get('content-length'))

        stats.register_calls += 1

        raw_input = self.rfile.read(clen)

        print("RAW INPUT: " + str(raw_input))

        # X-Amz-Target: TrentService.Encrypt
        aws_operation = self.headers['X-Amz-Target']

        if aws_operation == "TrentService.Encrypt":
            self._send_reply(self._do_encrypt(raw_input))
        elif aws_operation == "TrentService.Decrypt":
            self._send_reply(self._do_decrypt(raw_input))
        else:
            data = "Foo Bar TOO"
            self._send_reply(data.encode("utf-8"))

    def _do_encrypt(self, raw_input):
        request = json.loads(raw_input)

        print(request)

        plaintext = request["Plaintext"]
        keyid = request["KeyId"]

        ciphertext = SECRET_PREFIX.encode() + plaintext.encode()
        ciphertext = base64.b64encode(ciphertext).decode()

        print("Encoded the cipher")

        response = {
            "CiphertextBlob" : ciphertext,
            "KeyId" : keyid,
        }

        print(response)

        return json.dumps(response).encode('utf-8')

    def _do_decrypt(self, raw_input):
        request = json.loads(raw_input)
        blob = base64.b64decode(request["CiphertextBlob"]).decode()

        print("FOUND SECRET: " + blob)

        # our "encrypted" values start with the word SECRET_PREFIX otherwise they did not come from us
        if not blob.startswith(SECRET_PREFIX):
            raise ValueError()

        blob = blob[len(SECRET_PREFIX):]

        # TODO - add corrupt b64 fault
        response = {
            "Plaintext" : blob,
            "KeyId" : "Not a clue",
        }

        return json.dumps(response).encode('utf-8')

    def _do_stats(self):
        self._send_header()

        self.wfile.write(str(stats).encode('utf-8'))

    def _do_last_register(self):
        self._send_header()

        self.wfile.write(str(last_register).encode('utf-8'))

    def _do_last_metrics(self):
        self._send_header()

        self.wfile.write(str(last_metrics).encode('utf-8'))

    def _do_disable_faults(self):
        global disable_faults
        disable_faults = True
        self._send_header()

    def _do_enable_faults(self):
        global disable_faults
        disable_faults = False
        self._send_header()

def run(port, server_class=http.server.HTTPServer, handler_class=FreeMonHandler):
    """Run web server."""
    server_address = ('', port)

    httpd = server_class(server_address, handler_class)

    httpd.socket = ssl.wrap_socket (httpd.socket,
        certfile="jstests/libs/server.pem",
        ca_certs='jstests/libs/ca.pem', server_side=True)

    print("Mock KMS Server Listening on %s" % (str(server_address)))

    httpd.serve_forever()


def main():
    """Main Method."""
    global fault_type
    global disable_faults

    parser = argparse.ArgumentParser(description='MongoDB Mock Free Monitoring Endpoint.')

    parser.add_argument('-p', '--port', type=int, default=8000, help="Port to listen on")

    parser.add_argument('-v', '--verbose', action='count', help="Enable verbose tracing")

    parser.add_argument('--fault', type=str, help="Type of fault to inject")

    parser.add_argument('--disable-faults', action='store_true', help="Disable faults on startup")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if args.fault:
        if args.fault not in SUPPORTED_FAULT_TYPES:
            print("Unsupported fault type %s, supports types are %s" % (args.fault, SUPPORTED_FAULT_TYPES))
            sys.exit(1)

        fault_type = args.fault

    if args.disable_faults:
        disable_faults = True

    run(args.port)


if __name__ == '__main__':

    main()