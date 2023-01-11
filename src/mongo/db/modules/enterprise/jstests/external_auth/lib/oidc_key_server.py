#! /usr/bin/env python3
"""
Key server for OIDC tests.

Primary argument is a JSON string containing a map of key names and JWK files.
e.g.
{
  "custom-key-1": "custom-key-1.json",
  "custom-key-2": "custom-key-2.json",
  "custom-key-all": "custom-key-all.json",
}

GET requests to /keyname will return that file.
"""

import argparse
import http.server
import json
import logging
import socketserver
import sys
import urllib

jwk_map={}

class KeyServerHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests for OIDC keys.
    """
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        """Serve a GET request."""
        global jwk_map
        parts = urllib.parse.urlsplit(self.path)
        path = parts.path

        # /rotateKeys endpoint allows for key rotation.
        if path == '/rotateKeys':
            query_dict = urllib.parse.parse_qs(parts.query)
            new_jwk_map_str = query_dict.get('map', None)
            if new_jwk_map_str is None:
                msg = "Map query parameter not provided".encode()
                return self.construct_error(msg, http.HTTPStatus.BAD_REQUEST)

            jwk_map = json.loads(new_jwk_map_str[0])
            self.send_response(http.HTTPStatus.OK)
            self.send_header('Content-Type', 'text/json')
            self.send_header('Connection', 'close')
            self.end_headers()
            return None

        jwk_file = jwk_map.get(path[1:], None)
        if jwk_file is None:
            msg = "Unknown URL".encode()
            return self.construct_error(msg, http.HTTPStatus.NOT_FOUND)

        jwk = open(jwk_file, 'rb').read()
        self.send_response(http.HTTPStatus.OK)
        self.send_header('Content-Type', 'text/json')
        self.send_header('Content-Length', str(len(jwk)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(jwk)

    def construct_error(self, msg, err):
        self.send_response(err)
        self.send_header("Content-Type", "text/plain");
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)
        return None

def main():
    """Main Method."""
    global jwk_map

    parser = argparse.ArgumentParser(description='MongoDB OIDC Key Server.')

    parser.add_argument('-p', '--port', type=int, default=8000, help="Port to listen on")
    parser.add_argument('-v', '--verbose', action='count', help="Enable verbose tracing")
    parser.add_argument('jwk', type=str, help="Map of keyfiles to return")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    jwk_map = json.loads(args.jwk)
    server_address = ('', args.port)
    httpd = http.server.HTTPServer(server_address, KeyServerHandler)
    print("OIDC Key Server Listening on %s" % (str(server_address)))
    httpd.serve_forever()

if __name__ == '__main__':
    main()
