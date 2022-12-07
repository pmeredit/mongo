#! /usr/bin/env python3
"""Key server for OIDC tests"""

import argparse
import http.server
import logging
import socketserver
import sys

jwk_file=""

class KeyServerHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests for OIDC keys.
    """
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        """Serve a GET request, ignore path."""
        global jwk_file

        self.send_response(http.HTTPStatus.OK)
        self.send_header('Content-Type', 'text/json')
        self.send_header('Content-Length', str(len(jwk_file)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(jwk_file)

def main():
    """Main Method."""
    global jwk_file

    parser = argparse.ArgumentParser(description='MongoDB OIDC Key Server.')

    parser.add_argument('-p', '--port', type=int, default=8000, help="Port to listen on")
    parser.add_argument('-v', '--verbose', action='count', help="Enable verbose tracing")
    parser.add_argument('jwk', type=str, help="Keyfile to return")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    jwk_file = open(args.jwk, 'rb').read()
    server_address = ('', args.port)
    httpd = http.server.HTTPServer(server_address, KeyServerHandler)
    print("OIDC Key Server Listening on %s" % (str(server_address)))
    httpd.serve_forever()

if __name__ == '__main__':
    main()
