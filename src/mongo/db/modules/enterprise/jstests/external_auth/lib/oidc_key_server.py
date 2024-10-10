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

GET requests to /keyname/jwks will return that JWK files.
GET requests to /keyname/.well-known/openid-configuration will return issuer
 metadata.
"""

import argparse
import http.server
import json
import logging
import urllib

jwk_map = {}


class KeyServerHandler(http.server.BaseHTTPRequestHandler):
    """
    Handle requests for OIDC keys.
    """

    protocol_version = "HTTP/1.1"

    # /rotateKeys endpoint allows for key rotation.
    def doRotateKeys(self, parts):
        global jwk_map

        query_dict = urllib.parse.parse_qs(parts.query)
        new_jwk_map_str = query_dict.get("map", None)
        if new_jwk_map_str is None:
            msg = "Map query parameter not provided"
            return self.reply(msg, "text/plain", http.HTTPStatus.BAD_REQUEST)

        print("New JWK map: " + new_jwk_map_str[0])
        jwk_map = json.loads(new_jwk_map_str[0])
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "text/json")
        self.send_header("Connection", "close")
        self.end_headers()
        return None

    # Emit OpenID Connect Discovery metadata for an issuer
    def doOpenIdConfiguration(self, issuer):
        port = self.server.server_address[1]
        metadata = {
            "issuer": f"http://localhost:{port}/{issuer}",
            "authorization_endpoint": f"http://localhost:{port}/{issuer}/authorize",
            "token_endpoint": f"http://localhost:{port}/{issuer}/token",
            "jwks_uri": f"http://localhost:{port}/{issuer}/jwks",
        }
        return self.reply(json.dumps(metadata), "text/json", http.HTTPStatus.OK)

    # Emit an issuer's JWKS
    def doJWKS(self, issuer):
        global jwk_map

        jwk_file = jwk_map.get(issuer, None)
        if jwk_file is None:
            msg = "Unknown URL".encode()
            return self.reply(msg, "text/plain", http.HTTPStatus.NOT_FOUND)

        jwk = open(jwk_file, "rb").read()
        self.reply(jwk, "text/json", http.HTTPStatus.OK)

    def do_GET(self):
        """Serve a GET request."""

        parts = urllib.parse.urlsplit(self.path)
        path = parts.path

        match path.split("/")[1:]:
            case ["rotateKeys"]:
                return self.doRotateKeys(parts)
            case [issuer, ".well-known", "openid-configuration"]:
                return self.doOpenIdConfiguration(issuer)
            case [issuer, "jwks"]:
                return self.doJWKS(issuer)
            case _:
                msg = "Location not found"
                return self.reply(msg, "text/plain", http.HTTPStatus.NOT_FOUND)

    def reply(self, msg, contentType, err):
        self.send_response(err)
        self.send_header("Content-Type", contentType)
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        if not isinstance(msg, bytes):
            msg = msg.encode()
        self.wfile.write(msg)
        return None


def main():
    """Main Method."""
    global jwk_map

    parser = argparse.ArgumentParser(description="MongoDB OIDC Key Server.")

    parser.add_argument("-p", "--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("-v", "--verbose", action="count", help="Enable verbose tracing")
    parser.add_argument("jwk", type=str, help="Map of keyfiles to return")

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    jwk_map = json.loads(args.jwk)

    server_address = ("", args.port)
    httpd = http.server.HTTPServer(server_address, KeyServerHandler)
    print("OIDC Key Server Listening on %s" % (str(server_address)))
    httpd.serve_forever()


if __name__ == "__main__":
    main()
