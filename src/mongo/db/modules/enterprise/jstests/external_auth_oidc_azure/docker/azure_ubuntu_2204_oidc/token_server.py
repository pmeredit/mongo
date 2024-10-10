import http.server
import json
import traceback
import urllib.parse
from os import environ

import requests


class SimpleHttpServerImpl(http.server.BaseHTTPRequestHandler):
    def get_token(self, resource, object_id, api_version):
        endpoint = environ.get("IDENTITY_ENDPOINT")
        identity_header = environ.get("IDENTITY_HEADER")

        if not endpoint:
            raise Exception("Unable to get IDENTITY_ENDPOINT from env!")

        if not identity_header:
            raise Exception("Unable to get IDENTITY_HEADER from env!")

        req_params = {"api-version": api_version, "resource": resource, "object_id": object_id}

        req_headers = {"x-identity-header": identity_header}
        r = requests.get(endpoint, params=req_params, headers=req_headers)

        r.raise_for_status()

        return r.content

    def do_GET(self):
        response = ""

        try:
            parsed_path = urllib.parse.urlparse(self.path)
            parsed_query = urllib.parse.parse_qs(parsed_path.query)

            resource_field = parsed_query["resource"]
            object_id = parsed_query["object_id"]
            api_version = parsed_query["api-version"]

            # The JSON that comes back from MS here is escaped as a text blob, and thus we have to do some post-processing on it in order to parse - notably it uses
            # single quotes instead of the double quotes required by the JSON standard
            response = (
                self.get_token(resource_field, object_id, api_version).decode().replace("'", '"')
            )

        except Exception as e:
            response = json.dumps({"error": str(e), "trace": str(traceback.format_exc())})

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(response, "utf-8"))


server_address = ("127.0.0.1", 45678)
httpd = http.server.HTTPServer(server_address, SimpleHttpServerImpl)
httpd.serve_forever()
