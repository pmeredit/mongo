#!/usr/bin/env python3
"""
Run a local rest receiver for testing streams http requests.  This server
will log and output of the incoming requests to a known directory for the jstests
to pick it up from and run tests against. Additionally there is an echo feature for test cases.
"""

import argparse
import datetime
import io
import json
import logging
import os
import subprocess
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib import parse

logging.basicConfig(
    level=logging.NOTSET, format="%(name)s: %(asctime)s | %(levelname)s >>> %(message)s"
)
logger = logging.getLogger("Python REST Server")


class RESTServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, write_path):
        self.counter = defaultdict(int)
        self.write_path = write_path
        super().__init__(server_address, RequestHandlerClass)


# Simple webserver to receive and store rest requests
class RequestHandler(BaseHTTPRequestHandler):
    def _write_request_to_disk(self, parsed_path, request_json) -> None:
        self._log_with_server_details(request_json)

        # Write out request to temporary files
        try:
            job_name = parsed_path.path.split("/")[2]
        except:
            job_name = parsed_path.path.strip("/").replace("/", "_")

        job_counter = self.server.counter[job_name]
        logger.info(f"job: {job_name}_{job_counter}")

        try:
            with open(
                os.path.join(self.server.write_path, "{}_{}.json".format(job_name, job_counter)),
                "w",
            ) as f:
                f.write(json.dumps(request_json))
        except Exception as ex:
            logger.warning("Unable to write out file, giving up: {}".format(ex))
            raise

        self.server.counter[job_name] += 1

    def _extract_received_request(self, parsed_path):
        content_length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        if self.headers.get("content-type", "") == "application/json" and content_length > 0:
            body = json.loads(body)

        received_request = {
            "method": self.command,
            "path": parsed_path.path,
            "query": parse.parse_qs(parsed_path.query),
            "headers": dict(self.headers),
            "body": body,
        }

        return received_request

    def _log_with_server_details(self, received_request):
        fullLogRequest = dict(received_request)
        fullLogRequest["received_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        fullLogRequest["client"] = {
            "host": self.client_address[0],
            "port": self.client_address[1],
        }
        fullLogRequest["server"] = {
            "host": self.server.server_address[0],
            "port": self.server.server_address[1],
        }
        logger.info(f"{json.dumps(fullLogRequest)}")

    def _simple_handle(self, parsed_path, response_code):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(response_code)
        self.end_headers()
        self.wfile.write(io.BytesIO().getvalue())

    def _plain_text_handle(self, parsed_path, response_code):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(response_code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write("A_VALID_PLAINTEXT_RESPONSE".encode("utf-8"))

    def _large_payload_handle(self, parsed_path, response_code):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(response_code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()

        self.wfile.write(os.urandom(10000))

    def _object_with_serialized_fields(self, parsed_path, response_code):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(response_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"content": '{"foo": "bar"}', "nested": {"data": '{"abc": "xyz"}'}}).encode(
                "utf-8"
            )
        )

    def _array_with_serialized_fields(self, parsed_path, response_code):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(response_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps([{"content": '{"foo": "bar"}'}, {"content": '{"abc": "xyz"}'}]).encode(
                "utf-8"
            )
        )

    def _echo_handle(self, parsed_path):
        received_request = self._extract_received_request(parsed_path)

        self._write_request_to_disk(parsed_path, received_request)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(received_request).encode("utf-8"))

    def _handle(self):
        parsed_path = parse.urlparse(self.path)

        if parsed_path.path.startswith("/echo"):
            self._echo_handle(parsed_path)
        elif parsed_path.path.startswith("/notfound"):
            self._simple_handle(parsed_path, 404)
        elif parsed_path.path.startswith("/unauthorized"):
            self._simple_handle(parsed_path, 403)
        elif parsed_path.path.startswith("/servfail"):
            self._simple_handle(parsed_path, 500)
        elif parsed_path.path.startswith("/plaintext"):
            self._plain_text_handle(parsed_path, 200)
        elif parsed_path.path.startswith("/largepayload"):
            self._large_payload_handle(parsed_path, 200)
        elif parsed_path.path.startswith("/jsonObjectWithSerializedFields"):
            self._object_with_serialized_fields(parsed_path, 200)
        elif parsed_path.path.startswith("/jsonArrayWithSerializedFields"):
            self._array_with_serialized_fields(parsed_path, 200)
        else:
            self._echo_handle(parsed_path)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def do_PATCH(self):
        self._handle()

    def do_DELETE(self):
        self._handle()

    def do_HEAD(self):
        self._handle()

    def do_OPTIONS(self):
        self._handle()

    def log_message(self, format, *args):
        pass


def run(port, directory) -> int:
    dirPath = Path(directory)
    if not dirPath.exists():
        try:
            os.makedirs(directory)
        except Exception as ex:
            logger.warning("Unable to create directory, giving up: {}".format(ex))
            raise
    elif not dirPath.is_dir():
        logger.warning("Directory path is already a file, giving up")
        raise

    host = "localhost"
    with RESTServer((host, port), RequestHandler, directory) as server:
        logger.info(f"Running on {host}:{port}")
        server.serve_forever()


MIN_LIBCURL_VERSION = "7.78.0"
MIN_LIBCURL_MAJOR_VERSION = 7
MIN_LIBCURL_MINOR_VERSION = 78
MIN_LIBCURL_PATCH_VERSION = 0


def check_deps():
    # check whether this host has sufficient curl version
    result = subprocess.run(["curl", "--version"], capture_output=True, text=True)
    tokens = result.stdout.split(" ")
    version = tokens[1]
    logger.info(f"Curl version: {version}")

    semver = version.split(".")
    received_major_version = int(semver[0])
    received_minor_version = int(semver[1])
    received_patch_version = int(semver[2])
    if received_major_version < MIN_LIBCURL_MAJOR_VERSION:
        raise ValueError(
            f"Insufficient curl version. Expected >= {MIN_LIBCURL_VERSION}. Got {version}."
        )
    if received_minor_version < MIN_LIBCURL_MINOR_VERSION:
        raise ValueError(
            f"Insufficient curl version. Expected >= {MIN_LIBCURL_VERSION}. Got {version}."
        )
    if received_patch_version < MIN_LIBCURL_PATCH_VERSION:
        raise ValueError(
            f"Insufficient curl version. Expected >= {MIN_LIBCURL_VERSION}. Got {version}."
        )


if __name__ == "__main__":
    path = Path(__file__)
    os.chdir(path.parent.absolute())

    parser = argparse.ArgumentParser(description="Rest Server For Testing")

    parser.add_argument("-p", "--port", action="store", type=int, help="HTTP Listen Port")
    parser.add_argument(
        "-d",
        "--directory",
        action="store",
        type=str,
        help="HTTP incoming requests directory",
    )
    parser.add_argument(
        "-c",
        "--check",
        action="store_true",
        help="Whether to perform dependency check",
    )

    sub = parser.add_subparsers(title="Rest Server subcommands", help="sub-command help")

    (args, _) = parser.parse_known_args()
    if args.check:
        check_deps()
    else:
        run(args.port, args.directory)
