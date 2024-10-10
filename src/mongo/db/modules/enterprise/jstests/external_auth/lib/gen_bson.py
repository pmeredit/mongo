#!/usr/bin/env python3
"""
This script generates a BSON payload in base64 format from a JSON object.
"""

import argparse
import base64
import json

import bson


def parse_command_line() -> argparse.Namespace:
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="BSON payload generator")
    parser.add_argument("--payload", help="JSON payload to encode as BSON", type=str)
    parser.add_argument(
        "--output_file",
        help="Output BSON to the specified file instead of stdout",
        type=str,
        required=False,
    )
    return parser.parse_args()


def main():
    """Go go go."""
    args = parse_command_line()

    payload = bson.encode(json.loads(args.payload))

    if args.output_file:
        with open(args.output_file, "w+", encoding="utf-8") as out_file:
            out_file.write(base64.b64encode(payload).decode())
            out_file.close()
    else:
        print(base64.b64encode(payload).decode())


if __name__ == "__main__":
    main()
