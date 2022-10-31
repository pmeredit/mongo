#!/usr/bin/env python3
"""
This script generates a BSON payload in base64 format from a JSON object.
"""

import argparse
import base64
import bson
import json

def parse_command_line() -> argparse.Namespace:
    """Parse arguments."""
    parser = argparse.ArgumentParser(description='BSON payload generator')
    parser.add_argument('--payload', help='JSON payload to encode as BSON', type=str)
    return parser.parse_args()

def main():
    """Go go go."""
    args = parse_command_line()

    payload = bson.encode(json.loads(args.payload))
    print(base64.b64encode(payload).decode())

if __name__ == '__main__':
    main()
