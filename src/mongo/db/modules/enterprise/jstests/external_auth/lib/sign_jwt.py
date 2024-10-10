#!/usr/bin/env python3
"""
This script accepts a JWT header, body, and key file and produces a signed JWS as output.
"""

import argparse
import json

import jwt


def parse_command_line() -> argparse.Namespace:
    """Accept a named config file."""
    parser = argparse.ArgumentParser(description="JWS token signer")
    parser.add_argument("--header", help="JWS header", type=str, default="{}")
    parser.add_argument("--token", help="JWT token", type=str)
    parser.add_argument("--algorithm", help="Signature algorithm", type=str, default="RS256")
    parser.add_argument("--key", help="Path to RSA private key", type=str)
    return parser.parse_args()


def main():
    """Go go go."""
    args = parse_command_line()

    key = open(args.key, "r").read()

    headers = json.loads(args.header)
    if ("alg" in headers) and (args.algorithm != headers["alg"]):
        raise ValueError(
            "Signing algoritm %s does not match header %s" % (args.algorithm, headers["alg"])
        )

    jws = jwt.encode(json.loads(args.token), key, algorithm=args.algorithm, headers=headers)

    # Different versions of PyJWT use different output types.
    if type(jws) is bytes:
        print(jws.decode())
    else:
        print(jws)


if __name__ == "__main__":
    main()
