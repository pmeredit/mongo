#!/usr/bin/env python3
"""
This script is used to generate oidc_vars.js from oidc_vars.yml using oidc_vars.tpl.js.

Format of YaML document:

global:
  tokens_varname: 'kOIDCTokens'     # String(optional)   Variable name in which to emplace generated tokens
  payloads_varname: 'kOIDCPayloads' # String(optional)   Variable name in which to emplace generated payloads

tokens:                             # Sequence(optional) Top-level field containing a list of token definitions to be signed.
  - name: 'foo'                     # String(required)   Name for this token as it will be refered to from payloads and in tokens_varname.
    key: 'path/to/privkey.pem'      # String(required)   Path to a PEM encoded file containing the RSA key used for signing.
    alg: 'RS256'                    # String(required)   JWS signing algorithm to use.
    kid: 'foo-bar'                  # String(required)   Unique name encoded into the JWS header for the KeyID.
    body: {...}                     # Mapping(required)  A JWT body containing one or more claims.

payloads:                           # Sequence(optional) Top-level field containg a list of objects to be encoded as BSON using base64.
  - name: 'bar'                     # String(required)   Name for this payload as it will be refered to in payloads_varname.
    data: {....}                    # Mapping(required)  A series of BSON field names and value to encode.
                                    #                    Supported types include: Null, Bool, Int, String, Array, and Object.
                                    #                    Strings prefixed with 'token:' will use the signed content for token name which follows.
                                    #                    Integers will use int32 if possible, int64 otherwise.
"""

import argparse
import base64
import os
import re
import sys
from typing import Any, Dict, List, Optional

import yaml
from Cheetah.Template import Template

ENTERPRISE = "src/mongo/db/modules/enterprise/"
# Default config file to use if one is not specified.
CONFIGFILE = ENTERPRISE + "jstests/external_auth/lib/oidc_vars.yml"

# Default Cheetah template to use if one is not specified.
OUTPUT_TEMPLATE = ENTERPRISE + "jstests/external_auth/lib/oidc_vars.js.tpl"


def base64url_decode(s: str) -> bytes:
    """Wrap base64.urlsafe_b64decode() to account for python being strict about padding."""
    return base64.urlsafe_b64decode(s + "===="[0 : 4 - (len(s) % 4)])


def assert_required_str_field(mapping: Dict, fieldName: str) -> None:
    """Raise an error if the mapping field does not exist or is not a string."""
    if fieldName not in mapping:
        raise ValueError("Missing field '%s' in mapping %r" % (fieldName, mapping))

    value = mapping[fieldName]
    if type(value) is not str:
        raise ValueError(
            "Invalid type for string field '%s', got '%s' ': %r" % (fieldName, type(value), value)
        )


def get_nonempty_str_field(mapping: Dict, fieldName: str) -> str:
    """Extract a string field and assert it contains a value."""
    assert_required_str_field(mapping, fieldName)
    if len(mapping[fieldName]) == 0:
        raise ValueError("Field '%s' value must be a non-empty string in %r" % (fieldName, mapping))
    return mapping[fieldName]


def get_varname_field(mapping: Dict, fieldName: str, optional: bool = False) -> Optional[str]:
    """Extract a string field and validate it can be used as a varname."""
    if optional and (fieldName not in mapping):
        return None
    label = get_nonempty_str_field(mapping, fieldName)
    if re.match(r"^[a-zA-Z_]([a-zA-Z0-9_])*$", label) is None:
        raise ValueError(
            "%s must be a valid label using only alpha or underscore characters, or a digit not in the first position, got: %r"
            % (fieldName, label)
        )
    return label


class Global:
    def __init__(self, spec: Dict):
        """Construct an instance of Global."""
        self.tokens_varname = (
            get_varname_field(spec, "tokens_varname", optional=True) or "kOIDCTokens"
        )
        self.payloads_varname = (
            get_varname_field(spec, "payloads_varname", optional=True) or "kOIDCPayloads"
        )


class Token:
    def formatBody(self, spec: Any) -> str:
        if isinstance(spec, str):
            return f"`{spec}`"
        elif isinstance(spec, Dict):
            result = "{"
            for k, v in spec.items():
                result += f'"{k}": {self.formatBody(v)},'
            result += "}"
            return result
        else:
            return str(spec)

    def __init__(self, spec: Dict):
        """Construct an instance of Token."""
        self.name: str = get_nonempty_str_field(spec, "name")
        self.kid: str = get_nonempty_str_field(spec, "kid")
        self.key: str = get_nonempty_str_field(spec, "key")
        self.alg: str = get_nonempty_str_field(spec, "alg")
        if not os.path.exists(self.key):
            raise ValueError("Token %s: 'key' field must point to an extant file")
        if self.alg not in ["RS256", "RS384", "RS512"]:
            raise ValueError(
                "Token %s: Only RS256/384/512 algorithms supported, got '%s'"
                % (self.name, self.alg)
            )

        self.body: Dict = spec.get("body", {})
        if not isinstance(self.body, Dict):
            raise ValueError(
                "Token %s: Body must be a dictionary of key/value pairs, got: %r"
                % (self.name, self.body)
            )
        for k, v in self.body.copy().items():
            if v is None:
                del self.body[k]

        self.token: str = f"""OIDCsignJWT({{kid: "{self.kid}"}},{self.formatBody(self.body)}, "{self.key}", "{self.alg}")"""
        self.header: Dict = {
            "typ": "JWT",
            "kid": self.kid,
            "alg": self.alg,
        }


class Payload:
    def __init__(self, spec, tokens_by_name):
        """Construct an instance of Payload."""
        self.referenced_tokens: List[str] = []
        self.name: str = get_nonempty_str_field(spec, "name")
        self.spec: Dict = spec.get("data", {})
        if not isinstance(self.spec, Dict):
            raise ValueError(
                "Payload %s: Data must be a dictionary of key/value pairs, got: %r"
                % (self.name, self.spec)
            )
        self.data = self.interpolate(self.spec, tokens_by_name)
        self.bson: str = f"OIDCgenerateBSON({self.data})"

    def interpolate(self, spec: Any, tokens_by_name: Dict[str, Token]) -> str:
        """Substitute computed tokens for any string formatted as "token:name"."""
        if type(spec) is str:
            if spec.startswith("token:"):
                token_name = spec[6:]
                token = tokens_by_name.get(token_name)
                if token is None:
                    raise ValueError("Unknown token during interpolation: %s" % (token_name))
                self.referenced_tokens.append(token_name)
                return f'obj["kOIDCTokens"]["{token.name}"]'
            return f'"{spec}"'
        if isinstance(spec, Dict):
            result = "{"
            for k, v in spec.items():
                result += f'"{k}": {self.interpolate(v, tokens_by_name)},'
            result += "}"
            return result
        return str(spec)


def parse_command_line() -> argparse.Namespace:
    """Accept a named config file."""
    parser = argparse.ArgumentParser(description="OIDCVars generator")
    parser.add_argument("--config", help="OIDC vars defintion file", type=str, default=CONFIGFILE)
    parser.add_argument(
        "--template", help="Cheetah template to render", type=str, default=OUTPUT_TEMPLATE
    )
    parser.add_argument(
        "--output",
        help="Destination file for generated vars (default: stdout)",
        type=str,
        default="-",
    )
    return parser.parse_args()


def main():
    """Go go go."""
    args = parse_command_line()
    config = yaml.load(open(args.config, "r"), Loader=yaml.FullLoader)

    tokens = [Token(spec) for spec in config["tokens"]]
    tokens_by_name = {token.name: token for token in tokens}

    template_args = {
        "tokens": tokens,
        "payloads": [Payload(spec, tokens_by_name) for spec in config["payloads"]],
        "global": Global(config.get("global", {})),
    }

    dest = sys.stdout if args.output == "-" else open(args.output, "w")
    template = Template.compile(
        file=args.template,
        compilerSettings=dict(
            directiveStartToken="//#", directiveEndToken="//#", commentStartToken="//##"
        ),
        baseclass=dict,
        useCache=False,
    )
    dest.write(str(template(**template_args)))


if __name__ == "__main__":
    main()
