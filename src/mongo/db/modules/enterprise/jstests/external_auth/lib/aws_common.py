"""Loader for common AWS constants"""

import json
import os

# Load up our account data and such
_source_file = os.path.dirname(__file__) + "/aws_common.json"
with open(_source_file) as f:
    _details = json.load(f)


def get_users():
    """Returns the users section of the caches json file"""
    return _details["users"]
