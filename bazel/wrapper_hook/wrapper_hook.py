import os
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.append(str(REPO_ROOT))

# This script should be careful not to disrupt automatic mechanism which
# may be expecting certain stdout, always print to stderr.
sys.stdout = sys.stderr

from bazel.wrapper_hook.install_modules import install_modules
from bazel.wrapper_hook.wrapper_debug import wrapper_debug

wrapper_debug(f"wrapper hook script is using {sys.executable}")


def main():
    install_modules(sys.argv[1])

    from bazel.wrapper_hook.developer_bes_keywords import write_workstation_bazelrc
    from bazel.wrapper_hook.engflow_check import engflow_auth
    from bazel.wrapper_hook.plus_interface import test_runner_interface

    engflow_auth(sys.argv)

    write_workstation_bazelrc(sys.argv)

    args = test_runner_interface(
        sys.argv[1:], autocomplete_query=os.environ.get("MONGO_AUTOCOMPLETE_QUERY") == "1"
    )
    os.chmod(os.environ.get("MONGO_BAZEL_WRAPPER_ARGS"), 0o644)
    with open(os.environ.get("MONGO_BAZEL_WRAPPER_ARGS"), "w") as f:
        f.write("\n".join(args))
        f.write("\n")


if __name__ == "__main__":
    main()
