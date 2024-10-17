load("//bazel/platforms:remote_execution_containers.bzl", "REMOTE_EXECUTION_CONTAINERS")
load("//bazel/toolchains:mongo_toolchain_version.bzl", "TOOLCHAIN_MAP")
load("//bazel:utils.bzl", "get_host_distro_major_version")

_OS_MAP = {
    "macos": "@platforms//os:osx",
    "linux": "@platforms//os:linux",
    "windows": "@platforms//os:windows",
}

_ARCH_MAP = {
    "amd64": "@platforms//cpu:x86_64",
    "aarch64": "@platforms//cpu:arm64",
    "x86_64": "@platforms//cpu:x86_64",
    "ppc64le": "@platforms//cpu:ppc64le",
    "s390x": "@platforms//cpu:s390x",
}

def _setup_local_config_platform(ctx):
    """
    Generates our own local_config_platform, overriding bazel's built in generation.

    This allows is to setup the exec_properties on this platform so a user can use remote execution
    without need to specify a specific platform.
    """

    if "win" in ctx.os.name:
        os = "windows"
    elif "mac" in ctx.os.name:
        os = "macos"
    else:
        os = "linux"

    arch = ctx.os.arch

    os_constraint = _OS_MAP[os]
    arch_constraint = _ARCH_MAP[arch]

    constraints = [os_constraint, arch_constraint]

    # So Starlark doesn't throw an indentation error when this gets injected.
    constraints_str = ",\n        ".join(['"%s"' % c for c in constraints])

    distro = get_host_distro_major_version(ctx)
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "aarch64":
        arch = "arm64"

    # EngFlow's "default" pool is ARM64
    remote_execution_pool = "x86_64" if arch == "amd64" else "default"
    result = None
    toolchain_key = "{distro}_{arch}".format(distro = distro, arch = arch)
    if ctx.os.environ.get("USE_NATIVE_TOOLCHAIN"):
        exec_props = ""
        result = {"USE_NATIVE_TOOLCHAIN": "1"}
    elif distro != None and distro in REMOTE_EXECUTION_CONTAINERS:
        constraints_str += ',\n        "@//bazel/platforms:use_mongo_toolchain"'
        container_url = REMOTE_EXECUTION_CONTAINERS[distro]["container-url"]
        web_url = REMOTE_EXECUTION_CONTAINERS[distro]["web-url"]
        dockerfile = REMOTE_EXECUTION_CONTAINERS[distro]["dockerfile"]
        print("Local host platform is configured to use this container if doing remote execution: {} built from {}".format(web_url, dockerfile))
        exec_props = """
    exec_properties = {
        "container-image": "%s",
        "dockerNetwork": "standard",
        "Pool": "%s",
    },
""" % (container_url, remote_execution_pool)
        result = {"DISTRO": distro}
    elif distro != None and distro in TOOLCHAIN_MAP:
        constraints_str += ',\n        "@//bazel/platforms:use_mongo_toolchain"'
        result = {"DISTRO": distro}
        exec_props = ""
    else:
        result = {"USE_NATIVE_TOOLCHAIN": "1"}
        exec_props = ""

    platform_constraints_str = constraints_str

    substitutions = {
        "{constraints}": constraints_str,
        "{platform_constraints}": platform_constraints_str,
        "{exec_props}": exec_props,
    }

    ctx.template(
        "BUILD.bazel",
        ctx.attr.build_tpl,
        substitutions = substitutions,
    )

    ctx.template(
        "constraints.bzl",
        ctx.attr.constraints_tpl,
        substitutions = substitutions,
    )

    return result

setup_local_config_platform = repository_rule(
    implementation = _setup_local_config_platform,
    attrs = {
        "build_tpl": attr.label(
            default = "//bazel/platforms:local_config_platform.BUILD",
            doc = "Template modeling the builtin local config platform build file.",
        ),
        "constraints_tpl": attr.label(
            default = "//bazel/platforms:local_config_platform_constraints.bzl",
            doc = "Template modeling the builtin local config platform constraints file.",
        ),
    },
    environ = ["USE_NATIVE_TOOLCHAIN"],
)
