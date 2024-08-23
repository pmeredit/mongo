load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")
load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("//bazel/config:configs.bzl", "developer_dir_provider")

def generate_config_header_impl(ctx):
    cc_toolchain = find_cpp_toolchain(ctx)
    compiler_bin = cc_toolchain.compiler_executable
    input = ctx.attr.template.files.to_list()[0].path
    checks = ctx.attr.checks.files.to_list()[0].path

    # Generate compiler flags we need to make clang/gcc/msvc actually compile.
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
    )
    compile_variables = cc_common.create_compile_variables(
        feature_configuration = feature_configuration,
        cc_toolchain = cc_toolchain,
        user_compile_flags = ctx.fragments.cpp.cxxopts + ctx.fragments.cpp.copts,
    )
    compiler_flags = cc_common.get_memory_inefficient_command_line(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.cpp_compile,
        variables = compile_variables,
    )
    env_flags = cc_common.get_environment_variables(
        feature_configuration = feature_configuration,
        action_name = ACTION_NAMES.cpp_compile,
        variables = compile_variables,
    )

    python = ctx.toolchains["@bazel_tools//tools/python:toolchain_type"].py3_runtime
    generator_script = ctx.attr.generator_script.files.to_list()[0].path

    additional_inputs = []
    additional_inputs_depsets = []
    for additional_input in ctx.attr.additional_inputs:
        files = additional_input.files.to_list()
        additional_inputs_depsets.append(additional_input.files)
        for file in files:
            additional_inputs.append("--additional-input")
            additional_inputs.append(file.path)

    ctx.actions.run(
        executable = python.interpreter.path,
        outputs = [ctx.outputs.output, ctx.outputs.logfile],
        mnemonic = "ConfigHeaderGen",
        inputs = depset(transitive = [
            cc_toolchain.all_files,
            python.files,
            ctx.attr.generator_script.files,
            ctx.attr.template.files,
            ctx.attr.checks.files,
        ] + additional_inputs_depsets),
        env = {"DEVELOPER_DIR": ctx.attr._developer_dir[developer_dir_provider].path},
        arguments = [
                        generator_script,  # bazel/config/mongo_config_header.py
                        "--output-path",
                        ctx.outputs.output.path,
                        "--template-path",
                        input,
                        "--check-path",
                        checks,
                        "--log-path",
                        ctx.outputs.logfile.path,
                        "--compiler-path",
                        compiler_bin,
                        "--extra-definitions",
                        json.encode(ctx.attr.extra_definitions),
                    ] +
                    additional_inputs +
                    [
                        "--compiler-args",
                        " ".join(compiler_flags),
                        "--env-vars",
                        json.encode(env_flags),
                    ],
    )

    return [DefaultInfo(files = depset([ctx.outputs.output]))]

generate_config_header = rule(
    generate_config_header_impl,
    attrs = {
        "output": attr.output(
            doc = "The output of this rule.",
            mandatory = True,
        ),
        "logfile": attr.output(
            doc = "The logfile of this rule.",
            mandatory = True,
        ),
        "template": attr.label(
            doc = "The template file used to generate the header.",
            allow_single_file = True,
        ),
        "checks": attr.label(
            doc = "The input checks python script to run for this rule.",
            allow_single_file = True,
        ),
        "extra_definitions": attr.string_dict(
            doc = "Extra definitions to set.",
            default = {},
        ),
        "additional_inputs": attr.label_list(
            doc = "Additional inputs to this rule.",
            allow_files = True,
        ),
        "generator_script": attr.label(
            doc = "The python generator script to use.",
            default = "//bazel/config:generate_config_header.py",
            allow_single_file = True,
        ),
        "_cc_toolchain": attr.label(default = "@bazel_tools//tools/cpp:current_cc_toolchain"),
        "_developer_dir": attr.label(default = "//bazel/config:developer_dir"),
    },
    fragments = ["cpp"],
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type", "@bazel_tools//tools/python:toolchain_type"],
    output_to_genfiles = True,
)
