import os
import os.path
import SCons.Script.Main


def configure(conf, env):
    root = os.path.dirname(__file__)

    env.AddOption(
        "enterprise-features",
        default="*",
        dest="enterprise_features",
        action="store",
        type="string",
        const="*",
        nargs="?",
    )

    enterprise_features_option = env.GetOption("enterprise_features")

    all_features = [
        "audit",
        "encryptdb",
        "fcbis",
        "fips",
        "fle",
        "hot_backups",
        "inmemory",
        "kerberos",
        "ldap",
        "live_import",
        "magic_restore",
        "mongohouse",
        "queryable",
        "sasl",
        "streams",
        "serverless",
    ]

    selected_features = []
    configured_modules = []

    env["MONGO_ENTERPRISE_VERSION"] = True
    if enterprise_features_option == "*":
        selected_features = all_features
        configured_modules.append("enterprise")
    else:
        for choice in enterprise_features_option.split(","):
            if choice in all_features and not choice in selected_features:
                selected_features.append(choice)
            else:
                env.ConfError(
                    "Bad --enterprise-feature choice '{}'; choose from {} or '*'".format(
                        choice, ", ".join(all_features)
                    )
                )

        if "sasl" in selected_features and not "ldap" in selected_features:
            env.ConfError("the sasl enterprise feature depends on the ldap enterprise feature")
        if "search" in selected_features and not "fle" in selected_features:
            env.ConfError("the search enterprise feature depends on the fle enterprise " "feature")
        configured_modules.extend(selected_features)

    env["MONGO_ENTERPRISE_FEATURES"] = selected_features

    if "sasl" in env["MONGO_ENTERPRISE_FEATURES"]:
        env["MONGO_BUILD_SASL_CLIENT"] = True

    # Compute a path to use for including files that are generated in the build directory.
    # Computes: $BUILD_DIR/mongo/db/modules/<enterprise_module_name>/src
    src_include_path = os.path.join("$BUILD_DIR", str(env.Dir(root).Dir("src"))[4:])

    def injectEnterpriseModule(env, consumer=True, builder=False):
        # Inject an include path so that idl generated files can be included
        env.Append(CPPPATH=[src_include_path])

        # Inject an import path so that idl files can import other idl files in the enterprise repo
        # Computes: src/mongo/db/modules/<enterprise_module_name>/src
        env.Append(IDLCFLAGS=["--include", str(env.Dir(root).Dir("src"))])

        if consumer:
            if "MONGO_ENTERPRISE_VERSION" in env:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_VERSION", 1)])
            if "audit" in env["MONGO_ENTERPRISE_FEATURES"]:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_AUDIT", 1)])
            if "encryptdb" in env["MONGO_ENTERPRISE_FEATURES"]:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_ENCRYPTDB", 1)])

        return env

    env["MODULE_INJECTORS"]["enterprise"] = injectEnterpriseModule

    distsrc = env.Dir(root).Dir("distsrc")
    docs = env.Dir(root).Dir("docs")
    env.Append(
        MODULE_BANNERS=[
            distsrc.File("LICENSE-Enterprise.txt"),
        ]
    )
    if env.TargetOSIs("windows"):
        env.Append(
            MODULE_BANNERS=[
                distsrc.File("THIRD-PARTY-NOTICES.windows"),
            ]
        )

    return configured_modules
