/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

/**
 * See sasl/saslplug.h and the Cyrus SASL documentation for information about auxprop plugins and
 * their use.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include <cstring>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>

#include "cyrus_sasl_authentication_session.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"

#ifndef _WIN32
#error Windows only sasl auxprop plugin
#endif

namespace mongo {
namespace {

/// Name of the plugin.
char auxpropMongoDBInternalPluginName[] = "MongoDBInternalAuxprop";

/**
 * Implementation of sasl_auxprop_plug_t::auxprop_lookup method.
 *
 * It just returns OK so that there exists an auxprop plugin. This is important on Windows
 * since our Windows sasl builds lack the default sasldb auxprop plugin.
 */
int auxpropLookupMongoDBInternal(void* glob_context,
                                 sasl_server_params_t* sparams,
                                 unsigned flags,
                                 const char* user,
                                 unsigned ulen) noexcept {
    if (!sparams || !sparams->utils)
        return SASL_BADPARAM;

    return SASL_OK;
}

// On OS X, the saslplug.h header defines this to indicate the minimum
// acceptable plugin version. We want to respect that, but other
// systems may not define it. Fall back to defining it in terms of the
// basic PLUG_VERSION if it isn't defined.
#ifndef SASL_AUXPROP_PLUG_MIN_VERSION
#define SASL_AUXPROP_PLUG_MIN_VERSION SASL_AUXPROP_PLUG_VERSION
#endif

void auxpropLookupMongoDBInternalVoid(void* glob_context,
                                      sasl_server_params_t* sparams,
                                      unsigned flags,
                                      const char* user,
                                      unsigned ulen) noexcept {
    auxpropLookupMongoDBInternal(glob_context, sparams, flags, user, ulen);
}

/// Plugin vtable.
sasl_auxprop_plug_t auxpropMongoDBInternal = {};

/**
 * Entry point for initializing the plugin.
 */
int auxpropMongoDBInternalPluginInit(const sasl_utils_t* utils,
                                     int max_version,
                                     int* out_version,
                                     sasl_auxprop_plug_t** plug,
                                     const char* plugname) noexcept {
    if (!utils)
        return SASL_BADPARAM;

    if (max_version < SASL_AUXPROP_PLUG_MIN_VERSION)
        return SASL_BADVERS;

    *out_version = SASL_AUXPROP_PLUG_MIN_VERSION;
    *plug = &auxpropMongoDBInternal;
    return SASL_OK;
}

/**
 * Registers the plugin at process initialization time.
 */
MONGO_INITIALIZER_GENERAL(SaslAuxpropMongodbInternal,
                          ("CyrusSaslServerCore"),
                          ("CyrusSaslAllPluginsRegistered"))
(InitializerContext*) {
    auxpropMongoDBInternal.auxprop_lookup = auxpropLookupMongoDBInternal;

    auxpropMongoDBInternal.name = auxpropMongoDBInternalPluginName;
    int ret =
        sasl_auxprop_add_plugin(auxpropMongoDBInternal.name, auxpropMongoDBInternalPluginInit);
    if (SASL_OK != ret) {
        return Status(ErrorCodes::UnknownError,
                      mongoutils::str::stream() << "Could not add sasl auxprop plugin "
                                                << auxpropMongoDBInternal.name
                                                << ": "
                                                << sasl_errstring(ret, NULL, NULL));
    }

    return Status::OK();
}

}  // namespace
}  // namespace mongo
