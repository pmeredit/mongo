/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

/**
 * This module implements the Cyrus SASL canon plugin interface for canonicalizing user names on the
 * server.  This canonicalization occurs as part of SASL protocols when the SASL library wishes to
 * transform the client-supplied user name data into the canonical name for that user.
 *
 * The canonicalization rule is quite simple in MongoDB -- strip whitespace from either end, but do
 * _nothing_ else.  This differs from the default canonicalization rule in Cyrus SASL, which appends
 * "@" and the user realm (db) to user names when canonicalizing.
 *
 * See sasl/saslplug.h and the Cyrus SASL documentation for information about canon plugins and
 * their use.
 */

#include <cstring>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>

#include "mongo/base/init.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

    char canonMongoDBInternalPluginName[] = "MongoDBInternalCanon";

    /**
     * In MongoDB, canonicalized user names do not include the database realm (db).
     */
    int canonUserServer(void* glob_context,
                        sasl_server_params_t* sparams,
                        const char* userRaw,
                        unsigned userRawLen,
                        unsigned flags,
                        char* out,
                        unsigned outMax,
                        unsigned* outLen) throw () {

        if (!sparams || !sparams->utils || !userRaw || !out || !outLen)
            return SASL_BADPARAM;

        const sasl_utils_t* utils = sparams->utils;

        StringData user(userRaw, userRawLen ? userRawLen : strlen(userRaw));
        size_t firstNonWhitespace = 0;
        for (; firstNonWhitespace < user.size(); ++firstNonWhitespace) {
            if (!isspace(static_cast<unsigned char>(user[firstNonWhitespace])))
                break;
        }
        size_t lastWhitespaceOrEnd = user.size();
        for (; lastWhitespaceOrEnd > firstNonWhitespace; --lastWhitespaceOrEnd) {
            if (!isspace(static_cast<unsigned char>(user[lastWhitespaceOrEnd - 1])))
                break;
        }

        if (firstNonWhitespace == lastWhitespaceOrEnd) {
            utils->seterror(utils->conn, 0, "All-whitespace username.");
            return SASL_FAIL;
        }

        user = user.substr(firstNonWhitespace, lastWhitespaceOrEnd);
        if (user.size() > outMax) {
            utils->seterror(utils->conn, 0, "Canonicalized username too long.");
            return SASL_FAIL;
        }

        memmove(out, user.rawData(), user.size());
        *outLen = static_cast<unsigned>(user.size());
        return SASL_OK;
    }

    /// Plugin vtable.
    sasl_canonuser_plug_t canonMongoDBInternal = {
        0,                // features MBZ
        0,                // spare_int1 MBZ
        NULL,             // glob_context
        NULL,             // name
        NULL,             // canon_user_free
        canonUserServer,  // canon_user_server
        NULL,             // canon_user_client
        NULL,             // spare_fptr1
        NULL,             // spare_fptr2
        NULL,             // spare_fptr3
    };

    /**
     * Entry point for initializing the plugin.
     */
    int canonMongoDBInternalPluginInit(const sasl_utils_t* utils,
                                       int max_version,
                                       int* out_version,
                                       sasl_canonuser_plug_t** plug,
                                       const char* plugname) throw () {
        if (!utils)
            return SASL_BADPARAM;

        if (max_version < SASL_CANONUSER_PLUG_VERSION)
            return SASL_BADVERS;

        *out_version = SASL_CANONUSER_PLUG_VERSION;
        *plug = &canonMongoDBInternal;
        return SASL_OK;
    }

    /**
     * Registers the plugin at process initialization time.
     */
    MONGO_INITIALIZER_GENERAL(SaslCanonMongodbInternal, 
                              ("CyrusSaslServerCore"),
                              ("CyrusSaslAllPluginsRegistered"))
        (InitializerContext*) {

        canonMongoDBInternal.name = canonMongoDBInternalPluginName;
        int ret = sasl_canonuser_add_plugin(canonMongoDBInternal.name,
                                            canonMongoDBInternalPluginInit);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() <<
                          "Could not add sasl canonuser plugin " <<
                          canonMongoDBInternal.name << ": " <<
                          sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

}  // namespace
}  // namespace mongo
