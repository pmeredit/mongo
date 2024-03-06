/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

/**
 * This module implements the Cyrus SASL auxprop plugin interface for looking up properties of
 * users.  The implementation accesses MongoDB user privilege documents for the current cluster, and
 * is meant for use inside of Mongo server nodes, only.
 *
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
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

/// Name of the plugin.
char auxpropMongoDBInternalPluginName[] = "MongoDBInternalAuxprop";

/**
 * Implementation of sasl_auxprop_plug_t::auxprop_lookup method.
 *
 * Only supported property at present is the user's password data.
 *
 * Procedure is:
 * 1.) Finds out the target database.
 * 2.) Looks up the user's privilege document in that database via the AuthorizationManager.
 * 3.) For each property requested (in sparams->propctx)
 * 3a.) Skip if it is not in the class of properties we've been asked to answer for (auth/authz)
 * 3b.) Skip if it's been set already and we've been told not to override it.
 * 3c.) Skip if it's not a property we know how to set.
 * 3d.) Set the property if we haven't skipped it.
 */
int auxpropLookupMongoDBInternal(void* glob_context,
                                 sasl_server_params_t* sparams,
                                 unsigned flags,
                                 const char* user,
                                 unsigned ulen) throw() {
    try {
        if (!sparams || !sparams->utils)
            return SASL_BADPARAM;

        // Interpret the flags.
        const bool isAuthzLookup = flags & SASL_AUXPROP_AUTHZID;
        const bool isOverrideLookup = flags & SASL_AUXPROP_OVERRIDE;

#ifdef SASL_AUXPROP_VERIFY_AGAINST_HASH
        const bool verifyAgainstHashedPassword = flags & SASL_AUXPROP_VERIFY_AGAINST_HASH;
#else
        const bool verifyAgainstHashedPassword = false;
#endif

        // Look up the user's privilege document in the authentication database.
        BSONObj privilegeDocument;
        void* sessionContext;
        int (*ignored)();
        int ret =
            sparams->utils->getcallback(sparams->utils->conn,
                                        CyrusSaslAuthenticationSession::mongoSessionCallbackId,
                                        &ignored,
                                        &sessionContext);
        if (ret != SASL_OK)
            return SASL_FAIL;
        CyrusSaslAuthenticationSession* session =
            static_cast<CyrusSaslAuthenticationSession*>(sessionContext);

        User* userObj;
        // NOTE: since this module is only used for looking up authentication information, the
        // authentication database is also the source database for the user.
        UserName userName(StringData(user, ulen), session->getAuthenticationDatabase());
        Status status = session->getAuthorizationSession()->getAuthorizationManager().acquireUser(
            session->getOpCtxt(), userName, &userObj);

        if (!status.isOK()) {
            sparams->utils->log(sparams->utils->conn,
                                SASL_LOG_DEBUG,
                                "auxpropMongoDBInternal failed to find privilege document: %s",
                                status.toString().c_str());
            if (isAuthzLookup) {
                return SASL_OK;  // Cannot return NOUSER for authz lookups
            }
            return SASL_NOUSER;
        }

        const User::CredentialData creds = userObj->getCredentials();
        session->getAuthorizationSession()->getAuthorizationManager().releaseUser(userObj);

        // Iterate over the properties to fetch, and set the ones we know how to set.
        const propval* to_fetch = sparams->utils->prop_get(sparams->propctx);
        if (!to_fetch)
            return SASL_NOMEM;

        // SASL_CONTINUE is a code never set elsewhere in the loop, so we can detect the first
        // iteration when setting the return code.
        ret = SASL_CONTINUE;
        for (const propval* cur = to_fetch; cur->name; ++cur) {
            sparams->utils->log(sparams->utils->conn,
                                SASL_LOG_DEBUG,
                                "auxprop lookup %s flags %d",
                                cur->name,
                                flags);
            StringData propName = cur->name;

            // If this is an authz lookup, we want the properties without leading "*", otherwise, we
            // want the ones with leading "*".
            if (isAuthzLookup) {
                if (propName[0] == '*')
                    continue;
            } else {
                if (propName[0] != '*')
                    continue;
                propName = propName.substr(1);
            }

            if (cur->values) {
                if (isOverrideLookup ||
                    (verifyAgainstHashedPassword && (propName == SASL_AUX_PASSWORD_PROP))) {
                    sparams->utils->prop_erase(sparams->propctx, cur->name);
                } else {
                    continue;  // Not overriding, value already present, so continue.
                }
            }

            int curRet;
            std::string authMech = session->getMechanism();

            if (propName == SASL_AUX_PASSWORD_PROP &&
                (authMech == "CRAM-MD5" || authMech == "PLAIN")) {
                std::string userPassword = creds.password;
                if (userPassword.empty()) {
                    std::stringstream ss;
                    ss << "Failed to acquire authentication credentials for " << userName.toString()
                       << " using the " << authMech << " authentication mechanism";
                    std::string authenticationError = ss.str();
                    sparams->utils->log(
                        sparams->utils->conn, SASL_LOG_ERR, authenticationError.c_str());
                    return SASL_BADAUTH;
                }
                sparams->utils->prop_set(
                    sparams->propctx, cur->name, userPassword.c_str(), userPassword.size());
                curRet = SASL_OK;
            } else if (propName == "authPassword" && authMech == "SCRAM-SHA-1") {
                /* Create a SCRAM authPassword on the form:
                 * authPassword:= sasl-mech $ iter-count : salt $ storedKey : serverKey
                 * This form was chosen for the SCRAM Cyrus SASL plugin to conform with
                 * the LDAP authPassword property, see RFC 5803
                 */

                std::stringstream ss;
                ss << "SCRAM-SHA-1"
                   << "$";

                ss << creds.scram.iterationCount << ":";
                ss << creds.scram.salt << "$";
                ss << creds.scram.storedKey << ":";
                ss << creds.scram.serverKey;

                std::string authPassword = ss.str();
                sparams->utils->prop_set(
                    sparams->propctx, cur->name, authPassword.c_str(), authPassword.size());
                curRet = SASL_OK;
            } else {
                curRet = SASL_NOUSER;
            }

            // The rule for setting the final return code is that every return code overrides
            // SASL_CONTINUE and SASL_NOUSER, and anything other than SASL_NOUSER overrides SASL_OK.
            // This has the effect of setting SASL_OK if _any_ property can be set unless _any_
            // attempt to set a property returns an error other than SASL_NOUSER (item not found).
            if (ret == SASL_CONTINUE || ret == SASL_NOUSER) {
                ret = curRet;
            } else if (ret == SASL_OK) {
                if (curRet != SASL_NOUSER) {
                    ret = curRet;
                }
            }
        }

        if (ret == SASL_CONTINUE) {
            sparams->utils->log(
                sparams->utils->conn, SASL_LOG_DEBUG, "auxprop matched no properties");
            ret = SASL_OK;
        } else if (ret == SASL_NOUSER && isAuthzLookup)
            ret = SASL_OK;  // Cannot return NOUSER for authz lookups

        return ret;
    } catch (...) {
        StringBuilder sb;
        sb << "Unexpected exception in auxpropLookupMongoDBInternal: "
           << exceptionToStatus().reason();
        sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR, sb.str().c_str());
        return SASL_FAIL;
    }
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
                                      unsigned ulen) throw() {
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
                                     const char* plugname) throw() {
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
#if defined(__APPLE__) && (SASL_AUXPROP_PLUG_MIN_VERSION < SASL_AUXPROP_PLUG_VERSION)
    auxpropMongoDBInternal.auxprop_lookup_v4 = auxpropLookupMongoDBInternalVoid;
#elif (SASL_AUXPROP_PLUG_MIN_VERSION < 8)
    auxpropMongoDBInternal.auxprop_lookup = auxpropLookupMongoDBInternalVoid;
#else
    auxpropMongoDBInternal.auxprop_lookup = auxpropLookupMongoDBInternal;
#endif

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
