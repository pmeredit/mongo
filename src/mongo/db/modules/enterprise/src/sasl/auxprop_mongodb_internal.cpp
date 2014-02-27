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

#include <cstring>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/mongoutils/str.h"
#include "sasl_authentication_session.h"

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
                                     unsigned ulen) throw () {

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
        int ret = sparams->utils->getcallback(sparams->utils->conn,
                                              SaslAuthenticationSession::mongoSessionCallbackId,
                                              &ignored,
                                              &sessionContext);
        if (ret != SASL_OK)
            return SASL_FAIL;
        SaslAuthenticationSession* session = static_cast<SaslAuthenticationSession*>(
                sessionContext);

        User* userObj;
        // NOTE: since this module is only used for looking up authentication information, the
        // authentication database is also the source database for the user.
        Status status = session->getAuthorizationSession()->getAuthorizationManager().
                acquireUser(
                        UserName(StringData(user, ulen), session->getAuthenticationDatabase()),
                        &userObj);

        std::string userPassword;
        if (!status.isOK()) {
            sparams->utils->log(sparams->utils->conn,
                                SASL_LOG_DEBUG,
                                "auxpropMongoDBInternal failed to find privilege document: %s",
                                status.toString().c_str());
            userPassword = "";
        } else {
            userPassword = userObj->getCredentials().password;
            session->getAuthorizationSession()->getAuthorizationManager().releaseUser(userObj);
        }

        // Iterate over the properties to fetch, and set the ones we know how to set.
        const propval* to_fetch = sparams->utils->prop_get(sparams->propctx);
        if (!to_fetch)
            return SASL_NOMEM;

        // SASL_CONTINUE is a code never set elsewhere in the loop, so we can detect the first
        // iteration when setting the return code.
        ret = SASL_CONTINUE;
        for (const propval* cur = to_fetch; cur->name; ++cur) {
            sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG, "auxprop lookup %s flags %d",
                                cur->name, flags);
            StringData propName = cur->name;

            // If this is an authz lookup, we want the properties without leading "*", otherwise, we
            // want the ones with leading "*".
            if (isAuthzLookup) {
                if (propName[0] == '*')
                    continue;
            }
            else {
                if (propName[0] != '*')
                    continue;
                propName = propName.substr(1);
            }

            if (cur->values) {
                if (isOverrideLookup || (verifyAgainstHashedPassword &&
                                         (propName == SASL_AUX_PASSWORD_PROP))) {

                    sparams->utils->prop_erase(sparams->propctx, cur->name);
                }
                else {
                    continue;  // Not overriding, value already present, so continue.
                }
            }

            int curRet;
            if (propName == SASL_AUX_PASSWORD_PROP) {
                if (userPassword.empty()) {
                    curRet = SASL_NOUSER;
                }
                sparams->utils->prop_set(sparams->propctx,
                                         cur->name,
                                         userPassword.c_str(),
                                         userPassword.size());
                curRet = SASL_OK;
            }
            else {
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

            // If anything went wrong other than "item not found", proceed to the next property.
            if (curRet != SASL_OK && curRet != SASL_NOUSER) {
                break;
            }
        }

        if (ret == SASL_CONTINUE) {
            sparams->utils->log(sparams->utils->conn,
                                SASL_LOG_DEBUG,
                                "auxprop matched no properties");
            ret = SASL_OK;
        }
        else if (ret == SASL_NOUSER && isAuthzLookup)
            ret = SASL_OK;  // Cannot return NOUSER for authz lookups

        return ret;
    }

#if SASL_AUXPROP_PLUG_VERSION >= 8
#define MONGODB_AUXPROP_LOOKUP_FN auxpropLookupMongoDBInternal
#else
    void auxpropLookupMongoDBInternalVoid(void* glob_context,
                                          sasl_server_params_t* sparams,
                                          unsigned flags,
                                          const char* user,
                                          unsigned ulen) {
        auxpropLookupMongoDBInternal(glob_context,
                                     sparams,
                                     flags,
                                     user,
                                     ulen);
    }

#define MONGODB_AUXPROP_LOOKUP_FN auxpropLookupMongoDBInternalVoid
#endif

    /// Plugin vtable.
    sasl_auxprop_plug_t auxpropMongoDBInternal = {
        0,                             // features MBZ
        0,                             // spare_int1 MBZ
        NULL,                          // glob_context
        NULL,                          // auxprop_free
        MONGODB_AUXPROP_LOOKUP_FN,     // auxprop_lookup
        NULL,                          // name
        NULL,                          // auxprop_store
    };

    /**
     * Entry point for initializing the plugin.
     */
    int auxpropMongoDBInternalPluginInit(
            const sasl_utils_t* utils,
            int max_version,
            int* out_version,
            sasl_auxprop_plug_t** plug,
            const char* plugname) throw () {

        if (!utils)
            return SASL_BADPARAM;

        if (max_version < SASL_AUXPROP_PLUG_VERSION)
            return SASL_BADVERS;

        *out_version = SASL_AUXPROP_PLUG_VERSION;
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

        auxpropMongoDBInternal.name = auxpropMongoDBInternalPluginName;
        int ret = sasl_auxprop_add_plugin(auxpropMongoDBInternal.name,
                                          auxpropMongoDBInternalPluginInit);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() <<
                          "Could not add sasl auxprop plugin " <<
                          auxpropMongoDBInternal.name << ": " <<
                          sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

}  // namespace
}  // namespace mongo
