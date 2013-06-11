/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */


#include "mongo_gssapi.h"

#include <sasl/sasl.h>
#include <sasl/saslplug.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace gssapi {

    Status canonicalizeUserName(const StringData& name, std::string* canonicalName) {
        *canonicalName = name.toString();
        return Status::OK();
    }

    Status canonicalizeServerName(const StringData& name, std::string* canonicalName) {
        return Status(ErrorCodes::InternalError, "do not call canonicalizeServerName");
    }

    Status tryAcquireServerCredential(const StringData& principalName) {
        return Status::OK();
    }

}  // namespace gssapi

namespace {

    // The SSPI plugin implements the GSSAPI interface
    char sspiPluginName[] = "GSSAPI";

    /*
     *  SSPI server plugin impl
    */

    int sspiServerMechNew(void *glob_context,
                          sasl_server_params_t *sparams,
                          const char *challenge,
                          unsigned challen,
                          void **conn_context) {
        return SASL_WRONGMECH;
    }

    int sspiServerMechStep(void *conn_context,
                           sasl_server_params_t *sparams,
                           const char *clientin,
                           unsigned clientinlen,
                           const char **serverout,
                           unsigned *serveroutlen,
                           sasl_out_params_t *oparams) {
        return SASL_WRONGMECH;
    }

    void sspiServerMechDispose(void *conn_context, const sasl_utils_t *utils) {
    }

    void sspiServerMechFree(void *glob_context, const sasl_utils_t *utils) {
    }

    int sspiServerMechAvail(void *glob_context,
                            sasl_server_params_t *sparams,
                            void **conn_context) {
        return SASL_OK;
    }

    sasl_server_plug_t sspiServerPlugin[] = {
        {
            sspiPluginName,         /* mechanism name */
            0,                  /* best mech additional security layer strength factor */
            SASL_SEC_NOPLAINTEXT
            | SASL_SEC_NOACTIVE
            | SASL_SEC_NOANONYMOUS
            | SASL_SEC_MUTUAL_AUTH, /* best security flags */
            0,                      /* features of plugin */
            NULL,                   /* global state for mechanism */
            sspiServerMechNew,
            sspiServerMechStep,
            sspiServerMechDispose,
            sspiServerMechFree,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        }
    };

    int sspiServerPluginInit(const sasl_utils_t *utils,
                             int maxversion,
                             int *out_version,
                             sasl_server_plug_t **pluglist,
                             int *plugcount) {
        if (maxversion < SASL_SERVER_PLUG_VERSION) {
            return SASL_BADVERS;
        }

        *out_version = SASL_SERVER_PLUG_VERSION;
        *pluglist = sspiServerPlugin;
        *plugcount = 1;

        return SASL_OK;
    }

    /*
     * SSPI client plugin impl
    */

    int sspiClientMechNew(void *glob_context,
                          sasl_client_params_t *cparams,
                          void **conn_context) {
        return SASL_WRONGMECH;
    }

    int sspiClientMechStep(void *conn_context,
                           sasl_client_params_t *cparams,
                           const char *serverin,
                           unsigned serverinlen,
                           sasl_interact_t **prompt_need,
                           const char **clientout,
                           unsigned *clientoutlen,
                           sasl_out_params_t *oparams) {
        return SASL_BADSERV;
    }

    void sspiClientMechDispose(void *conn_context, const sasl_utils_t *utils) {
    }

    void sspiClientMechFree(void *glob_context, const sasl_utils_t *utils) {
    }

    sasl_client_plug_t sspiClientPlugin[] = {
        {
            sspiPluginName, /* mechanism name */
            0,      /* best mech additional security layer strength factor */
            SASL_SEC_NOPLAINTEXT /* eam: copied from kerberos_v4 */
            | SASL_SEC_NOACTIVE
            | SASL_SEC_NOANONYMOUS
            | SASL_SEC_MUTUAL_AUTH, /* best security flags */
            SASL_FEAT_SERVER_FIRST, /* features of plugin */
            NULL,   /* required prompt ids, NULL = user/pass only */
            NULL,   /* global state for mechanism */
            sspiClientMechNew,
            sspiClientMechStep,
            sspiClientMechDispose,
            sspiClientMechFree,
            NULL,
            NULL,
            NULL
        }
    };

    int sspiClientPluginInit(const sasl_utils_t *utils,
                             int max_version,
                             int *out_version,
                             sasl_client_plug_t **pluglist,
                             int *plugcount) {
        if (max_version < SASL_CLIENT_PLUG_VERSION) {
            utils->seterror(utils->conn, 0, "Wrong SSPI version");
            return SASL_BADVERS;
        }

        *out_version = SASL_CLIENT_PLUG_VERSION;
        *pluglist = sspiClientPlugin;
        *plugcount = 1;

        return SASL_OK;
    }

    /**
     * Registers the plugin at process initialization time.
     */
    MONGO_INITIALIZER_WITH_PREREQUISITES(SaslSspiPlugin, ("CyrusSaslServerLibrary"))
        (InitializerContext*) {
        int ret = sasl_client_add_plugin(sspiPluginName,
                                         sspiClientPluginInit);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << "Could not add SASL Client SSPI plugin "
                          << sspiPluginName << ": " << sasl_errstring(ret, NULL, NULL));
        }

        ret = sasl_server_add_plugin(sspiPluginName,
                                     sspiServerPluginInit);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << "Could not add SASL Server SSPI plugin "
                          << sspiPluginName << ": " << sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

} // namespace
} // namespace mongo
