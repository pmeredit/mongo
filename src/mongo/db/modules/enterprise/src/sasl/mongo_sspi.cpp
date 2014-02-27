/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */


#define SECURITY_WIN32 1   // for sspi.h

#include "mongo/platform/basic.h"

#include "mongo_gssapi.h"

#include <boost/scoped_array.hpp>
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <security.h>
#include <sspi.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/text.h"

extern "C" int plain_server_plug_init(const sasl_utils_t *utils,
                                      int maxversion,
                                      int *out_version,
                                      sasl_server_plug_t **pluglist,
                                      int *plugcount);

extern "C" int crammd5_server_plug_init(const sasl_utils_t *utils,
                                        int maxversion,
                                        int *out_version,
                                        sasl_server_plug_t **pluglist,
                                        int *plugcount);


namespace mongo {
namespace gssapi {

    Status canonicalizeUserName(const StringData& name, std::string* canonicalName) {
        *canonicalName = name.toString();
        return Status::OK();
    }

    Status canonicalizeServerName(const StringData& name, std::string* canonicalName) {
        return Status(ErrorCodes::InternalError, "do not call canonicalizeServerName");
    }

    Status tryAcquireServerCredential(const std::string& principalName) {
        std::wstring utf16principalName = toWideString(principalName.c_str());
        CredHandle cred;
        TimeStamp ignored;
        SECURITY_STATUS status = 
            AcquireCredentialsHandleW(const_cast<wchar_t*>(utf16principalName.c_str()),
                                      L"kerberos",
                                      SECPKG_CRED_INBOUND,
                                      NULL,  // LOGON id
                                      NULL,  // auth data
                                      NULL,  // get key fn
                                      NULL,  // get key arg
                                      &cred,
                                      &ignored);
        if (status != SEC_E_OK) {
            char *err;
            if (!FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | 
                                FORMAT_MESSAGE_FROM_SYSTEM |
                                FORMAT_MESSAGE_IGNORE_INSERTS,
                                NULL,
                                status,
                                MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                (LPSTR) &err,
                                0,
                            NULL)) {
                return Status(ErrorCodes::UnknownError,
                              "sspi could not translate error message");
            }
            ON_BLOCK_EXIT(LocalFree, err);
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() <<
                          "sspi could not acquire server credential for " <<
                          principalName << "; " <<
                          err);
        }
        FreeCredentialsHandle(&cred);
        return Status::OK();
    }

}  // namespace gssapi

namespace {

    // The SSPI plugin implements the GSSAPI mechanism
    char sspiPluginName[] = "GSSAPI";

    // An instance of this structure stores state that is used between calls to various SASL 
    // plugin functions during an authentication attempt.
    struct SspiConnContext {
        SspiConnContext() :
            haveCred(false),
            haveCtxt(false),
            authComplete(false),
            supportComplete(false)
        {}
        ~SspiConnContext() {
            if (haveCtxt) {
                DeleteSecurityContext(&ctx);
            }
            if (haveCred) {
                FreeCredentialsHandle(&cred);
            }
        }

        CredHandle cred;
        bool haveCred;
        CtxtHandle ctx;
        bool haveCtxt;
        bool authComplete;
        bool supportComplete;
    };

    void HandleLastError(const sasl_utils_t* utils, DWORD errCode, const char* msg) {
        char *err;
        if (!FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | 
                            FORMAT_MESSAGE_FROM_SYSTEM |
                            FORMAT_MESSAGE_IGNORE_INSERTS,
                            NULL,
                            errCode,
                            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                            (LPSTR) &err,
                            0,
                            NULL)) {
            return;
        }

        std::string buffer(mongoutils::str::stream() << "SSPI: " << msg << ": " << err);
        utils->seterror(utils->conn, 0, "%s", buffer.c_str());
        LocalFree(err);
    }


    /*
     *  SSPI server plugin impl
     */

    int sspiServerMechNew(void *glob_context,
                          sasl_server_params_t *sparams,
                          const char *challenge,
                          unsigned challen,
                          void **conn_context) throw() {
        TimeStamp ignored;
        std::auto_ptr<SspiConnContext> pcctx(new SspiConnContext());

        // Compose principal name
        if (sparams->serverFQDN == NULL || strlen(sparams->serverFQDN) == 0) {
            sparams->utils->seterror(sparams->utils->conn, 0, "SSPI: no serverFQDN");
            return SASL_FAIL;
        }


        std::wstring principalName = toWideString(std::string(mongoutils::str::stream() << 
                                                              sparams->service << 
                                                              "/" << 
                                                              sparams->serverFQDN).c_str());
        LOG(2) << "SSPI principal name: " << toUtf8String(principalName);

        SECURITY_STATUS status = 
            AcquireCredentialsHandle(const_cast<wchar_t*>(principalName.c_str()),
                                     L"kerberos",
                                     SECPKG_CRED_INBOUND,
                                     NULL,  // LOGON id
                                     NULL,  // auth data
                                     NULL,  // get key fn
                                     NULL,  // get key arg
                                     &pcctx->cred,
                                     &ignored);
        if (status != SEC_E_OK) {
            HandleLastError(sparams->utils, status, "AcquireCredentialsHandle");
            return SASL_FAIL;
        }

        *conn_context = pcctx.release();
        return SASL_OK;
    }

    int sendSecuritySupport(SspiConnContext* pcctx,
                            sasl_server_params_t* sparams,
                            unsigned clientinlen,
                            const char **serverout,
                            unsigned *serveroutlen,
                            sasl_out_params_t* oparams) {
        SecPkgContext_Sizes sizes;
        SECURITY_STATUS status = QueryContextAttributes(&pcctx->ctx,
                                                        SECPKG_ATTR_SIZES,
                                                        &sizes);
        if (status != SEC_E_OK) {
            HandleLastError(sparams->utils, status, "QueryContextAttributes(sizes)");
            return SASL_FAIL;
        }

        // Check that the client gave us no data
        if (clientinlen != 0) {
            sparams->utils->seterror(sparams->utils->conn, 0, 
                                     "SSPI: client unexpectedly sent data");
            return SASL_FAIL;
        }

        // Encrypt a message with the security layer support
        int plaintextMessageSize = 4;  // per RFC4752
        boost::scoped_array<char> message(new char[sizes.cbSecurityTrailer +
                                                   plaintextMessageSize +
                                                   sizes.cbBlockSize]);
        char* plaintextMessage = message.get() + sizes.cbSecurityTrailer;
        plaintextMessage[0] = 1; // LAYER_NONE
        plaintextMessage[1] = 0;
        plaintextMessage[2] = 0;
        plaintextMessage[3] = 0;
            
        SecBuffer wrapBufs[3];
        SecBufferDesc wrapBufDesc;
        wrapBufDesc.cBuffers = 3;
        wrapBufDesc.pBuffers = wrapBufs;
        wrapBufDesc.ulVersion = SECBUFFER_VERSION;

        wrapBufs[0].cbBuffer = sizes.cbSecurityTrailer;
        wrapBufs[0].BufferType = SECBUFFER_TOKEN;
        wrapBufs[0].pvBuffer = message.get();

        wrapBufs[1].cbBuffer = plaintextMessageSize;
        wrapBufs[1].BufferType = SECBUFFER_DATA;
        wrapBufs[1].pvBuffer = message.get() + sizes.cbSecurityTrailer;
        
        wrapBufs[2].cbBuffer = sizes.cbBlockSize;
        wrapBufs[2].BufferType = SECBUFFER_PADDING;
        wrapBufs[2].pvBuffer = message.get() + sizes.cbSecurityTrailer + plaintextMessageSize;

        status = EncryptMessage(&pcctx->ctx,
                                SECQOP_WRAP_NO_ENCRYPT,
                                &wrapBufDesc,
                                0);

        if (status != SEC_E_OK) {
            HandleLastError(sparams->utils, status, "EncryptMessage");
            return SASL_FAIL;
        }

        // Prepare the data buffer to send to client.
        // Note that the cbBuffer values may have changed from their original values above;
        // hence, we copy each buffer into a new contiguous storage location.
        *serveroutlen = wrapBufs[0].cbBuffer + wrapBufs[1].cbBuffer + wrapBufs[2].cbBuffer;
        char* newoutbuf = static_cast<char*>(sparams->utils->malloc(*serveroutlen));

        memcpy(newoutbuf, 
               wrapBufs[0].pvBuffer, 
               wrapBufs[0].cbBuffer);
        memcpy(newoutbuf + wrapBufs[0].cbBuffer, 
               wrapBufs[1].pvBuffer, 
               wrapBufs[1].cbBuffer);
        memcpy(newoutbuf + wrapBufs[0].cbBuffer + wrapBufs[1].cbBuffer, 
               wrapBufs[2].pvBuffer, 
               wrapBufs[2].cbBuffer);

        *serverout = newoutbuf;
        pcctx->supportComplete = true;

        return SASL_CONTINUE;
    }

    int setAuthIdAndAuthzId(SspiConnContext* pcctx,
                            sasl_server_params_t* sparams,
                            const char* clientin,
                            unsigned int clientinlen,
                            sasl_out_params_t* oparams) {
        oparams->doneflag = 1;
        oparams->mech_ssf = 0;
        oparams->maxoutbuf = 0;
        oparams->encode_context = NULL;
        oparams->encode = NULL;
        oparams->decode_context = NULL;
        oparams->decode = NULL;
        oparams->param_version = 0;

        // Fetch the name that we authenticated out of the context
        SecPkgContext_NativeNamesW namesx;
        SECURITY_STATUS status = QueryContextAttributesW(&pcctx->ctx,
                                                         SECPKG_ATTR_NATIVE_NAMES,
                                                         &namesx);
        if (status != SEC_E_OK) {
            HandleLastError(sparams->utils, status, "QueryContextAttributes");
            return SASL_FAIL;
        }
        ON_BLOCK_EXIT(FreeContextBuffer, namesx.sClientName);
        ON_BLOCK_EXIT(FreeContextBuffer, namesx.sServerName);

        LOG(2) << "SSPI authenticated name: " << toUtf8String(namesx.sClientName);

        int ret = sparams->canon_user(sparams->utils->conn,
                                      toUtf8String(namesx.sClientName).c_str(), 
                                      0,
                                      SASL_CU_AUTHID,
                                      oparams);

        if (ret != SASL_OK) return ret;


        // Now fetch the authz id out of the message the client passed us, and
        // set it in the oparams.
        boost::scoped_array<char> message(new char[clientinlen]);
        memcpy(message.get(), clientin, clientinlen);

        SecBuffer wrapBufs[2];
        SecBufferDesc wrapBufDesc;
        wrapBufDesc.cBuffers = 2;
        wrapBufDesc.pBuffers = wrapBufs;
        wrapBufDesc.ulVersion = SECBUFFER_VERSION;

        wrapBufs[0].cbBuffer = clientinlen;
        wrapBufs[0].BufferType = SECBUFFER_STREAM;
        wrapBufs[0].pvBuffer = message.get();

        wrapBufs[1].cbBuffer = 0;
        wrapBufs[1].BufferType = SECBUFFER_DATA;
        wrapBufs[1].pvBuffer = NULL;

        ULONG pfQOP = 0;
        status = DecryptMessage(&pcctx->ctx,
                                &wrapBufDesc,
                                0,
                                &pfQOP);
        LOG(4) << "SSPI encrypted size: " << wrapBufs[0].cbBuffer << 
            " decrypted size: " << wrapBufs[1].cbBuffer << 
            " encrypted msg pointer: " << wrapBufs[0].pvBuffer <<
            " decrypted msg pointer: " << wrapBufs[1].pvBuffer;

        if (status != SEC_E_OK) {
            HandleLastError(sparams->utils, status, "DecryptMessage");
            return SASL_FAIL;
        }

        // Confirm that the client agrees to use no security layer
        const char* decryptedMessage = static_cast<char*>(wrapBufs[1].pvBuffer);
        int decryptedMessageSize = wrapBufs[1].cbBuffer;
        if (decryptedMessageSize < 4) {
            sparams->utils->seterror(sparams->utils->conn, 0, 
                                     "SSPI: no security layer request in auth handshake");
            return SASL_FAIL;
        }
            
        if (!(decryptedMessage[0]==1 &&
              decryptedMessage[1]==0 &&
              decryptedMessage[2]==0 &&
              decryptedMessage[3]==0)) {
            sparams->utils->seterror(sparams->utils->conn, 0, 
                                     "SSPI: wrong security layer from client");
            return SASL_FAIL;
        }

        // fetch authz id
        int authzLength = decryptedMessageSize - 4;
        if (authzLength <= 0) {
            sparams->utils->seterror(sparams->utils->conn, 0, 
                                     "SSPI: no authz name in auth handshake");
            return SASL_FAIL;
        }
        std::string authz(decryptedMessage + 4, authzLength);

        LOG(2) << "SSPI name provided by client: " << authz;

        ret = sparams->canon_user(sparams->utils->conn,
                                  authz.c_str(), 
                                  authzLength,
                                  SASL_CU_AUTHZID,
                                  oparams);
        return ret;
    }


    int sspiServerMechStep(void *conn_context,
                           sasl_server_params_t *sparams,
                           const char *clientin,
                           unsigned clientinlen,
                           const char **serverout,
                           unsigned *serveroutlen,
                           sasl_out_params_t *oparams) throw() {
        SspiConnContext* pcctx = static_cast<SspiConnContext*>(conn_context);
        *serverout = NULL;
        *serveroutlen = 0;
        
        // There are three phases of the GSSAPI mechanism here.  Each time we receive a response
        // from the client via the SASL glue code, sspiServerMechStep is called and we select a
        // phase based on our current state.  The first phase is repeated calling of
        // AcceptSecurityContext with the data provided by the client, while it returns CONTINUE.
        // When it returns OK, we set authComplete and transition to the next phase.  Note that this
        // first phase can take several exchanges between client and server before transitioning to
        // the next phase.  In the second phase, the server encrypts a message describing the
        // security layers it supports.  We then set supportComplete and move to the next phase.  In
        // the third phase, the server checks that the client replied with the proper message, and
        // confirms that the name produced by the server authentication matches the name the client
        // wanted to authenticate.  If everything succeeds up to this point, authentication is
        // deemed a success by the server.

        if (pcctx->supportComplete) {
            // Third phase
            return setAuthIdAndAuthzId(pcctx, sparams, clientin, clientinlen, oparams);
        }
        if (pcctx->authComplete) {
            // Second phase
            return sendSecuritySupport(pcctx, sparams, clientinlen,
                                       serverout, serveroutlen, oparams);
        }

        // First phase

        SecBufferDesc inbuf;
        SecBuffer inBufs[1];
        SecBufferDesc outbuf;
        SecBuffer outBufs[1];

        inbuf.ulVersion = SECBUFFER_VERSION;
        inbuf.cBuffers = 1;
        inbuf.pBuffers = inBufs;
        inBufs[0].pvBuffer = const_cast<char*>(clientin);
        inBufs[0].cbBuffer = clientinlen;
        inBufs[0].BufferType = SECBUFFER_TOKEN;

        outbuf.ulVersion = SECBUFFER_VERSION;
        outbuf.cBuffers = 1;
        outbuf.pBuffers = outBufs;
        outBufs[0].pvBuffer = NULL;
        outBufs[0].cbBuffer = 0;
        outBufs[0].BufferType = SECBUFFER_TOKEN;
        
        ULONG contextAttr = 0;
        SECURITY_STATUS status = AcceptSecurityContext(&pcctx->cred,
                                                       pcctx->haveCtxt ? &pcctx->ctx : NULL,
                                                       &inbuf,
                                                       ASC_REQ_ALLOCATE_MEMORY |
                                                       ASC_REQ_INTEGRITY |
                                                       ASC_REQ_MUTUAL_AUTH,
                                                       SECURITY_NETWORK_DREP,
                                                       &pcctx->ctx,
                                                       &outbuf,
                                                       &contextAttr,
                                                       NULL);
        if (status != SEC_E_OK && status != SEC_I_CONTINUE_NEEDED) {
            HandleLastError(sparams->utils, status, "AcceptSecurityContext");
            return SASL_FAIL;
        }

        // The first time AcceptSecurityContext() is called, it returns a Context.
        // We now pass this Context in as the second parameter, in successive calls
        // to AcceptSecurityContext().  This flag enables that logic.
        pcctx->haveCtxt = true;
        
        char *newoutbuf = static_cast<char*>(sparams->utils->malloc(outBufs[0].cbBuffer));
        *serveroutlen = outBufs[0].cbBuffer;
        memcpy(newoutbuf, outBufs[0].pvBuffer, *serveroutlen);
        *serverout = newoutbuf;
        FreeContextBuffer(outbuf.pBuffers[0].pvBuffer);

        if (status == SEC_I_CONTINUE_NEEDED) {
            // Not done with phase 1 yet
            return SASL_CONTINUE;
        }
        // We must have returned SEC_E_OK, so set flag to trigger next phase of MechStep
        pcctx->authComplete = true;
        return SASL_CONTINUE;

    }

    void sspiServerMechDispose(void *conn_context, const sasl_utils_t *utils) {
        SspiConnContext* pcctx = static_cast<SspiConnContext*>(conn_context);
        delete pcctx;
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
            sspiPluginName,                /* mechanism name */
            112,                           /* best mech additional security layer strength factor */
            SASL_SEC_NOPLAINTEXT
            | SASL_SEC_NOACTIVE
            | SASL_SEC_NOANONYMOUS
            | SASL_SEC_MUTUAL_AUTH
            | SASL_SEC_PASS_CREDENTIALS,
            SASL_FEAT_WANT_CLIENT_FIRST
            | SASL_FEAT_ALLOWS_PROXY
            | SASL_FEAT_DONTUSE_USERPASSWD,    /* features */
            NULL,                              /* global state for mechanism */
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

    /**
     * Registers the plugin at process initialization time.
     */
    MONGO_INITIALIZER_GENERAL(SaslSspiServerPlugin, 
                              ("CyrusSaslServerCore"), 
                              ("CyrusSaslAllPluginsRegistered"))
        (InitializerContext*) {
        int ret = sasl_server_add_plugin(sspiPluginName,
                                     sspiServerPluginInit);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << "Could not add SASL Server SSPI plugin "
                          << sspiPluginName << ": " << sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

    MONGO_INITIALIZER_GENERAL(SaslCramServerPlugin, 
                              ("CyrusSaslServerCore"),
                              ("CyrusSaslAllPluginsRegistered"))
        (InitializerContext*) {
        int ret = sasl_server_add_plugin("CRAMMD5",
                                     crammd5_server_plug_init);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << "Could not add SASL Server CRAM-MD5 plugin "
                          << sspiPluginName << ": " << sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

    MONGO_INITIALIZER_GENERAL(SaslPlainServerPlugin, 
                              ("CyrusSaslServerCore"),
                              ("CyrusSaslAllPluginsRegistered"))
        (InitializerContext*) {
        int ret = sasl_server_add_plugin("PLAIN",
                                     plain_server_plug_init);
        if (SASL_OK != ret) {
            return Status(ErrorCodes::UnknownError,
                          mongoutils::str::stream() << "Could not add SASL Server PLAIN plugin "
                          << sspiPluginName << ": " << sasl_errstring(ret, NULL, NULL));
        }

        return Status::OK();
    }

        

} // namespace
} // namespace mongo
