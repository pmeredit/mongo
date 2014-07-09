/*
 * Copyright (C) 2014 10gen, Inc.  All Rights Reserved.
 */

#include "native_sasl_authentication_session.h"

#include <boost/range/size.hpp>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo_gssapi.h"
#include "sasl_options.h"

namespace mongo {
namespace {
    SaslAuthenticationSession* createNativeSaslAuthenticationSession(
        AuthorizationSession* authzSession) {
        return new NativeSaslAuthenticationSession(authzSession);
    }

    MONGO_INITIALIZER(NativeSaslServerCore)(InitializerContext* context) {
        SaslAuthenticationSession::create = createNativeSaslAuthenticationSession;
        return Status::OK();
    }
} //namespace
    
    NativeSaslAuthenticationSession::NativeSaslAuthenticationSession(
        AuthorizationSession* authzSession) :
        SaslAuthenticationSession(authzSession) {
    }

    NativeSaslAuthenticationSession::~NativeSaslAuthenticationSession() {}

    Status NativeSaslAuthenticationSession::start(const StringData& authenticationDatabase,
                                                 const StringData& mechanism,
                                                 const StringData& serviceName,
                                                 const StringData& serviceHostname,
                                                 int64_t conversationId,
                                                 bool autoAuthorize) {
        /*fassert(18626, conversationId > 0);

        if (_conversationId != 0) {
            return Status(ErrorCodes::AlreadyInitialized,
                          "Cannot call start() twice on same NativeSaslAuthenticationSession.");
        }

        _authenticationDatabase = authenticationDatabase.toString();
        _serviceName = serviceName.toString();
        _serviceHostname = serviceHostname.toString();
        _conversationId = conversationId;
        _autoAuthorize = autoAuthorize;
        _mechInfo = _findMechanismInfo(mechanism);

        if (NULL == _mechInfo) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "Unsupported mechanism " << mechanism);
        }*/

        return Status(ErrorCodes::BadValue,
                      mongoutils::str::stream() << "Native SASL start() is not implemented yet");
    }

    Status NativeSaslAuthenticationSession::step(const StringData& inputData, 
                                                 std::string* outputData) {
        /*const char* output;
        unsigned outputLen;
        const char* const input = inputData.empty() ? NULL : inputData.rawData();
        const unsigned inputLen = static_cast<unsigned>(inputData.size());*/
        
        return Status(ErrorCodes::BadValue,
                      mongoutils::str::stream() << "Native SASL step() is not implemented yet");
    }

    std::string NativeSaslAuthenticationSession::getPrincipalId() const {
        return "";
    }

    const char* NativeSaslAuthenticationSession::getMechanism() const {
        return "";
    }

}  // namespace mongo
