/*
 * Copyright (C) 2018 MongoDB Inc.  All Rights Reserved.
 */

#pragma once

#include <sasl/sasl.h>

#include <string>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"

namespace mongo {

template <typename Policy>
struct CyrusSaslMechShim : MakeServerMechanism<Policy> {
    explicit CyrusSaslMechShim(std::string authenticationDatabase);

    virtual ~CyrusSaslMechShim() {
        if (_saslConnection) {
            sasl_dispose(&_saslConnection);
        }
    }


    StringData getPrincipalName() const final;

    StatusWith<std::tuple<bool, std::string>> stepImpl(OperationContext* opCtx,
                                                       StringData input) final;

private:
    size_t _saslStep = 0;
    static constexpr int maxCallbacks = 4;
    sasl_conn_t* _saslConnection;
    sasl_callback_t _callbacks[maxCallbacks];
};

// Explicitly instantiated in cyrus_sasl_authentication_session.cpp
extern template struct CyrusSaslMechShim<PLAINPolicy>;
extern template struct CyrusSaslMechShim<GSSAPIPolicy>;

using CyrusPLAINServerMechanism = CyrusSaslMechShim<PLAINPolicy>;

struct CyrusPlainServerFactory : MakeServerFactory<CyrusPLAINServerMechanism> {
    static constexpr bool isInternal = false;
    bool canMakeMechanismForUser(const User* user) const final {
        auto credentials = user->getCredentials();
        return credentials.isExternal;
    }
};
extern CyrusPlainServerFactory cyrusPlainServerFactory;

struct CyrusGSSAPIServerMechanism : public CyrusSaslMechShim<GSSAPIPolicy> {
    explicit CyrusGSSAPIServerMechanism(std::string authenticationDatabase)
        : CyrusSaslMechShim(std::move(authenticationDatabase)) {}

    /**
     * GSSAPI-specific method for determining if "authenticatedUser" may act as "requestedUser."
     *
     * The GSSAPI mechanism in Cyrus SASL strips the kerberos realm from the authenticated user
     * name, if it matches the server realm.  So, for GSSAPI authentication, we must re-canonicalize
     * the authenticated user name before validating it..
     */
    bool isAuthorizedToActAs(StringData requestedUser, StringData authenticatedUser) final;
};

struct CyrusGSSAPIServerFactory : MakeServerFactory<CyrusGSSAPIServerMechanism> {
    static constexpr bool isInternal = false;
    bool canMakeMechanismForUser(const User* user) const final {
        auto credentials = user->getCredentials();
        return credentials.isExternal;
    }
};

}  // namespace mongo
