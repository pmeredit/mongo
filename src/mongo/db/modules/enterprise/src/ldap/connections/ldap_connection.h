/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/db/auth/user_acquisition_stats.h"

#include "../ldap_connection_options.h"
#include "../ldap_type_aliases.h"

namespace mongo {

struct LDAPBindOptions;
struct LDAPConnectionOptions;
class LDAPQuery;

class Status;
template <typename T>
class StatusWith;
class StringData;

/**
 * Represents a connection to an LDAP server.
 * This is a base class, whose children may consume libraries to implement in a platform
 * specific manner.
 *
 * Invariant: Must never be accessed by more than one thread at a time.
 */
class LDAPConnection {
public:
    explicit LDAPConnection(LDAPConnectionOptions options)
        : _connectionOptions(std::move(options)) {}
    virtual ~LDAPConnection() = default;

    /**
     * Connect to an LDAP server.
     *
     *   @param hostURI The connection string, of format 'ldap(s)://<host>(:<port)'
     *   @return Any errors arising from the connection attempt
     */
    virtual Status connect() = 0;

    /**
     * Attempt to bind as a user.
     *
     *   @param options All information needed to bind
     *   @return Any errors arising from the bind attempt
     */
    virtual Status bindAsUser(const LDAPBindOptions& options,
                              TickSource* tickSource,
                              UserAcquisitionStats* userAcquisitionStats) = 0;

    virtual boost::optional<std::string> currentBoundUser() const = 0;

    /**
     * Perform a query against the LDAP database.
     *
     *   @param query All parameters defining the query
     *   @param results A map of all results returned. Consists of a map from the DN of each
     *                  returned entity to a map of its attribute key-value pairs
     */
    virtual StatusWith<LDAPEntityCollection> query(LDAPQuery query,
                                                   TickSource* tickSource,
                                                   UserAcquisitionStats* userAcquisitionStats) = 0;

    /**
     * Validate that the remote LDAP server is alive and answering our requests.
     */
    virtual Status checkLiveness(TickSource* tickSource,
                                 UserAcquisitionStats* userAcquisitionStats) = 0;

    /**
     * Disconnect from the database.
     *
     *  @return Any errors arising from disconnecting.
     */
    virtual Status disconnect(TickSource* tickSource,
                              UserAcquisitionStats* userAcquisitionStats) = 0;

protected:
    LDAPConnectionOptions _connectionOptions;
    // Used to keep bind options in scope regardless of delays.
    boost::optional<LDAPBindOptions> _bindOptions;
};

}  // namespace mongo
