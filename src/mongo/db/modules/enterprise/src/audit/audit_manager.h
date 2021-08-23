/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "audit/audit_config_gen.h"
#include "audit/audit_header_options_gen.h"
#include "audit_enc_comp_manager.h"
#include "audit_format.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {
class Client;

namespace optionenvironment {
class Environment;
}  // namespace optionenvironment

namespace audit {

/**
 * Contains server-wide auditing configuration.
 */
class AuditManager {
public:
    AuditManager();

    bool isEnabled() const {
        return _enabled;
    }

    /**
     * Called by MONGO_INITIALIZER or TestFixture to setup
     * AuditManager from system configuration file.
     */
    void initialize(const optionenvironment::Environment&);

    AuditFormat getFormat() const {
        return _format;
    }

    bool getRuntimeConfiguration() const {
        return _runtimeConfiguration;
    }

    bool getCompressionEnabled() const {
        return _compressionEnabled;
    }

    const std::string& getPath() const {
        return _path;
    }

    bool getAuditAuthorizationSuccess() const {
        return getConfig()->auditAuthorizationSuccess.load();
    }

    const AuditEncryptionCompressionManager* getAuditEncryptionCompressionManager() {
        invariant(_ac);
        return _ac.get();
    }

    void setAuditAuthorizationSuccess(bool val);

    BSONObj getFilterBSON() const {
        return getConfig()->filterBSON;
    }

    /**
     * Check the event to be audited against the filter (if any)
     * and return true if it should be emitted to the audit log.
     */
    bool shouldAudit(const MatchableDocument* event) const {
        if (!_enabled) {
            return false;
        }
        auto cfg = getConfig();
        if (!cfg->filter) {
            return true;
        }
        return cfg->filter->matches(event);
    }

    /**
     * Get the current configuration generation.
     */
    const OID& getConfigGeneration() const {
        return getConfig()->generation;
    }

    /**
     * Check if 'file' is set as the audit destination.
     */
    bool isFileDestination() const {
        return (_format == AuditFormat::AuditFormatJsonFile) ||
            (_format == AuditFormat::AuditFormatBsonFile);
    }

    /**
     * Read the entire in-memory configuration guarded by lock.
     */
    AuditConfigDocument getAuditConfig() const;

    /**
     * Read the entire in-memory configuration guarded by lock.
     */
    AuditHeaderOptionsDocument getAuditHeaderOptions() const;

    /**
     * Create a MatchExpression from an owned filter object.
     */
    static std::unique_ptr<MatchExpression> parseFilter(BSONObj filter);

    /**
     * Update the in-memory configuration.
     */
    void setConfiguration(Client* client, const AuditConfigDocument& config);

protected:
    friend class AuditOpObserver;

    // Current in-memory state for runtime audit configuration.
    // Relies on thread safety of shared_ptr's copy constructor.
    // Writes happen in setConfiguration() by creating a new
    // config then swapping it in using std::atomic_exchange().
    // auditAuthorizationSuccess is special-cased as its own
    // atomic to allow runtime setParameter updates when
    // _runtimeConfiguration is false.
    struct RuntimeConfiguration {
        AtomicWord<bool> auditAuthorizationSuccess{false};
        BSONObj filterBSON;
        std::unique_ptr<MatchExpression> filter;
        OID generation;
    };

    std::shared_ptr<RuntimeConfiguration> getConfig() const {
        return std::atomic_load(&_config);  // NOLINT
    }

private:
    void _initializeAuditLog(const optionenvironment::Environment&);
    void _setDestinationFromConfig(const optionenvironment::Environment&);

private:
    // True if auditing should be done
    bool _enabled{false};

    // Path to audit log file, or :console if output to the terminal is desired
    std::string _path;

    // Format of the output, either text or BSON
    AuditFormat _format;

    // Configure filter/auditAuthorizationSuccess from {setAuditConfig:...}
    bool _runtimeConfiguration{false};

    // Configure compression/encryption of audit logs
    bool _compressionEnabled{false};

    // Current audit filter and audit success settings.
    std::shared_ptr<RuntimeConfiguration> _config;

    // We exclusively take this mutex during setConfiguration
    // to avoid confusion in the audit log about concurrent sets.
    Mutex _setConfigurationMutex = MONGO_MAKE_LATCH("AuditManager::setConfiguration");

    // Object to call encryption and compression on the audit logs.
    std::unique_ptr<AuditEncryptionCompressionManager> _ac;
};

/*
 * Gets the singleton AuditManager object for this server process.
 */
AuditManager* getGlobalAuditManager();

}  // namespace audit
}  // namespace mongo
