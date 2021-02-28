/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {
namespace audit {

enum AuditFormat {
    AuditFormatJsonFile = 0,
    AuditFormatBsonFile = 1,
    AuditFormatConsole = 2,
    AuditFormatSyslog = 3
};

/**
 * Contains server-wide auditing configuration.
 */
class AuditManager {
public:
    ~AuditManager();

    bool isEnabled() const {
        return _enabled;
    }

    /**
     * Called by MONGO_INITIALIZER to setup AuditManager from
     * system configuration file.
     */
    void initialize();

    const AuditFormat getFormat() const {
        return _format;
    }

    const std::string& getPath() const {
        return _path;
    }

    bool getAuditAuthorizationSuccess() const {
        return _auditAuthorizationSuccess.load();
    }

    void setAuditAuthorizationSuccess(bool val) {
        _auditAuthorizationSuccess.store(val);
    }

    /**
     * Check the event to be audited against the filter (if any)
     * and return true if it should be emitted to the audit log.
     */
    bool shouldAudit(const MatchableDocument* event) const {
        if (!_enabled) {
            return false;
        }
        if (!_filter) {
            return true;
        }
        return _filter->matches(event);
    }

private:
    void _setDestinationFromConfig();
    void _setFilter(BSONObj filter);

private:
    // True if auditing should be done
    bool _enabled{false};

    // Path to audit log file, or :console if output to the terminal is desired
    std::string _path;

    // Format of the output, either text or BSON
    AuditFormat _format;

    // Audit CRUD ops as well.
    AtomicWord<bool> _auditAuthorizationSuccess;

    // Global filter describing what the admin desires to audit
    // MatchExpression does not keep the original BSON in scope,
    // so we anchor it here.
    BSONObj _filterBSON;
    std::unique_ptr<MatchExpression> _filter;
};

/*
 * Gets the singleton AuditManager object for this server process.
 */
AuditManager* getGlobalAuditManager();

}  // namespace audit
}  // namespace mongo
