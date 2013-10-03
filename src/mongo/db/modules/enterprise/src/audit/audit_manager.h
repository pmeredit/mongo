/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <cstddef>

namespace mongo {
    class MatchExpression;

namespace audit {

    /**
     * Contains server-wide auditing configuration.
     */
    class AuditManager {
    public:
        AuditManager() : auditFilter(NULL), enabled(false) {}

        // Global filter describing what the admin desires to audit
        MatchExpression* auditFilter;

        // True if auditing should be done
        bool enabled;
    };
}
}
