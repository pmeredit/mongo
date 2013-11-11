/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <cstddef>
#include <string>

namespace mongo {
    class MatchExpression;

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
        AuditManager() : auditFilter(NULL), enabled(false) {}

        // Global filter describing what the admin desires to audit
        MatchExpression* auditFilter;

        // True if auditing should be done
        bool enabled;

        // Path to audit log file, or :console if output to the terminal is desired
        std::string auditLogPath;

        // Format of the output, either text or BSON
        AuditFormat auditFormat;
    };
}
}
