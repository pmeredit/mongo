/**
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include "audit_manager.h"

namespace mongo {
namespace audit {
    /*
     * Gets the singleton AuditManager object for this server process.
     */
    AuditManager* getGlobalAuditManager();

    /*
     * Sets the singleton AuditManager object for this server process.
     * Must be called once at startup and then never again (unless clearGlobalAuditManager
     * is called, at which point this can be called again, but should only happen in tests).
     */
    void setGlobalAuditManager(AuditManager* auditManager);

    /*
     * Sets the singleton AuditManager object for this server process to NULL.
     * Should only be used in tests.
     */
    void clearGlobalAuditManager();
} // namespace audit
} // namespace mongo
