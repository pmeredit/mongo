/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

namespace mongo {
namespace audit {

    enum AuditFormat {
        AuditFormatText = 0,
        AuditFormatBson = 1
    };

    AuditFormat getAuditFormat();

} // namespace audit
} // namespace mongo
