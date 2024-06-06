/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

namespace mongo {
namespace audit {

enum class AuditFormat {
    AuditFormatJsonFile = 0,
    AuditFormatBsonFile = 1,
    AuditFormatConsole = 2,
    AuditFormatSyslog = 3,
    AuditFormatMock = 4,
};

}  // namespace audit
}  // namespace mongo
