/*
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"

namespace mongo {
namespace audit {

Status validateAuditLogDestination(const std::string& dest);
Status validateAuditLogFormat(const std::string& format);

}  // namespace audit
}  // namespace mongo
