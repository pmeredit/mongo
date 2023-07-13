/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/matcher/expression.h"

namespace mongo {

/**
 * Validates a vector search filter. Raises a uassert error if an unsupported match operation is
 * found.
 */
void validateVectorSearchFilter(const MatchExpression* expr);

}  // namespace mongo
