/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include "mongo/base/status.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/record_store.h"

namespace mongo {

/**
 * Converts SQLite return codes to MongoDB statuses.
 */
Status sqliteRCToStatus(int retCode, const char* prefix = NULL);

/**
 * Checks if retStatus == desiredStatus; else calls fassert.
 */
void checkStatus(int retStatus, int desiredStatus, const char* fnName, const char* errMsg = NULL);

/**
 * Validate helper function to log an error and append the error to the results.
 */
void validateLogAndAppendError(ValidateResults* results, const std::string& errMsg);

/**
 * Checks if the database file is corrupt.
 */
void doValidate(OperationContext* opCtx, ValidateResults* results);

}  // namespace mongo
