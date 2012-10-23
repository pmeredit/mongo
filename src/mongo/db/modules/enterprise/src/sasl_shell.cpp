/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_shell.h"

#include <gsasl.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/assert_util.h"

namespace mongo {

namespace {

    Gsasl* _gsaslLibraryContext = NULL;

    MONGO_INITIALIZER(SaslShellContext)(InitializerContext* context) {
        fassert(0, _gsaslLibraryContext == NULL);

        if (!gsasl_check_version(GSASL_VERSION))
            return Status(ErrorCodes::UnknownError, "Incompatible gsasl library.");

        int rc = gsasl_init(&_gsaslLibraryContext);
        if (GSASL_OK != rc)
            return Status(ErrorCodes::UnknownError, gsasl_strerror(rc));
        return Status::OK();
    }

}  // namespace

    Gsasl* getShellGsaslContext() { return _gsaslLibraryContext; }

}  // namespace mongo
