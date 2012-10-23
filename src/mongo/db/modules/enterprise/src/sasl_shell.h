/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#pragma once

struct Gsasl;

namespace mongo {

    Gsasl* getShellGsaslContext();

}  // namespace mongo
