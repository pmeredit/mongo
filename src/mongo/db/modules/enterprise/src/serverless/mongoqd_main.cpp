/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"
#include <iostream>

#include "mongoqd_main.h"

#include "mongo/s/mongos_main.h"


namespace mongo {

ExitCode mongoqd_main(int argc, char* argv[]) {
    return mongos_main(argc, argv);
}

}  // namespace mongo
