/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/base/status.h"

namespace mongo {
namespace fts {

    class RlpEnvironment;

    Status registerRlpLanguages(RlpEnvironment* rlpEnvironment);

}  // namespace fts
}  // namespace mongo
