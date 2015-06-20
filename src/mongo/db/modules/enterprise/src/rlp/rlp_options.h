/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include <string>

namespace mongo {
namespace fts {

/**
 * RLP Global Parameters class
 */
struct RLPGlobalParams {
    /**
     * BT Root
     * see BT_RLP_Environment::SetBTRootDirector documentation for more details,
     * but this is the parent directory of a RLP SDK installation
     */
    std::string btRoot;
};

extern RLPGlobalParams rlpGlobalParams;

}  // namespace fts
}  // namespace mongo
