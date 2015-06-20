/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <memory>
#include <string>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/platform/shared_library.h"
#include "rlp_environment.h"

namespace mongo {
namespace fts {

/**
 * RlpLoader is responsible for loading Basis Tech Rosette Linguistics Platform binaries,
 * loading required C API calls from these libraries, and ensuring that the current loaded
 * RLP libraries are compatible with the RLP SDK we built against.
 */
class RlpLoader {
    MONGO_DISALLOW_COPYING(RlpLoader);

public:
    /**
     * Initialize RLP system
     */
    static StatusWith<std::unique_ptr<RlpLoader>> create(std::string btRoot, bool verbose);

    RlpEnvironment* getEnvironment() {
        return _environment.get();
    }

private:
    RlpLoader(std::unique_ptr<SharedLibrary> libraryCoreC,
              std::unique_ptr<RlpEnvironment> environment)
        : _libraryCoreC(std::move(libraryCoreC)), _environment(std::move(environment)) {}

private:
    const std::unique_ptr<SharedLibrary> _libraryCoreC;
    const std::unique_ptr<RlpEnvironment> _environment;
};

}  // namespace fts
}  // namespace mongo
