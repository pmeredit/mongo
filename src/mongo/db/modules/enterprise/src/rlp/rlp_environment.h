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
#include "rlp_context.h"
#include "rlp_decls.h"

namespace mongo {
namespace fts {

    class ContextFactory;

/**
* Declare all the C functions we need to call as private members of the RlpLoader class
*
* Generates
*  char* (* _rlp ) (int arg1)
*/
#define RLP_C_FUNC_DL_MEMBER(ret, name, args) ret(*_##name) args;

/**
* Declare all the C functions as methods on the class
*/
#define RLP_INTERFACE_FORWARD(ret, name, unused_args) \
    template <typename... Args>                       \
    ret name(Args&&... args) {                        \
        return _##name(std::forward<Args>(args)...);  \
    }

    /**
     * RlpLoader is responsible for loading Basis Tech Rosette Linguistics Platform binaries,
     * loading required C API calls from these libraries, and ensuring that the current loaded
     * RLP libraries are compatible with the RLP SDK we built against.
     */
    class RlpEnvironment {
        MONGO_DISALLOW_COPYING(RlpEnvironment);

    public:
        RlpEnvironment() = default;
        ~RlpEnvironment();

        /**
         * Initialize RLP system
         */
        static StatusWith<std::unique_ptr<RlpEnvironment>> create(std::string btRoot,
                                                                  bool verbose,
                                                                  SharedLibrary* coreLibrary);

        ContextFactory* getFactory() { return _factory.get(); }
        BT_RLP_EnvironmentC* getEnvironment() { return _rlpenv; }

        RLP_C_FUNC(RLP_INTERFACE_FORWARD)

    private:
        RLP_C_FUNC(RLP_C_FUNC_DL_MEMBER)

    private:
        BT_RLP_EnvironmentC* _rlpenv = nullptr;
        std::unique_ptr<ContextFactory> _factory;
    };

}  // namespace fts
}  // namespace mongo
