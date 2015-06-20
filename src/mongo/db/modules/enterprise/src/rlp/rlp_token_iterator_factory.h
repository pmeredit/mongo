/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <bt_rlp_c.h>

#include "mongo/base/disallow_copying.h"

#include "rlp_environment.h"
#include "rlp_token_iterator.h"

namespace mongo {
namespace fts {

class RlpContextCacheHandle;
class RlpTokenIteratorFactory;

/**
 * RlpTokenIteratorFactory
 *
 * Modeled after BT_RLP_TokenIteratorFactory, a simple C++ wrapper around the RLP C API
 * See rlp/doc/api-reference/cpp-reference/classBT__RLP__TokenIteratorFactory.html
 */
class RlpTokenIteratorFactory {
    MONGO_DISALLOW_COPYING(RlpTokenIteratorFactory);

public:
    RlpTokenIteratorFactory(RlpEnvironment* rlpEnvironment);

    void setReturnReadings(bool flag);
    void setReturnCompoundComponents(bool flag);
    RlpTokenIterator createIterator(BT_RLP_ContextC* context);

private:
    /**
     * RlpIteratorFactoryDeleteContext
     *
     * std::unique_ptr Deleter function for BT_RLP_TokenIteratorFactoryC
     */
    class RlpIteratorFactoryDeleteContext {
    public:
        RlpIteratorFactoryDeleteContext(RlpEnvironment* rlpEnvironment);

        void operator()(BT_RLP_TokenIteratorFactoryC* iterator) const;
        RlpEnvironment* getRlpEnvironment();

    private:
        RlpEnvironment* _rlpEnvironment;
    };

private:
    std::unique_ptr<BT_RLP_TokenIteratorFactoryC, RlpIteratorFactoryDeleteContext> const _factory;
};

}  // namespace fts
}  // namespace mongo
