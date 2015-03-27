/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <bt_rlp_c.h>

#include "mongo/base/disallow_copying.h"

#include "rlp_loader.h"

namespace mongo {
namespace fts {

    class RlpContextCacheHandle;
    class RlpTokenIteratorFactory;

    /**
     * RlpTokenIterator
     *
     * Modeled after BT_RLP_TokenIterator, a simple C++ wrapper around the RLP C API
     * See rlp/doc/api-reference/cpp-reference/classBT_RLP_TokenIterator.html
     */
    class RlpTokenIterator {
        MONGO_DISALLOW_COPYING(RlpTokenIterator);
        friend class RlpTokenIteratorFactory;

    public:
        RlpTokenIterator(RlpEnvironment* rlpEnvironment);
        RlpTokenIterator(RlpEnvironment* rlpEnvironment, BT_RLP_TokenIteratorC* iterator);

#if defined(_MSC_VER) && _MSC_VER < 1900
        RlpTokenIterator(RlpTokenIterator&& other);

        RlpTokenIterator& operator=(RlpTokenIterator&& other);
#endif

        void reset(RlpTokenIterator&& other);
        bool next();
        bool isStopWord();
        const BT_Char16* getToken();
        const BT_Char16* getLemma();
        const BT_Char16* getStem();

    private:
        /**
         * RlpIteratorDeleteContext
         *
         * std::unique_ptr Deleter function for BT_RLP_TokenIteratorC
         */
        class RlpIteratorDeleteContext {
        public:
            RlpIteratorDeleteContext(RlpEnvironment* rlpEnvironment);

            void operator()(BT_RLP_TokenIteratorC* iterator) const;
            RlpEnvironment* getRlpEnvironment();

        private:
            RlpEnvironment* _rlpEnvironment;
        };

    private:
        std::unique_ptr<BT_RLP_TokenIteratorC, RlpIteratorDeleteContext> _iterator;
    };

}  // namespace fts
}  // namespace mongo
