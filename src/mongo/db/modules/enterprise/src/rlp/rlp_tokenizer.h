/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/fts/fts_tokenizer.h"
#include "mongo/util/stringutils.h"

#include "rlp_context.h"
#include "rlp_stringbuffer.h"
#include "rlp_token_iterator.h"
#include "rlp_token_iterator_factory.h"

namespace mongo {
namespace fts {

/**
 * Helper method to convert a given UTF-16 character or surrogate pair to its Unicode codepoint
 * value.
 */
char32_t utf16ToCodepoint(const BT_Char16* stem, size_t len);

/**
 * RlpFTSTokenizer
 *
 * Implements an FTSTokenizer using RLPTokenIterator from the
 * Basis Tech Rosette Linguistics Platform library
 */
class RlpFTSTokenizer : public FTSTokenizer {
    MONGO_DISALLOW_COPYING(RlpFTSTokenizer);

public:
    RlpFTSTokenizer(std::unique_ptr<RlpContext, RlpContext::CacheReturnContext> context);

    void reset(StringData document, Options options) final;

    bool moveNext() final;

    /**
     * Note: returned string lifetime is tied to lifetime of class.
     * Also, it is invalidated on each call to moveNext.
     */
    StringData get() const final;

private:
    std::unique_ptr<RlpContext, RlpContext::CacheReturnContext> _context;
    RlpTokenIteratorFactory _factory;
    RlpTokenIterator _iterator;
    RlpStringBuffer _stem;
    Options _options;
};

}  // namespace fts
}  // namespace mongo
