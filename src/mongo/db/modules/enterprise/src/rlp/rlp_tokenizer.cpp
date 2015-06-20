/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "rlp_tokenizer.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace fts {

RlpFTSTokenizer::RlpFTSTokenizer(
    std::unique_ptr<RlpContext, RlpContext::CacheReturnContext> context)
    : _context(std::move(context)),
      _factory(_context->getEnvironment()),
      _iterator(_context->getEnvironment()),
      _stem(_context->getEnvironment()) {
    // Set flags to improve RLP performance by skipping unnecessary analysis for tokenization
    _factory.setReturnReadings(false);
    _factory.setReturnCompoundComponents(false);
}

void RlpFTSTokenizer::reset(StringData document, Options options) {
    _options = std::move(options);

    BT_Result rc = _context->processBuffer(
        document.rawData(), document.size(), _context->getLanguage(), "UTF8", 0);
    uassert(28627,
            str::stream() << "Unable to process the string: '"
                          << (document.size() < 256 ? document
                                                    : document.substr(0, 256).toString() + "...")
                          << "'; received return code: " << static_cast<int>(rc) << ".",
            rc == BT_OK);

    _iterator.reset(_factory.createIterator(_context->getContext()));
}

bool RlpFTSTokenizer::moveNext() {
    while (_iterator.next()) {
        // Skip stop words
        if ((_options & FTSTokenizer::FilterStopWords) && _iterator.isStopWord())
            continue;

        // Arabic, Persian, & Urdu Language Analyzer returns STEM
        // Stemmer returns STEM
        const BT_Char16* stem = _iterator.getStem();

        // European Language Analyzer returns LEMMA, also CJK with the right options
        // (which we do not set)
        if ((stem == nullptr) || (stem[0] == L'\0')) {
            stem = _iterator.getLemma();
        }

        // Fallback to TOKEN
        // Or simply choose TOKEN when we want Case Sensitive tokens
        if ((stem == nullptr) || (stem[0] == L'\0') ||
            (_options & FTSTokenizer::GenerateCaseSensitiveTokens)) {
            stem = _iterator.getToken();
        }

        // Filter punctuation out of the token stream
        // RLP returns consecutive punctuation characters as separate tokens
        // BUG: We do not handle non-ASCII characters, we should filter on Pattern_Syntax,
        // see UAX # 44 (www.unicode.org/reports/tr44/)
        // TODO: use ICU and u_hasBinaryProperty(UTF16_GET(stem), UCHAR_PATTERN_SYNTAX),
        // remove strict length check to handle surrogates
        // JIRA: SERVER-8423
        size_t len = _context->getEnvironment()->bt_xwcslen(stem);
        if (len == 1 && iswpunct(stem[0])) {
            continue;
        }

        _stem.assign(stem, len);

        return true;
    }

    return false;
}

StringData RlpFTSTokenizer::get() const {
    return _stem.getStringData();
}

}  // namespace fts
}  // namespace mongo
