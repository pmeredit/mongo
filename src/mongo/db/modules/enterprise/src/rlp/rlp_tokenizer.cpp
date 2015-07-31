/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "rlp_tokenizer.h"

#include "mongo/db/fts/unicode/codepoints.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace fts {

/**
 * Helper method to convert a given UTF-16 character or surrogate pair to its Unicode codepoint
 * value. Algorithm comes from https://tools.ietf.org/html/rfc2781 and
 * http://unicode.org/faq/utf_bom.html#utf16-4.
 */
char32_t utf16ToCodepoint(const BT_Char16* stem, size_t len) {
    // If the character is not a member of a surrogate pair, treat the first byte as a Unicode
    // codepoint and check if it is a delimiter.
    if ((stem[0] < 0xd800) || (stem[0] > 0xdfff)) {
        return static_cast<char32_t>(stem[0]);
    }

    // Ensure this is a valid UTF-16 code pair, where the first and second bytes are a valid high
    // and low surrogate values respectively. Bad UTF-8 should have been filtered out by now, and we
    // can assume that RLP's UTF-8 to UTF-16 conversion isn't broken.
    invariant((stem[0] >= 0xd800) && (stem[0] <= 0xdbff) && (len > 1) && (stem[1] >= 0xdc00) &&
              (stem[1] <= 0xdfff));

    // Now that we know that both units of the code pair are valid UTF-16 surrogate values, convert
    // the values to the corresponding Unicode codepoint.
    const char32_t kSurrogateOffset = 0x10000 - (0xd800 << 10) - 0xdc00;
    return static_cast<char32_t>((stem[0] << 10) + stem[1] + kSurrogateOffset);
}

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
        if ((_options & FTSTokenizer::kFilterStopWords) && _iterator.isStopWord())
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
            (_options & FTSTokenizer::kGenerateCaseSensitiveTokens)) {
            stem = _iterator.getToken();
        }

        // Filter punctuation out of the token stream
        // RLP returns consecutive punctuation characters as separate tokens
        size_t len = _context->getEnvironment()->bt_xwcslen(stem);
        if (len > 0 && unicode::codepointIsDelimiter(utf16ToCodepoint(stem, len),
                                                     unicode::DelimiterListLanguage::kNotEnglish)) {
            continue;
        }

        _stem.assign(
            stem, len, (_options & FTSTokenizer::kGenerateDiacriticSensitiveTokens) ? false : true);

        return true;
    }

    return false;
}

StringData RlpFTSTokenizer::get() const {
    return _stem.getStringData();
}

}  // namespace fts
}  // namespace mongo
