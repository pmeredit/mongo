/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/db/fts/fts_query.h"
#include "mongo/db/fts/fts_tokenizer.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/text.h"

#include "rlp_language.h"
#include "rlp_loader.h"
#include "rlp_tokenizer.h"

namespace mongo {
namespace fts {
namespace {
using std::string;
using std::vector;

class RlpTest : public mongo::unittest::Test {
public:
    void setUp() final {
        char* btRoot = getenv("MONGOD_UNITTEST_RLP_LANGUAGE_TEST_BTROOT");

        ASSERT(btRoot);

        StatusWith<std::unique_ptr<RlpLoader>> sw = RlpLoader::create(btRoot, true);

        ASSERT_OK(sw.getStatus());

        rlpLoader = std::move(sw.getValue());

        registerRlpLanguages(rlpLoader->getEnvironment(), false);

        return;
    }

private:
    std::unique_ptr<RlpLoader> rlpLoader;
};

vector<string> tokenizeString(const char* str, const char* language) {
    StatusWithFTSLanguage swl = FTSLanguage::make(language, TEXT_INDEX_VERSION_3);
    ASSERT_OK(swl);

    std::unique_ptr<FTSTokenizer> tokenizer(swl.getValue()->createTokenizer());

    tokenizer->reset(str, FTSTokenizer::kNone);

    vector<string> terms;

    while (tokenizer->moveNext()) {
        terms.push_back(tokenizer->get().toString());
    }

    return terms;
}

TEST_F(RlpTest, Arabic) {
    // Do a for loop to ensure we hit the context cache correctly
    for (int i = 0; i < 3; i++) {
        // http://en.wikipedia.org/wiki/Arabic_language
        // I love reading a lot
        vector<string> terms = tokenizeString("أنا أحب القراءة كثيرا۔", "arabic");

        ASSERT_EQUALS(4U, terms.size());
        ASSERT_EQUALS("أنا", terms[0]);
        ASSERT_EQUALS("أحب", terms[1]);
        ASSERT_EQUALS("قراء", terms[2]);
        ASSERT_EQUALS("كثير", terms[3]);
    }
}

TEST_F(RlpTest, Urdu) {
    // http://en.wikipedia.org/wiki/Urdu_language
    // lit. "(I) felt happiness (after) meeting you".
    vector<string> terms = tokenizeString("آپ سے مِل کر خوشی ہوئی۔", "urdu");

    ASSERT_EQUALS(6U, terms.size());
    ASSERT_EQUALS("آپ", terms[0]);
    ASSERT_EQUALS("س", terms[1]);
    ASSERT_EQUALS("مل", terms[2]);
    ASSERT_EQUALS("کر", terms[3]);
    ASSERT_EQUALS("خوش", terms[4]);
    ASSERT_EQUALS("ہوئی", terms[5]);
}

TEST_F(RlpTest, WesternFarsi) {
    // http://en.wikipedia.org/wiki/Persian_grammar
    // My dog is smaller than your cat.
    vector<string> terms = tokenizeString(
        "سگ من از گربه‌ی تو کوچک‌تر است", "iranian persian");

    ASSERT_EQUALS(7U, terms.size());
    ASSERT_EQUALS("سگ", terms[0]);
    ASSERT_EQUALS("من", terms[1]);
    ASSERT_EQUALS("از", terms[2]);
    ASSERT_EQUALS("گرب", terms[3]);
    ASSERT_EQUALS("تو", terms[4]);
    ASSERT_EQUALS("کوچکتر", terms[5]);
    ASSERT_EQUALS("اس", terms[6]);
}

TEST_F(RlpTest, Dari) {
    // http://en.wikipedia.org/wiki/Dari_language
    // to speak - persian farsi/persian dari
    vector<string> terms = tokenizeString("حرف زدن/گپ زدن", "dari");

    ASSERT_EQUALS(4U, terms.size());
    ASSERT_EQUALS("حرف", terms[0]);
    ASSERT_EQUALS("زد", terms[1]);
    ASSERT_EQUALS("گپ", terms[2]);
    ASSERT_EQUALS("زد", terms[3]);
}

TEST_F(RlpTest, SimplifiedChinese) {
    // Beijing University Biology Department
    vector<string> terms = tokenizeString("北京大学生物系", "simplified chinese");

    ASSERT_EQUALS(2U, terms.size());
    ASSERT_EQUALS("北京大学", terms[0]);
    ASSERT_EQUALS("生物系", terms[1]);
}

TEST_F(RlpTest, TraditionalChinese) {
    // Beijing University Biology Department
    vector<string> terms = tokenizeString("北京大學生物系", "traditional chinese");

    ASSERT_EQUALS(2U, terms.size());
    ASSERT_EQUALS("北京大學", terms[0]);
    ASSERT_EQUALS("生物系", terms[1]);
}

TEST(EncodingTest, Utf16Codepoint) {
    // Should map to itself.
    unsigned short singleByte[1] = {0xd3};

    // Should map to codepoint 0x12345.
    unsigned short multiByte[2] = {0xd808, 0xdf45};

    ASSERT_EQUALS(0xd3, utf16ToCodepoint(singleByte, 1));
    ASSERT_EQUALS(0x12345, utf16ToCodepoint(multiByte, 2));
}

DEATH_TEST(EncodingTest, Utf16UnmatchedSurrogate, "Invariant failure (stem[0] >= 0xd800) &&") {
    unsigned short unmatchedSurrogate[1] = {0xd808};
    utf16ToCodepoint(unmatchedSurrogate, 1);
}

DEATH_TEST(EncodingTest, Utf16BadSurrogate, "Invariant failure (stem[0] >= 0xd800) &&") {
    unsigned short invalidSurrogate[2] = {0xd808, 0xd3};
    utf16ToCodepoint(invalidSurrogate, 2);
}

}  // namespace
}  // namespace fts
}  // namespace mongo
