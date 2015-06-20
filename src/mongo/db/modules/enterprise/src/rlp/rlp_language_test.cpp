/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/db/fts/fts_query.h"
#include "mongo/db/fts/fts_tokenizer.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/text.h"

#include "rlp_language.h"
#include "rlp_loader.h"

namespace mongo {
namespace fts {
namespace {
using std::string;
using std::vector;

#ifdef MSC_VER
// Microsoft VS 2013 does not handle UTF-8 strings in char literal strings, error C4566
// The Microsoft compiler can be tricked into using UTF-8 strings as follows:
// 1. The file has a UTF-8 BOM
// 2. The string literal is a wide character string literal (ie, prefixed with L)
// at this point.
#define UTF8(x) toUtf8String(L##x)
#else
#define UTF8(x) x
#endif

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
    StatusWithFTSLanguage swl = FTSLanguage::make(language, TEXT_INDEX_VERSION_2);
    ASSERT_OK(swl);

    std::unique_ptr<FTSTokenizer> tokenizer(swl.getValue()->createTokenizer());

    tokenizer->reset(str, FTSTokenizer::None);

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
        vector<string> terms = tokenizeString(UTF8("أنا أحب القراءة كثيرا۔"), "arabic");

        ASSERT_EQUALS(5U, terms.size());
        ASSERT_EQUALS(UTF8("أنا"), terms[0]);
        ASSERT_EQUALS(UTF8("أحب"), terms[1]);
        ASSERT_EQUALS(UTF8("قراء"), terms[2]);
        ASSERT_EQUALS(UTF8("كثير"), terms[3]);
        ASSERT_EQUALS(UTF8("۔"),
                      terms[4]);  // U+06D4 - ARABIC FULL STOP  - TODO: filter this out with ICU
    }
}

TEST_F(RlpTest, Urdu) {
    // http://en.wikipedia.org/wiki/Urdu_language
    // lit. "(I) felt happiness (after) meeting you".
    vector<string> terms = tokenizeString(UTF8("آپ سے مِل کر خوشی ہوئی۔"), "urdu");

    ASSERT_EQUALS(7U, terms.size());
    ASSERT_EQUALS(UTF8("آپ"), terms[0]);
    ASSERT_EQUALS(UTF8("س"), terms[1]);
    ASSERT_EQUALS(UTF8("مل"), terms[2]);
    ASSERT_EQUALS(UTF8("کر"), terms[3]);
    ASSERT_EQUALS(UTF8("خوش"), terms[4]);
    ASSERT_EQUALS(UTF8("ہوئی"), terms[5]);
    ASSERT_EQUALS(UTF8("۔"), terms[6]);
}

TEST_F(RlpTest, WesternFarsi) {
    // http://en.wikipedia.org/wiki/Persian_grammar
    // My dog is smaller than your cat.
    vector<string> terms = tokenizeString(
        UTF8("سگ من از گربه‌ی تو کوچک‌تر است"), "iranian persian");

    ASSERT_EQUALS(7U, terms.size());
    ASSERT_EQUALS(UTF8("سگ"), terms[0]);
    ASSERT_EQUALS(UTF8("من"), terms[1]);
    ASSERT_EQUALS(UTF8("از"), terms[2]);
    ASSERT_EQUALS(UTF8("گرب"), terms[3]);
    ASSERT_EQUALS(UTF8("تو"), terms[4]);
    ASSERT_EQUALS(UTF8("کوچکتر"), terms[5]);
    ASSERT_EQUALS(UTF8("اس"), terms[6]);
}

TEST_F(RlpTest, Dari) {
    // http://en.wikipedia.org/wiki/Dari_language
    // to speak - persian farsi/persian dari
    vector<string> terms = tokenizeString(UTF8("حرف زدن/گپ زدن"), "dari");

    ASSERT_EQUALS(4U, terms.size());
    ASSERT_EQUALS(UTF8("حرف"), terms[0]);
    ASSERT_EQUALS(UTF8("زد"), terms[1]);
    ASSERT_EQUALS(UTF8("گپ"), terms[2]);
    ASSERT_EQUALS(UTF8("زد"), terms[3]);
}

TEST_F(RlpTest, SimplifiedChinese) {
    // Beijing University Biology Department
    vector<string> terms = tokenizeString(UTF8("北京大学生物系"), "simplified chinese");

    ASSERT_EQUALS(2U, terms.size());
    ASSERT_EQUALS(UTF8("北京大学"), terms[0]);
    ASSERT_EQUALS(UTF8("生物系"), terms[1]);
}

TEST_F(RlpTest, TraditionalChinese) {
    // Beijing University Biology Department
    vector<string> terms = tokenizeString(UTF8("北京大學生物系"), "traditional chinese");

    ASSERT_EQUALS(2U, terms.size());
    ASSERT_EQUALS(UTF8("北京大學"), terms[0]);
    ASSERT_EQUALS(UTF8("生物系"), terms[1]);
}
}  // namespace
}  // namespace fts
}  // namespace mongo
