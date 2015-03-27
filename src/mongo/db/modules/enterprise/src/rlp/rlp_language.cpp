/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/base/make_string_vector.h"
#include "mongo/base/status.h"
#include "mongo/db/fts/fts_language.h"
#include "mongo/db/fts/fts_tokenizer.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "rlp_loader.h"
#include "rlp_options.h"
#include "rlp_tokenizer.h"

namespace mongo {
namespace fts {
namespace {

    // FTS RLP Language map
    //
    // Parameters:
    // - C++ unique identifier suffix
    // - lower case string name
    // - lower list of language aliases
    // - BT_LANGUAGE_ID language_id
    // - RLP XML context
    //
#define MONGO_FTS_RLP_LANGUAGE_LIST(MONGO_FTS_RLP_LANGUAGE_DECL)\
    MONGO_FTS_RLP_LANGUAGE_DECL(arabic  , "arabic"             , ("ara")         , BT_LANGUAGE_ARABIC              , kArabicContext) \
    MONGO_FTS_RLP_LANGUAGE_DECL(dari    , "dari"               , ("prs")         , BT_LANGUAGE_DARI                , kPersianContext) \
    MONGO_FTS_RLP_LANGUAGE_DECL(persian , "iranian persian"    , ("pes")         , BT_LANGUAGE_WESTERN_FARSI       , kPersianContext) \
    MONGO_FTS_RLP_LANGUAGE_DECL(urdu    , "urdu"               , ("urd")         , BT_LANGUAGE_URDU                , kUrduContext) \
    MONGO_FTS_RLP_LANGUAGE_DECL(zhs     , "simplified chinese" , ("zhs", "hans") , BT_LANGUAGE_SIMPLIFIED_CHINESE  , kChineseContext) \
    MONGO_FTS_RLP_LANGUAGE_DECL(zht     , "traditional chinese", ("zht", "hant") , BT_LANGUAGE_TRADITIONAL_CHINESE , kChineseContext)

    // Declare compilation unit local RLP language object
    // Must be declared statically as global language map only keeps a pointer to the language
    // instance
    //
#define LANGUAGE_DECL(id, name, aliases, language_id, context) \
    RlpFTSLanguage language##id(language_id, context);

    // Registers each language and language aliases in the language map if the user has a license
    // for the language
    //
#define LANGUAGE_INIT(id, name, aliases, language_id, context) \
    anyLanguageRegistered |= registerRlpLanguage(                 \
        rlpEnvironment, name, language_id, MONGO_MAKE_STRING_VECTOR aliases, &(language##id));

    // Supports Arabic
    // Returns RLP STEM, TOKEN
    const char kArabicContext[] =
        "<?xml version='1.0' encoding='utf-8' standalone='no' ?>"
        "<!DOCTYPE contextconfig SYSTEM 'http://www.basistech.com/dtds/2003/contextconfig.dtd'>"
        "<contextconfig>"
            "<properties>"
                // <!--Perform Normalization Form KC (NFKC)-->
                "<property name='com.basistech.ecn.FormKCNormalization' value='yes' /> "
            "</properties>"
            "<languageprocessors>"
                "<languageprocessor>Unicode Converter</languageprocessor>"
                "<languageprocessor>Encoding and Character Normalizer</languageprocessor>"
                "<languageprocessor>Sentence Breaker</languageprocessor>"
                "<languageprocessor>Word Breaker</languageprocessor>"
                "<languageprocessor>Arabic Language Analyzer</languageprocessor>"
                "<languageprocessor>Stopword Locator</languageprocessor>"
            "</languageprocessors>"
        "</contextconfig>";

    // Supports Western Farsi & Dari, depends on BT_LANGUAGE_ID
    // Returns RLP STEM, TOKEN
    const char kPersianContext[] =
        "<?xml version='1.0' encoding='utf-8' standalone='no' ?>"
        "<!DOCTYPE contextconfig SYSTEM 'http://www.basistech.com/dtds/2003/contextconfig.dtd'>"
        "<contextconfig>"
            "<properties>"
                // <!--Perform Normalization Form KC (NFKC)-->
                "<property name='com.basistech.ecn.FormKCNormalization' value='yes' /> "
            "</properties>"
            "<languageprocessors>"
                "<languageprocessor>Unicode Converter</languageprocessor>"
                "<languageprocessor>Encoding and Character Normalizer</languageprocessor>"
                "<languageprocessor>Sentence Breaker</languageprocessor>"
                "<languageprocessor>Word Breaker</languageprocessor>"
                "<languageprocessor>Persian Language Analyzer</languageprocessor>"
                "<languageprocessor>Stopword Locator</languageprocessor>"
            "</languageprocessors>"
        "</contextconfig>";

    // Supports Urdu
    // Returns RLP STEM, TOKEN
    const char kUrduContext[] =
        "<?xml version='1.0' encoding='utf-8' standalone='no' ?>"
        "<!DOCTYPE contextconfig SYSTEM 'http://www.basistech.com/dtds/2003/contextconfig.dtd'>"
        "<contextconfig>"
            "<properties>"
                // <!--Perform Normalization Form KC (NFKC)-->
                "<property name='com.basistech.ecn.FormKCNormalization' value='yes' /> "
            "</properties>"
            "<languageprocessors>"
                "<languageprocessor>Unicode Converter</languageprocessor>"
                "<languageprocessor>Encoding and Character Normalizer</languageprocessor>"
                "<languageprocessor>Sentence Breaker</languageprocessor>"
                "<languageprocessor>Word Breaker</languageprocessor>"
                "<languageprocessor>Urdu Language Analyzer</languageprocessor>"
                "<languageprocessor>Stopword Locator</languageprocessor>"
            "</languageprocessors>"
        "</contextconfig>";

    // Supports Simplified & Traditional Chinese scripts, depends on BT_LANGUAGE_ID
    // Returns RLP TOKEN
    const char kChineseContext[] =
        "<?xml version='1.0' encoding='utf-8' standalone='no' ?>"
        "<!DOCTYPE contextconfig SYSTEM 'http://www.basistech.com/dtds/2003/contextconfig.dtd'>"
        "<contextconfig>"
            "<properties>"
                // <!--To minimize memory usage-->
                "<property name='com.basistech.cla.pos' value='no' /> "
            "</properties>"
            "<languageprocessors>"
                "<languageprocessor>Unicode Converter</languageprocessor>"
                "<languageprocessor>Script Region Locator</languageprocessor>"
                "<languageprocessor>Sentence Breaker</languageprocessor>"
                "<languageprocessor>Chinese Language Analyzer</languageprocessor>"
            "</languageprocessors>"
        "</contextconfig>";

    /**
     * RlpFTSLanguage
     *
     * Represents all the information needs to create a tokenizer for an RLP language.
     */
    class RlpFTSLanguage : public FTSLanguage {
    public:
        RlpFTSLanguage(BT_LanguageID language, StringData context)
            : _language(language), _context(context), _rlpEnvironment(nullptr) {}

        void registerEnvironment(RlpEnvironment* rlpEnvironment) {
            _rlpEnvironment = rlpEnvironment;
        }

        std::unique_ptr<FTSTokenizer> createTokenizer() const final {
            std::unique_ptr<RlpContext, RlpContext::CacheReturnContext> handle(
                _rlpEnvironment->getFactory()->getContext(_language, _context));
            return stdx::make_unique<RlpFTSTokenizer>(std::move(handle));
        }

    private:
        const BT_LanguageID _language;
        const StringData _context;
        RlpEnvironment* _rlpEnvironment;
    };

    MONGO_FTS_RLP_LANGUAGE_LIST(LANGUAGE_DECL);

    bool registerRlpLanguage(RlpEnvironment* rlpEnvironment,
                             const StringData languageName,
                             BT_LanguageID languageId,
                             const std::vector<std::string>& aliases,
                             RlpFTSLanguage* language) {
        // If the user does not have an RLP license for this language, it is ok
        // We just do not support that language for this session
        //
        if (!rlpEnvironment->BT_RLP_Environment_HasLicenseForLanguage(
                rlpEnvironment->getEnvironment(), languageId, BT_RLP_LICENSE_FEATURE_TOKENIZER)) {
            warning() << "Cannot find valid license for RLP language " << languageName;
            return false;
        }

        LOG(2) << "Registering RLP Language " << languageName;

        language->registerEnvironment(rlpEnvironment);

        FTSLanguage::registerLanguage(languageName, TEXT_INDEX_VERSION_2, language);

        for (auto it = aliases.cbegin(); it != aliases.cend(); it++) {
            FTSLanguage::registerLanguageAlias(language, *it, TEXT_INDEX_VERSION_2);
        }

        return true;
    }
}  // namespace

    Status registerRlpLanguages(RlpEnvironment* rlpEnvironment) {
        bool anyLanguageRegistered = false;

        MONGO_FTS_RLP_LANGUAGE_LIST(LANGUAGE_INIT);

        if (!anyLanguageRegistered) {
            return Status(
                ErrorCodes::InvalidOptions,
                "Failed to find at least one Rosette Linguistics Platform language to load.");
        }

        return Status::OK();
    }

}  // namespace fts
}  // namespace mongo
