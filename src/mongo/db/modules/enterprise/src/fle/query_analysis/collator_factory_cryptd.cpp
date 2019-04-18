/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/query/collation/collation_spec.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/db/service_context.h"

namespace mongo {

/**
 * A no-op implementation of the collator interface for mongocryptd. Some of the interfaces which
 * mongocryptd uses for query analysis expect a CollatorInterface pointer, where a null pointer
 * represents the simple collation and a non-null pointer represents a non-simple collation. We
 * provide this no-op implementation solely for compatibility with these interfaces. It should never
 * be used in an attempt to evaluate string comparisons.
 */
class CollatorInterfaceCryptd final : public CollatorInterface {
public:
    CollatorInterfaceCryptd() : CollatorInterface(CollationSpec("mock_locale", "mock_version")) {}

    std::unique_ptr<CollatorInterface> clone() const final {
        return std::make_unique<CollatorInterfaceCryptd>();
    }

    int compare(StringData left, StringData right) const final {
        // This collator interface is only used for static query analysis and should never be used
        // to make runtime collation-aware comparisons.
        MONGO_UNREACHABLE;
    }

    ComparisonKey getComparisonKey(StringData) const final {
        // This collator interface is only used for static query analysis and should never be used
        // to make runtime collation-aware comparisons.
        MONGO_UNREACHABLE;
    }
};

/**
 * A factory for instantiating CollatorInterfaceCryptd.
 */
class CollatorFactoryCryptd final : public CollatorFactoryInterface {
public:
    /**
     * If 'spec' is equal to {locale: "simple"}, returns nullptr. Otherwise, instantiates a
     * CollatorInterfaceCryptd.
     */
    StatusWith<std::unique_ptr<CollatorInterface>> makeFromBSON(const BSONObj& spec) final {
        const auto& cmp = SimpleBSONObjComparator::kInstance;
        if (cmp.evaluate(spec == CollationSpec::kSimpleSpec)) {
            return {nullptr};
        }
        return std::make_unique<CollatorInterfaceCryptd>();
    }
};

// On mongocryptd, initialize the service context with a cryptd-specific collator factor. This
// avoids depending on actual ICU sources or data files for a real collation implementation.
ServiceContext::ConstructorActionRegisterer registerCryptdCollatorFactor{
    "CreateCryptdCollatorFactory", [](ServiceContext* serviceContext) {
        CollatorFactoryInterface::set(serviceContext, std::make_unique<CollatorFactoryCryptd>());
    }};

}  // namespace mongo
