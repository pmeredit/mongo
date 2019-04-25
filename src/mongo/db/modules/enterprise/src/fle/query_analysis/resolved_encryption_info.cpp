/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "resolved_encryption_info.h"

namespace mongo {

ResolvedEncryptionInfo::ResolvedEncryptionInfo(EncryptSchemaKeyId keyId,
                                               FleAlgorithmEnum algorithm,
                                               boost::optional<ConstDataRange> initializationVector,
                                               boost::optional<MatcherTypeSet> bsonTypeSet)
    : keyId(std::move(keyId)),
      algorithm(algorithm),
      initializationVector(dataRangeToVector(initializationVector)),
      bsonTypeSet(std::move(bsonTypeSet)) {
    if (algorithm == FleAlgorithmEnum::kDeterministic) {
        uassert(31051,
                "A deterministically encrypted field must have exactly one specified "
                "non-object type.",
                this->bsonTypeSet && this->bsonTypeSet->isSingleType() &&
                    !this->bsonTypeSet->hasType(BSONType::Object));

        uassert(51096,
                "Deterministic algorithm must be accompanied with an initialization vector in "
                "encrypt object combined with encryptMetadata",
                this->initializationVector);
    }

    if (this->bsonTypeSet) {
        auto& typeSet = *this->bsonTypeSet;
        auto checkType = [&typeSet](BSONType typeToCheck) {
            uassert(31041,
                    std::string("Cannot encrypt single-valued type").append(typeName(typeToCheck)),
                    !typeSet.hasType(typeToCheck));
        };
        checkType(BSONType::MinKey);
        checkType(BSONType::MaxKey);
        checkType(BSONType::Undefined);
        checkType(BSONType::jstNULL);
    }
}

boost::optional<std::vector<std::uint8_t>> ResolvedEncryptionInfo::dataRangeToVector(
    boost::optional<ConstDataRange> dataRange) {
    if (!dataRange) {
        return boost::none;
    }

    return std::vector<std::uint8_t>(
        reinterpret_cast<const uint8_t*>(dataRange->data()),
        reinterpret_cast<const uint8_t*>(dataRange->data() + dataRange->length()));
}

}  // namespace mongo
