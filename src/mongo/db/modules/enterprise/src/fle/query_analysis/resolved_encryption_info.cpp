/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "resolved_encryption_info.h"

namespace mongo {

ResolvedEncryptionInfo::ResolvedEncryptionInfo(EncryptSchemaKeyId keyId,
                                               FleAlgorithmEnum algorithm,
                                               boost::optional<MatcherTypeSet> bsonTypeSet)
    : keyId(std::move(keyId)), algorithm(algorithm), bsonTypeSet(std::move(bsonTypeSet)) {
    if (algorithm == FleAlgorithmEnum::kDeterministic) {
        uassert(31051,
                "A deterministically encrypted field must have exactly one specified "
                "non-object type.",
                this->bsonTypeSet && this->bsonTypeSet->isSingleType() &&
                    !this->bsonTypeSet->hasType(BSONType::Object));

        uassert(31122,
                "Cannot use deterministic encryption with element of type array",
                !this->bsonTypeSet->hasType(BSONType::Array));
    }

    if (this->bsonTypeSet) {
        auto& typeSet = *this->bsonTypeSet;
        auto checkType = [&typeSet](BSONType typeToCheck) {
            uassert(31041,
                    std::string("Cannot encrypt element of type: ").append(typeName(typeToCheck)),
                    !typeSet.hasType(typeToCheck));
        };
        checkType(BSONType::MinKey);
        checkType(BSONType::MaxKey);
        checkType(BSONType::Undefined);
        checkType(BSONType::jstNULL);
    }
}

}  // namespace mongo
