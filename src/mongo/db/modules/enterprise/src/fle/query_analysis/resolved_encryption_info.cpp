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
                "A deterministically encrypted field must have exactly one specified type.",
                this->bsonTypeSet && this->bsonTypeSet->isSingleType());

        auto& typeSet = *this->bsonTypeSet;
        auto checkTypeForDeterministic = [&typeSet](BSONType typeToCheck) {
            uassert(31122,
                    str::stream() << "Cannot use deterministic encryption for element of type: "
                                  << typeName(typeToCheck),
                    !typeSet.hasType(typeToCheck));
        };

        // These types cannot be supported with deterministic encryption because they are non-scalar
        // types. In general, equality comparison of objects and arrays (or the object inside a code
        // w/ scope) is not byte-wise.
        checkTypeForDeterministic(BSONType::Array);
        checkTypeForDeterministic(BSONType::Object);
        checkTypeForDeterministic(BSONType::CodeWScope);

        // Bool is not supported with deterministic encryption because it would be easy to reverse
        // the encryption. There will be only two distinct ciphertext values, and the distribution
        // may reveal which maps to true and which to false.
        checkTypeForDeterministic(BSONType::Bool);

        // Byte-wise comparison is also not correct in all cases for floating point numbers. For
        // instance, MQL equality semantics make all NaNs equal. Similarly, +0 and -0 are considered
        // equal. However, the various NaNs and various zeros consist of different byte sequences.
        //
        // For decimal128, the problem extends beyond NaNs and zeros. Any decimal128 number has
        // equalivalence classes: NumberDecimal(2.1) is equal to NumberDecimal(2.10) which is equal
        // to NumberDecimal(2.100), and so on. The encodings differ depending on the number of
        // significant digits.
        checkTypeForDeterministic(BSONType::NumberDouble);
        checkTypeForDeterministic(BSONType::NumberDecimal);
    }

    // Check for types that are prohibited for all encryption algorithms.
    if (this->bsonTypeSet) {
        auto& typeSet = *this->bsonTypeSet;
        auto checkType = [&typeSet](BSONType typeToCheck) {
            uassert(31041,
                    str::stream() << "Cannot encrypt element of type: " << typeName(typeToCheck),
                    !typeSet.hasType(typeToCheck));
        };

        // These "single-valued" types carry no information other than the type. Client-side
        // encryption does not hide the type, so encrypting these types would achieve nothing and is
        // actively prohibited.
        checkType(BSONType::MinKey);
        checkType(BSONType::MaxKey);
        checkType(BSONType::Undefined);
        checkType(BSONType::jstNULL);
    }
}

}  // namespace mongo
