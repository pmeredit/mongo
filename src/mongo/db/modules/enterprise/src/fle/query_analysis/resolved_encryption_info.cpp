/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "resolved_encryption_info.h"

namespace mongo {

ResolvedEncryptionInfo::ResolvedEncryptionInfo(
    UUID uuid,
    BSONType bsonType,
    boost::optional<std::vector<QueryTypeConfig>> fle2SupportedQueries)
    : keyId(EncryptSchemaKeyId({std::move(uuid)})),
      bsonTypeSet(MatcherTypeSet(bsonType)),
      fle2SupportedQueries(std::move(fle2SupportedQueries)) {

    // A field is considered unindexed unless the user specifies supported queries for it.
    algorithm = Fle2AlgorithmInt::kUnindexed;
    if (this->fle2SupportedQueries) {
        for (const auto& supportedQuery : this->fle2SupportedQueries.get()) {
            uassert(6316400,
                    "Encrypted field must have query type equality",
                    supportedQuery.getQueryType() == QueryTypeEnum::Equality);
            algorithm = Fle2AlgorithmInt::kEquality;
        }
    }

    // Check for types that are prohibited for all encryption algorithms and types not legal in
    // combination with the FLE 2 encryption algorithm.
    for (auto&& type : this->bsonTypeSet->bsonTypes) {
        // TODO SERVER-63657: replace "FLE 2" in the following error message with chosen string.
        uassert(6316404,
                str::stream() << "Cannot use FLE 2 encryption for element of type: "
                              << typeName(type),
                isTypeLegalWithFLE2(type));
        throwIfIllegalTypeForEncryption(type);
    }
}

ResolvedEncryptionInfo::ResolvedEncryptionInfo(EncryptSchemaKeyId keyId,
                                               FleAlgorithmEnum algorithm,
                                               boost::optional<MatcherTypeSet> bsonTypeSet)
    : keyId(std::move(keyId)), algorithm(algorithm), bsonTypeSet(std::move(bsonTypeSet)) {
    if (algorithm == FleAlgorithmEnum::kDeterministic) {
        uassert(31051,
                "A deterministically encrypted field must have exactly one specified type.",
                this->bsonTypeSet && this->bsonTypeSet->isSingleType());

        // Check for bson types that are not legal in combination with the deterministic encryption
        // algorithm.
        for (auto&& type : this->bsonTypeSet->bsonTypes) {
            uassert(31122,
                    str::stream() << "Cannot use deterministic encryption for element of type: "
                                  << typeName(type),
                    isTypeLegalWithDeterministic(type));
        }

        uassert(
            31169,
            "A deterministically encrypted field cannot have a keyId represented by a JSON pointer",
            this->keyId.type() != EncryptSchemaKeyId::Type::kJSONPointer);
    }

    // Check for types that are prohibited for all encryption algorithms.
    if (this->bsonTypeSet) {
        for (auto&& type : this->bsonTypeSet->bsonTypes) {
            throwIfIllegalTypeForEncryption(type);
        }
    }
}

bool ResolvedEncryptionInfo::algorithmIs(FleAlgorithmEnum fle1Alg) const {
    if (auto actualFle1Alg = stdx::get_if<FleAlgorithmEnum>(&algorithm)) {
        return *actualFle1Alg == fle1Alg;
    }
    return false;
}

bool ResolvedEncryptionInfo::algorithmIs(Fle2AlgorithmInt fle2Alg) const {
    if (auto actualFle2Alg = stdx::get_if<Fle2AlgorithmInt>(&algorithm)) {
        return *actualFle2Alg == fle2Alg;
    }
    return false;
}

bool ResolvedEncryptionInfo::isFle2Encrypted() const {
    return algorithmIs(Fle2AlgorithmInt::kUnindexed) || algorithmIs(Fle2AlgorithmInt::kEquality);
}

bool ResolvedEncryptionInfo::isTypeLegalWithDeterministic(BSONType bsonType) {
    switch (bsonType) {
        // These types cannot be supported with deterministic encryption because they are non-scalar
        // types. In general, equality comparison of objects and arrays (or the object inside a code
        // w/ scope) is not byte-wise.
        case BSONType::Array:
        case BSONType::Object:
        case BSONType::CodeWScope:

        // Bool is not supported with deterministic encryption because it would be easy to reverse
        // the encryption. There will be only two distinct ciphertext values, and the distribution
        // may reveal which maps to true and which to false.
        case BSONType::Bool:

        // Byte-wise comparison is also not correct in all cases for floating point numbers. For
        // instance, MQL equality semantics make all NaNs equal. Similarly, +0 and -0 are considered
        // equal. However, the various NaNs and various zeros consist of different byte sequences.
        //
        // For decimal128, the problem extends beyond NaNs and zeros. Any decimal128 number has
        // equalivalence classes: NumberDecimal(2.1) is equal to NumberDecimal(2.10) which is equal
        // to NumberDecimal(2.100), and so on. The encodings differ depending on the number of
        // significant digits.
        case BSONType::NumberDouble:
        case BSONType::NumberDecimal:
            return false;

        default:
            return true;
    }
}

bool ResolvedEncryptionInfo::isTypeLegalWithFLE2(BSONType bsonType) const {
    // Unindexed FLE 2 encryption behaves similarly to random encryption.
    if (algorithmIs(Fle2AlgorithmInt::kUnindexed)) {
        return true;
    }

    // FLE 2 encryption with equality queries behaves similarly to deterministic encryption. The
    // reason for banning the following types is due to the same logic as above: FLE 2 encryption
    // also relies on exact bit-for-bit comparison, which in general does not hold for these types.
    // Note: bool IS supported under FLE 2 encryption but not FLE 1 deterministic encryption.
    switch (bsonType) {
        case BSONType::Array:
        case BSONType::Object:
        case BSONType::CodeWScope:
        case BSONType::NumberDouble:
        case BSONType::NumberDecimal:
            return false;

        default:
            return true;
    }
}

void ResolvedEncryptionInfo::throwIfIllegalElemForEncryption(BSONElement elem) {
    elem.type() == BSONType::BinData ? throwIfIllegalBinDataSubType(elem.binDataType())
                                     : throwIfIllegalTypeForEncryption(elem.type());
}

void ResolvedEncryptionInfo::throwIfIllegalBinDataSubType(BinDataType binType) {
    // Encrypting already encrypted data is not allowed.
    uassert(64095,
            str::stream() << "Cannot encrypt binary data of subtype 6",
            binType != BinDataType::Encrypt);
}

void ResolvedEncryptionInfo::throwIfIllegalTypeForEncryption(BSONType bsonType) {
    switch (bsonType) {
        // These "single-valued" types carry no information other than the type. Client-side
        // encryption does not hide the type, so encrypting these types would achieve nothing and is
        // actively prohibited.
        case BSONType::MinKey:
        case BSONType::MaxKey:
        case BSONType::Undefined:
        case BSONType::jstNULL:
            uasserted(31041,
                      str::stream() << "Cannot encrypt element of type: " << typeName(bsonType));
        default:
            return;
    }
}

}  // namespace mongo
