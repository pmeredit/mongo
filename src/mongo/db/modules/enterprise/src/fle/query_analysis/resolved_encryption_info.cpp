/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "resolved_encryption_info.h"
#include "mongo/crypto/encryption_fields_util.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "mongo/platform/basic.h"
#include "mongo/util/overloaded_visitor.h"

namespace mongo {

namespace {

bool isTypeLegalForFle1Algorithm(BSONType bsonType, FleAlgorithmEnum algo) {
    if (algo == FleAlgorithmEnum::kRandom) {
        switch (bsonType) {
            // These "single-valued" types carry no information other than the type. Client-side
            // encryption does not hide the type, so encrypting these types would achieve nothing
            // and is actively prohibited.
            case BSONType::MinKey:
            case BSONType::MaxKey:
            case BSONType::Undefined:
            case BSONType::jstNULL:
                return false;
            default:
                return true;
        }
    } else if (algo == FleAlgorithmEnum::kDeterministic) {
        switch (bsonType) {
            // These "single-valued" types carry no information other than the type. Client-side
            // encryption does not hide the type, so encrypting these types would achieve nothing
            // and is actively prohibited.
            case BSONType::MinKey:
            case BSONType::MaxKey:
            case BSONType::Undefined:
            case BSONType::jstNULL:

            // These types cannot be supported with deterministic encryption because they are
            // non-scalar types. In general, equality comparison of objects and arrays (or the
            // object inside a code w/ scope) is not byte-wise.
            case BSONType::Array:
            case BSONType::Object:
            case BSONType::CodeWScope:

            // Bool is not supported with deterministic encryption because it would be easy to
            // reverse the encryption. There will be only two distinct ciphertext values, and the
            // distribution may reveal which maps to true and which to false.
            case BSONType::Bool:

            // Byte-wise comparison is also not correct in all cases for floating point numbers. For
            // instance, MQL equality semantics make all NaNs equal. Similarly, +0 and -0 are
            // considered equal. However, the various NaNs and various zeros consist of different
            // byte sequences.
            //
            // For decimal128, the problem extends beyond NaNs and zeros. Any decimal128 number has
            // equalivalence classes: NumberDecimal(2.1) is equal to NumberDecimal(2.10) which is
            // equal to NumberDecimal(2.100), and so on. The encodings differ depending on the
            // number of significant digits.
            case BSONType::NumberDouble:
            case BSONType::NumberDecimal:
                return false;

            default:
                return true;
        }
    } else {
        MONGO_UNREACHABLE;
    }
}

}  // namespace

ResolvedEncryptionInfo::ResolvedEncryptionInfo(
    UUID uuid,
    boost::optional<BSONType> bsonType,
    boost::optional<std::vector<QueryTypeConfig>> fle2SupportedQueries)
    : keyId(EncryptSchemaKeyId({std::move(uuid)})),
      bsonTypeSet(bsonType ? boost::optional<MatcherTypeSet>(bsonType.value()) : boost::none),
      fle2SupportedQueries(std::move(fle2SupportedQueries)) {

    // TODO: SERVER-67421
    // This function (and others) will need to be modified when the "algorithm"
    // datastructure is changed to a set (in resolved_encryption_info.h).

    // A field is considered unindexed unless the user specifies supported queries for it.
    algorithm = Fle2AlgorithmInt::kUnindexed;

    if (this->fle2SupportedQueries) {
        for (const auto& supportedQuery : this->fle2SupportedQueries.value()) {
            switch (supportedQuery.getQueryType()) {
                case QueryTypeEnum::Equality:
                    algorithm = Fle2AlgorithmInt::kEquality;
                    break;
                case QueryTypeEnum::RangePreviewDeprecated:
                case QueryTypeEnum::Range:
                    algorithm = Fle2AlgorithmInt::kRange;
                    break;
                default:
                    uasserted(6316400, "Encrypted field must have query type range or equality");
            }
        }
    }

    // Check for types that are prohibited for all encryption algorithms and types not legal in
    // combination with the FLE 2 encryption algorithm.
    if (this->bsonTypeSet.has_value()) {
        for (auto&& type : this->bsonTypeSet->bsonTypes) {
            uassert(6316404,
                    str::stream() << "Cannot use Queryable Encryption for element of type: "
                                  << typeName(type),
                    this->isTypeLegal(type));
        }
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

        uassert(
            31169,
            "A deterministically encrypted field cannot have a keyId represented by a JSON pointer",
            this->keyId.type() != EncryptSchemaKeyId::Type::kJSONPointer);
    }

    // Check for types that are prohibited for all encryption algorithms.
    if (this->bsonTypeSet) {
        for (auto&& type : this->bsonTypeSet->bsonTypes) {
            uassert(31122,
                    str::stream() << "Cannot encrypt element of type: " << typeName(type),
                    this->isTypeLegal(type));
        }
    }
}

bool ResolvedEncryptionInfo::algorithmIs(FleAlgorithmEnum fle1Alg) const {
    if (auto actualFle1Alg = get_if<FleAlgorithmEnum>(&algorithm)) {
        return *actualFle1Alg == fle1Alg;
    }
    return false;
}

bool ResolvedEncryptionInfo::algorithmIs(Fle2AlgorithmInt fle2Alg) const {
    if (auto actualFle2Alg = get_if<Fle2AlgorithmInt>(&algorithm)) {
        return *actualFle2Alg == fle2Alg;
    }
    return false;
}

bool ResolvedEncryptionInfo::isFle2Encrypted() const {
    return holds_alternative<Fle2AlgorithmInt>(algorithm);
}

bool ResolvedEncryptionInfo::isElemLegalForEncryption(BSONElement elem) const {
    if (elem.type() == BSONType::BinData)
        return isBinDataSubTypeLegalForEncryption(elem.binDataType());

    return isTypeLegal(elem.type());
}

bool ResolvedEncryptionInfo::isBinDataSubTypeLegalForEncryption(BinDataType binType) const {
    // Encrypting already encrypted data is not allowed.
    if (binType == BinDataType::Encrypt)
        return false;
    return true;
}

bool ResolvedEncryptionInfo::isTypeLegal(BSONType bsonType) const {
    return visit(OverloadedVisitor{[&](FleAlgorithmEnum algo) {
                                       return isTypeLegalForFle1Algorithm(bsonType, algo);
                                   },
                                   [&](Fle2AlgorithmInt algo) {
                                       switch (algo) {
                                           case Fle2AlgorithmInt::kEquality:
                                               return isFLE2EqualityIndexedSupportedType(bsonType);
                                           case Fle2AlgorithmInt::kRange:
                                               return isFLE2RangeIndexedSupportedType(bsonType);
                                           case Fle2AlgorithmInt::kUnindexed:
                                               return isFLE2UnindexedSupportedType(bsonType);
                                       }
                                       MONGO_UNREACHABLE;
                                   }},
                 algorithm);
}
}  // namespace mongo
