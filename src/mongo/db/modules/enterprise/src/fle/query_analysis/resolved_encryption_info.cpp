/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "resolved_encryption_info.h"
#include "mongo/crypto/encryption_fields_util.h"
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
        for (auto& supportedQuery : this->fle2SupportedQueries.value()) {
            bool rangeQuery = false;
            switch (supportedQuery.getQueryType()) {
                case QueryTypeEnum::Equality:
                    algorithm = Fle2AlgorithmInt::kEquality;
                    break;
                case QueryTypeEnum::Range:
                    algorithm = Fle2AlgorithmInt::kRange;
                    rangeQuery = true;
                    break;
                default:
                    uassert(
                        6316400, "Encrypted field must have query type range or equality", 0 == 1);
            }
            if (!rangeQuery) {
                // Sparsity could be passed in for an equality index, but it will never be utilized
                // by our code in a non-range case, so it doesn't really matter.
                uassert(6720000,
                        "You cannot use any of {min, max, sparsity} on non-range queries",
                        !(supportedQuery.getMin() || supportedQuery.getMax()));
            } else {
                if (bsonType == BSONType::NumberDouble) {
                    supportedQuery.setMin(mongo::Value(std::numeric_limits<double>::min()));
                    supportedQuery.setMax(mongo::Value(std::numeric_limits<double>::max()));
                }
                if (bsonType == BSONType::NumberDecimal) {
                    supportedQuery.setMin(mongo::Value(Decimal128::kLargestNegative));
                    supportedQuery.setMax(mongo::Value(Decimal128::kLargestPositive));
                }
                uassert(6720001,
                        "You must set a min and a max value for range queries",
                        supportedQuery.getMin() && supportedQuery.getMax());
                isRangeConfigLegal(supportedQuery);
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
    return stdx::holds_alternative<Fle2AlgorithmInt>(algorithm);
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
    return stdx::visit(
        OverloadedVisitor{
            [&](FleAlgorithmEnum algo) { return isTypeLegalForFle1Algorithm(bsonType, algo); },
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

Value ResolvedEncryptionInfo::coerceValueToRangeIndexTypes(
    Value val, BSONType fieldType, mongo::StringData useCase = "literal") const {

    BSONType valType = val.getType();

    if (valType == fieldType)
        return val;

    if (valType == Date || fieldType == Date) {
        uassert(6720002,
                str::stream() << "If the " << useCase
                              << " type is a date, the type of the "
                                 "field must also be data (and vice versa). "
                              << useCase << " type: " << valType << " , Field type: " << fieldType,
                valType == fieldType);
        return val;
    }

    uassert(6742000,
            str::stream() << useCase << " type isn't supported for range encrypted indices. "
                          << useCase << " type: " << valType,
            isNumericBSONType(valType));

    // If we get to this point, we've already established that valType and fieldType are NOT the
    // same type, so if either of them is a double or a decimal we can't coerce.
    if (valType == NumberDecimal || valType == NumberDouble || fieldType == NumberDecimal ||
        fieldType == NumberDouble) {
        uasserted(
            6742002,
            str::stream()
                << "If the " << useCase
                << " type and the field type are not the same type and one or both of them is"
                   " a double or a decimal, coercion of the "
                << useCase
                << " type to field type is not supported, due to possible loss of precision.");
    }

    switch (fieldType) {
        case NumberInt:
            return Value(val.Value::coerceToInt());
        case NumberLong:
            return Value(val.Value::coerceToLong());
        default:
            MONGO_UNREACHABLE;
    }
}

void ResolvedEncryptionInfo::isRangeConfigLegal(QueryTypeConfig query) const {
    uassert(6720003,
            "Min and max must both be set for range queries.",
            query.getMin() && query.getMax());

    if (this->bsonTypeSet) {
        for (auto&& fieldType : this->bsonTypeSet->bsonTypes) {
            Value min = *query.getMin();
            Value max = *query.getMax();
            BSONType minType = min.getType();
            BSONType maxType = max.getType();

            uassert(6720004, "Min and max must the same type.", minType == maxType);

            Value coercedMin = coerceValueToRangeIndexTypes(min, fieldType, "bounds");
            Value coercedMax = coerceValueToRangeIndexTypes(max, fieldType, "bounds");

            uassert(6720005, "Min must be <= max.", Value::compare(min, max, nullptr) <= 0);
        }
    }
}
}  // namespace mongo
