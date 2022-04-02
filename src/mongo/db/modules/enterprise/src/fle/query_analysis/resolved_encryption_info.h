/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/crypto/encryption_fields_gen.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/matcher/matcher_type_set.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_types.h"

namespace mongo {

/**
 * Represents the complete encryption metadata associated with a particular field path in the
 * encryption schema tree. Unlike the IDL types used to implement the 'encrypt' and
 * 'encryptMetadata' JSON Schema keywords (for FLE 1) or the EncryptedFieldConfig (for FLE 2), this
 * is an internal representation which has no meaningful serialization to and from BSON.
 *
 * Checks the following preconditions on construction:
 *  - Throws a user assertion if 'bsonTypeSet' is invalid in combination with kDeterministic
 *  encryption algorithm or with the FLE 2 encryption algorithm, depending on the constructor used.
 *  For example, with deterministic encryption the BSON type set must contain exactly one type.
 *  - Throws a user assertion if 'bsonTypeSet' specifies any single-valued types (e.g. null,
 *  undefined, minKey, or maxKey).
 */
struct ResolvedEncryptionInfo {
    /**
     * Throws an exception if 'bsonType' is never permitted to be encrypted by the client,
     * regardless of the encryption algorithm or any other encryption options.
     */
    static void throwIfIllegalTypeForEncryption(BSONType bsonType);

    /**
     * Returns true if 'bsonType' is allowed in combination with deterministic encryption.
     */
    static bool isTypeLegalWithDeterministic(BSONType bsonType);

    /**
     * Returns true if 'bsonType' is allowed in combination with FLE 2 encryption.
     */
    bool isTypeLegalWithFLE2(BSONType bsonType) const;

    /**
     * Returns true if 'algorithm' is any kind of FLE 2 encryption algorithm.
     */
    bool isFle2Encrypted() const;

    bool algorithmIs(FleAlgorithmEnum fle1Alg) const;
    bool algorithmIs(Fle2AlgorithmInt fle2Alg) const;

    /**
     * Constructs a ResolvedEncryptionInfo for FLE 2 encryption, throwing a user assertion on any
     * illegal combination of options.
     */
    ResolvedEncryptionInfo(UUID uuid,
                           BSONType bsonType,
                           boost::optional<std::vector<QueryTypeConfig>> fle2SupportedQueries);

    /**
     * Constructs a ResolvedEncryptionInfo, throwing a user assertion on any illegal combination of
     * options.
     */
    ResolvedEncryptionInfo(EncryptSchemaKeyId keyId,
                           FleAlgorithmEnum algorithm,
                           boost::optional<MatcherTypeSet> bsonTypeSet);

    bool operator==(const ResolvedEncryptionInfo& other) const {
        return keyId == other.keyId && algorithm == other.algorithm &&
            bsonTypeSet == other.bsonTypeSet;
    }

    bool operator!=(const ResolvedEncryptionInfo& other) const {
        return !(*this == other);
    }

    EncryptSchemaKeyId keyId;
    stdx::variant<FleAlgorithmEnum, Fle2AlgorithmInt> algorithm;
    boost::optional<MatcherTypeSet> bsonTypeSet;

    // For fields encrypted with FLE 2 encryption. When 'fle2SupportedQueries' is empty, then we
    // must have algorithm type Fle2AlgorithmInt::kUnindexed, and the field will be treated like
    // it is randomly encrypted during query analysis. Otherwise, it can be treated similarly (but
    // not identically) to deterministic encryption.
    boost::optional<std::vector<QueryTypeConfig>> fle2SupportedQueries;
};

}  // namespace mongo
