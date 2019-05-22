/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/matcher/matcher_type_set.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/matcher/schema/encrypt_schema_types.h"

namespace mongo {

/**
 * Represents the complete encryption metadata associated with a particular field path in the
 * encryption schema tree. Unlike the IDL types used to implement the 'encrypt' and
 * 'encryptMetadata' JSON Schema keywords, this is an internal representation which has no
 * meaningful serialization to and from BSON.
 *
 * Checks the following preconditions on construction:
 *  - Throws a user assertion if 'bsonTypeSet' is invalid in combination with kDeterministic
 *  encryption algorithm. For example, with deterministic encryption the BSON type set must contain
 *  exactly one type.
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

    const EncryptSchemaKeyId keyId;
    const FleAlgorithmEnum algorithm;
    const boost::optional<MatcherTypeSet> bsonTypeSet;
};

}  // namespace mongo
