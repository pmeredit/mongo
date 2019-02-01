/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "encryption_schema_tree.h"

#include "mongo/bson/json.h"
#include "mongo/db/bson/bson_helper.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

TEST(EncryptionSchemaTreeTest, MarksTopLevelFieldAsEncrypted) {
    BSONObj schema =
        fromjson(R"({type: "object", properties: {ssn: {encrypt: {}}, name: {type: "string"}}})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("ssn")));
    ASSERT_FALSE(result->isEncrypted(FieldRef("ssn.nonexistent")));
    ASSERT_FALSE(result->isEncrypted(FieldRef("name")));
}

TEST(EncryptionSchemaTreeTest, MarksNestedFieldsAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: "object", 
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("user.ssn")));
    ASSERT_FALSE(result->isEncrypted(FieldRef("user")));
    ASSERT_FALSE(result->isEncrypted(FieldRef("user.name")));
}

TEST(EncryptionSchemaTreeTest, MarksNumericFieldNameAsEncrypted) {
    BSONObj schema = BSON("type"
                          << "object"
                          << "properties"
                          << BSON("0" << BSON("encrypt" << BSONObj())));
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("0")));

    schema = BSON("type"
                  << "object"
                  << "properties"
                  << BSON("nested" << BSON("type"
                                           << "object"
                                           << "properties"
                                           << BSON("0" << BSON("encrypt" << BSONObj())))));
    result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("nested.0")));
}

TEST(EncryptionSchemaTreeTest, MarksMultipleFieldsAsEncrypted) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {            
            ssn: {encrypt: {}},
            accountNumber: {encrypt: {}},
            super: {
                type: "object",
                properties: {
                    secret: {encrypt: {}}
                }
            }
        }})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("ssn")));
    ASSERT_TRUE(result->isEncrypted(FieldRef("accountNumber")));
    ASSERT_TRUE(result->isEncrypted(FieldRef("super.secret")));
    ASSERT_FALSE(result->isEncrypted(FieldRef("super")));
}

TEST(EncryptionSchemaTreeTest, TopLevelEncryptMarksEmptyPathAsEncrypted) {
    BSONObj schema = fromjson(R"({encrypt: {}})");
    auto result = EncryptionSchemaTreeNode::parse(schema);
    ASSERT_TRUE(result->isEncrypted(FieldRef("")));
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptAlongsideAnotherTypeKeyword) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                type: "object"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                bsonType: "BinData"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

TEST(EncryptionSchemaTreeTest, FailsToParseEncryptWithSiblingKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                properties: {invalid: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                minimum: 5,
                items: {}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51078);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfConflictingEncryptKeywords) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: {},
                properties: {
                    invalid: {encrypt: {}}
                },
                items: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinItems) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                type: "array", 
                items: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: "object",
                properties: {
                    ssn: {
                        type: "array", 
                        items: {encrypt: {}}
                    }
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptWithinAdditionalItems) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                type: "array", 
                items: {},
                additionalItems: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                additionalItems: {encrypt: {}}
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsNotTypeRestricted) {
    BSONObj schema = fromjson(R"({properties: {ssn: {encrypt: {}}}})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseIfEncryptParentIsTypeRestrictedWithMultipleTypes) {
    BSONObj schema = fromjson(R"({
        type: "object", 
        properties: {
            user: {
                type: ["object", "array"],
                properties: {
                    ssn: {encrypt: {}}
                }
            }
        }})");
    ASSERT_THROWS_CODE(EncryptionSchemaTreeNode::parse(schema), AssertionException, 51077);
}

TEST(EncryptionSchemaTreeTest, FailsToParseInvalidSchema) {
    BSONObj schema = fromjson(R"({properties: "invalid"})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::TypeMismatch);

    schema = fromjson(R"({
        type: "object", 
        properties: {
            ssn: {
                encrypt: "invalid"
            }
        }})");
    ASSERT_THROWS_CODE(
        EncryptionSchemaTreeNode::parse(schema), AssertionException, ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
