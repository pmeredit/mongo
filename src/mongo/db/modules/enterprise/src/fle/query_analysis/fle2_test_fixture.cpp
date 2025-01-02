/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "fle2_test_fixture.h"

#include "mongo/bson/json.h"

namespace mongo {

void FLE2TestFixture::setUp() {
    kSsnFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "ssn",
                        "bsonType": "string",
                        "queries": {"queryType": "equality", "contention" : 1}
                    }
                ]
            }
        )");
    kAgeFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    }
                ]
            }
        )");
    kNestedAge = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "user.age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    }
                ]
            }
        )");
    kAgeAndSalaryFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    },
                    {
                        "keyId": {'$binary': "BSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "salary",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 1000000000, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    }
                ]
            }
        )");
    kAllFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    },
                    {
                        "keyId": {'$binary': "BSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "nested.age",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 200, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    },
                    {
                        "keyId": {'$binary': "CSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "salary",
                        "bsonType": "int",
                        "queries": {"queryType": "range", "min": 0, "max": 1000000000, "trimFactor": 0, "sparsity": 1, "contention" : 1}
                    },
                    {
                        "keyId": {'$binary': "DSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "ssn",
                        "bsonType": "string",
                        "queries": {"queryType": "equality", "contention" : 1}
                    }
                ]
            }
        )");
    kDateFields = fromjson(R"(
            {
                "fields": [
                    {
                        "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                        "path": "date",
                        "bsonType": "date",
                        "queries": {"queryType": "range", "trimFactor": 0, "sparsity": 1}
                    }
                ]
            }
        )");
    kTextFields = fromjson(R"(
        {
            "fields": [
                {
                    "keyId": {'$binary': "ASNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                    "path": "substringField",
                    "bsonType": "string",
                    "queries": {
                        "queryType": "substringPreview",
                        "strMaxLength": 100,
                        "strMinQueryLength": 1,
                        "strMaxQueryLength": 100,
                        "caseSensitive": false,
                        "diacriticSensitive": false
                    }
                },
                {
                    "keyId": {'$binary': "BSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                    "path": "suffixField",
                    "bsonType": "string",
                    "queries": {
                        "queryType": "suffixPreview",
                        "strMinQueryLength": 2,
                        "strMaxQueryLength": 20,
                        "caseSensitive": true,
                        "diacriticSensitive": false
                    }
                },
                {
                    "keyId": {'$binary': "CSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                    "path": "prefixField",
                    "bsonType": "string",
                    "queries": {
                        "queryType": "prefixPreview",
                        "strMinQueryLength": 2,
                        "strMaxQueryLength": 20,
                        "caseSensitive": false,
                        "diacriticSensitive": true
                    }
                },
                {
                    "keyId": {'$binary': "DSNFZ4mrze/ty6mHZUMhAQ==", $type: "04"},
                    "path": "comboField",
                    "bsonType": "string",
                    "queries": [{
                        "queryType": "prefixPreview",
                        "strMinQueryLength": 2,
                        "strMaxQueryLength": 20,
                        "caseSensitive": false,
                        "diacriticSensitive": true
                    },
                    {
                        "queryType": "suffixPreview",
                        "strMinQueryLength": 2,
                        "strMaxQueryLength": 20,
                        "caseSensitive": false,
                        "diacriticSensitive": true
                    }]
                }
            ]
        }
    )");

    limitsBackingBSON = BSON("minDouble" << -std::numeric_limits<double>::infinity() << "maxDouble"
                                         << std::numeric_limits<double>::infinity());
    kMinDouble = limitsBackingBSON["minDouble"];
    kMaxDouble = limitsBackingBSON["maxDouble"];
}

}  // namespace mongo
