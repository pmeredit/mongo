#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/constants.h"
#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/operator_dag_factory.h"
#include <iostream>

namespace streams {
namespace {

using std::string;
using std::tuple;
using std::vector;
using namespace mongo;

class ParseTest : public AggregationContextFixture {
public:
    std::unique_ptr<OperatorDag> parse(const std::string& pipeline) {
        OperatorDagFactory factory;
        const auto inputBson = fromjson("{pipeline: " + pipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        auto rawPipeline = parsePipelineFromBSON(inputBson["pipeline"]);
        return factory.fromBson(getExpCtx(), rawPipeline);
    }
};

TEST_F(ParseTest, SmokeTestRegularParsingErrorsWork) {
    vector<BSONObj> invalidUserPipeline{
        BSON("$addFields" << 1),
    };
    OperatorDagFactory factory;
    ASSERT_THROWS_CODE(
        factory.fromBson(getExpCtx(), invalidUserPipeline), AssertionException, 40272);
}

TEST_F(ParseTest, SmokeTestOnlySupportedStages) {
    std::string pipeline = R"(
[
    { $match: { a: 1 }},
    { $project: { a: 1 }},
    { $densify: { field: "timestamp", range: { step: 1, unit: "hour" } } }
]
    )";

    // We don't support $densify.
    ASSERT_THROWS_CODE(parse(pipeline), AssertionException, ErrorCode::kTemporaryErrorCode);
}


/**
Parse a user defined pipeline with all the supported MDP mapping stages.
Verify that we can create an OperatorDag from it, and that the operators
are of the correct type.
*/
TEST_F(ParseTest, SmokeTestSupportedStagesWork1) {
    OperatorDagFactory factory;

    std::string pipeline = R"(
[
    { $addFields: { a: 1 } },
    { $match: { a: 1 } },
    { $project: { a: 1 } },
    { $redact: { $cond: { 
        if: { $eq: [ "$a", 1 ] },
        then: "$$DESCEND",
        else: "$$PRUNE"
    }}},
    { $replaceRoot: { newRoot: "$name" }},
    { $replaceWith: "$name" },
    { $set: {
        b: 1
    }},
    { $unset: "copies" },
    { $unwind: "$sizes" }
]
    )";

    std::unique_ptr<OperatorDag> dag(parse(pipeline));
    auto& ops = dag->operators();
    ASSERT_EQ(ops.size(), 9);
    vector<DocumentSourceWrapperOperator*> mappers;
    for (const auto& op : ops) {
        ASSERT_TRUE(dynamic_cast<DocumentSourceWrapperOperator*>(op.get()));
        mappers.push_back(dynamic_cast<DocumentSourceWrapperOperator*>(op.get()));
    }
    ASSERT_EQ(mappers[0]->getName(), "AddFieldsOperator");
    ASSERT_EQ(mappers[1]->getName(), "MatchOperator");
    ASSERT_EQ(mappers[2]->getName(), "ProjectOperator");
    ASSERT_EQ(mappers[3]->getName(), "RedactOperator");
    ASSERT_EQ(mappers[4]->getName(), "ReplaceRootOperator");
    ASSERT_EQ(mappers[5]->getName(), "ReplaceRootOperator");  // From the user's $replaceWith.
    ASSERT_EQ(mappers[6]->getName(), "SetOperator");
    ASSERT_EQ(mappers[7]->getName(), "ProjectOperator");  // From the user's $unset.
    ASSERT_EQ(mappers[8]->getName(), "UnwindOperator");
}

/**
Put together different permutations of valid pipelines, and make sure they can
be parsed into an OpereatorDag.
We don't do much validation here on the results here,
other than "at least one operator was created" and "parsing didn't crash".
*/
TEST_F(ParseTest, SmokeTestSupportedStagesWork2) {
    vector<BSONObj> validStages{
        BSON("$addFields" << BSON("a" << 1)),
        BSON("$match" << BSON("a" << 1)),
        BSON("$project" << BSON("a" << 1)),
        BSON(
            "$redact" << BSON("$cond" << BSON("if" << BSON("$eq" << BSON_ARRAY("$a" << 1)) << "then"
                                                   << "$$DESCEND"
                                                   << "else"
                                                   << "$$PRUNE"))),
        BSON("$replaceRoot" << BSON("newRoot"
                                    << "$name")),
        BSON("$set" << BSON("b" << 1)),
        BSON("$unwind"
             << "$sizes"),
    };

    vector<vector<BSONObj>> validUserPipelines;
    for (size_t i = 0; i < validStages.size(); i++) {
        for (size_t j = 0; j < validStages.size(); j++) {
            for (size_t k = 0; k < validStages.size(); k++) {
                vector<BSONObj> pipeline{validStages[i], validStages[j], validStages[k]};
                validUserPipelines.push_back(pipeline);
            }
        }
    }

    OperatorDagFactory factory;
    for (const auto& pipeline : validUserPipelines) {
        std::unique_ptr<OperatorDag> dag(factory.fromBson(getExpCtx(), pipeline));
        const auto& ops = dag->operators();
        ASSERT_GTE(ops.size(), 1);
    }
}

/**
 * Verify that we're taking advantage of the pipeline->optimize logic.
 * The two $match stages should be merged into one.
 */
TEST_F(ParseTest, SmokeTestStagesOptimized) {
    vector<BSONObj> pipeline{BSON("$addFields" << BSON("a" << 1)),
                             BSON("$match" << BSON("a" << 1)),
                             BSON("$match" << BSON("a" << 1))};

    OperatorDagFactory factory;
    std::unique_ptr<OperatorDag> dag(factory.fromBson(getExpCtx(), pipeline));
    const auto& ops = dag->operators();
    ASSERT_EQ(ops.size(), 2);
    ASSERT_EQ(ops[0]->getName(), "AddFieldsOperator");
    ASSERT_EQ(ops[1]->getName(), "MatchOperator");
}

// TEST_F(ParseTest, OperatorDagHasCorrectSuccessors) {
//     // TODO: in next work.
// }

}  // namespace
}  // namespace streams
