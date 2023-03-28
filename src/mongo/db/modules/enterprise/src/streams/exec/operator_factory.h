#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/constants.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include <unordered_map>

namespace streams {

/**
 * OperatorFactory is used to create streaming Operators from DocumentSources.
 * See usage in Parser.
 */
class OperatorFactory {
public:
    void validateByName(const std::string& name);
    std::unique_ptr<Operator> toOperator(mongo::DocumentSource* source);
    std::unique_ptr<Operator> toOperator(KafkaConsumerOperator::Options options);
};

};  // namespace streams
