#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/constants.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include <unordered_map>

namespace streams {

class OperatorFactory {
public:
    void validateByName(const std::string& name);
    std::unique_ptr<Operator> toOperator(mongo::DocumentSource* source);
};

};  // namespace streams
