#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {
class ExpressionContext;
}

namespace streams {

class DocumentSourceValidateStub : public mongo::DocumentSource {
public:
    constexpr static char kStageName[] = "$validate";

    mongo::BSONObj bsonOptions() {
        return _bsonOptions;
    }

    DocumentSourceValidateStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                               mongo::BSONObj bsonOptions)
        : DocumentSource(kStageName, expCtx), _bsonOptions(bsonOptions) {}

    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

protected:
    const char* getSourceName() const override {
        return kStageName;
    }

    mongo::StageConstraints constraints(mongo::Pipeline::SplitState pipeState) const override;

    void addVariableRefs(std::set<mongo::Variables::Id>* refs) const final {
        // From the document_source.h file:
        //  Populate 'refs' with the variables referred to by this stage, including user and system
        //  variables but excluding $$ROOT. Note that field path references are not considered
        //  variables.
        // This is a no-op, as we don't use any user or system variables.
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        MONGO_UNREACHABLE;
    }

    GetNextResult doGetNext() override {
        MONGO_UNREACHABLE;
    }

    mongo::Value serialize(
        const mongo::SerializationOptions& opts = mongo::SerializationOptions{}) const override {
        MONGO_UNREACHABLE;
    }

private:
    mongo::BSONObj _bsonOptions;
};

}  // namespace streams
