#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/parser.h"
#include "streams/exec/source_stage_gen.h"
#include "streams/exec/window_operator.h"
#include "streams/exec/window_pipeline.h"

namespace streams {

class DocumentSourceWindowStub : public mongo::DocumentSource {

public:
    constexpr static auto kStageName = "$tumblingWindow"_sd;

    mongo::BSONObj bsonOptions() {
        return _bsonOptions;
    }

    DocumentSourceWindowStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                             mongo::BSONObj bsonOptions)
        : DocumentSource(getSourceName(), expCtx), _bsonOptions(bsonOptions) {}

    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

protected:
    const char* getSourceName() const override {
        return kStageName.rawData();
    }

    mongo::StageConstraints constraints(mongo::Pipeline::SplitState pipeState) const override;

    void addVariableRefs(std::set<mongo::Variables::Id>* refs) const final override {
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
        mongo::SerializationOptions opts = mongo::SerializationOptions()) const override {
        MONGO_UNREACHABLE;
    }

private:
    mongo::BSONObj _bsonOptions;
};

}  // namespace streams
