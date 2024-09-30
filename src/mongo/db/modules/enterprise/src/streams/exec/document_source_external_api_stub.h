#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/document_source.h"

namespace mongo {
class ExpressionContext;
}

namespace streams {

class DocumentSourceExternalApiStub : public mongo::DocumentSource {
public:
    constexpr static char kStageName[] = "$externalAPI";

    DocumentSourceExternalApiStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                  mongo::BSONObj bsonOptions)
        : DocumentSource(kStageName, expCtx), _bsonOptions(bsonOptions) {}

    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

    mongo::DocumentSourceType getType() const override {
        return mongo::DocumentSourceType::kExternalApi;
    }

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
        uasserted(
            mongo::ErrorCodes::NotImplemented,
            fmt::format(
                "distributedPlanLogic is not implemented for {}, it can only be used in Atlas "
                "Stream Processing.",
                getSourceName()));
    }

    GetNextResult doGetNext() override {
        uasserted(mongo::ErrorCodes::NotImplemented,
                  fmt::format("doGetNext is not implemented for {}, it can only be used in Atlas "
                              "Stream Processing.",
                              getSourceName()));
    }

    mongo::Value serialize(
        const mongo::SerializationOptions& opts = mongo::SerializationOptions{}) const override;

private:
    mongo::BSONObj _bsonOptions;
};

}  // namespace streams
