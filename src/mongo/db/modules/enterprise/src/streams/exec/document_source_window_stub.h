/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class ExpressionContext;
}
namespace streams {

class DocumentSourceWindowStub : public mongo::DocumentSource {
public:
    mongo::BSONObj bsonOptions() {
        return _bsonOptions;
    }

    DocumentSourceWindowStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                             mongo::StringData stageName,
                             mongo::BSONObj bsonOptions)
        : DocumentSource(stageName, expCtx), _bsonOptions(bsonOptions) {}

    static const Id& id;

    Id getId() const override {
        return id;
    }

protected:
    mongo::StageConstraints constraints(mongo::Pipeline::SplitState pipeState) const override;

    void addVariableRefs(std::set<mongo::Variables::Id>* refs) const final {
        // From the document_source.h file:
        //  Populate 'refs' with the variables referred to by this stage, including user and system
        //  variables but excluding $$ROOT. Note that field path references are not considered
        //  variables.
        // This is a no-op, as we don't use any user or system variables.
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        uasserted(mongo::ErrorCodes::NotImplemented,
                  fmt::format("distributedPlanLogic is not implemented for {}", getSourceName()));
    }

    GetNextResult doGetNext() override {
        uasserted(mongo::ErrorCodes::NotImplemented,
                  fmt::format("doGetNext is not implemented for {}, it can only be used in Atlas "
                              "Stream Processing.",
                              getSourceName()));
    }

    mongo::Value serialize(
        const mongo::SerializationOptions& opts = mongo::SerializationOptions{}) const override {
        uasserted(mongo::ErrorCodes::NotImplemented,
                  fmt::format("serialize is not implemented for {}", getSourceName()));
    }

    mongo::DepsTracker::State getDependencies(mongo::DepsTracker* deps) const override {
        // Returns no dependency here. The actual dependency checking will be implemented by the
        // stages in the window pipeline.
        return mongo::DepsTracker::State::EXHAUSTIVE_ALL;
    }

    GetModPathsReturn getModifiedPaths() const override {
        // Returns no dependency here. The actual dependency checking will be implemented by the
        // stages in the window pipeline.
        return {GetModPathsReturn::Type::kFiniteSet, mongo::OrderedPathSet{}, {}};
    }

private:
    mongo::BSONObj _bsonOptions;
};

class DocumentSourceTumblingWindowStub final : public DocumentSourceWindowStub {
public:
    constexpr static char kStageName[] = "$tumblingWindow";

    DocumentSourceTumblingWindowStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                     mongo::BSONObj bsonOptions)
        : DocumentSourceWindowStub(expCtx, kStageName, bsonOptions) {}


    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

protected:
    const char* getSourceName() const override {
        return kStageName;
    }
};

class DocumentSourceHoppingWindowStub final : public DocumentSourceWindowStub {
public:
    constexpr static char kStageName[] = "$hoppingWindow";

    DocumentSourceHoppingWindowStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                    mongo::BSONObj bsonOptions)
        : DocumentSourceWindowStub(expCtx, kStageName, bsonOptions) {}

    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

protected:
    const char* getSourceName() const override {
        return kStageName;
    }
};

class DocumentSourceSessionWindowStub final : public DocumentSourceWindowStub {
public:
    constexpr static char kStageName[] = "$sessionWindow";

    DocumentSourceSessionWindowStub(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                    mongo::BSONObj bsonOptions)
        : DocumentSourceWindowStub(expCtx, kStageName, bsonOptions) {}


    static std::list<boost::intrusive_ptr<mongo::DocumentSource>> createFromBson(
        mongo::BSONElement elem, const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx);

protected:
    const char* getSourceName() const override {
        return kStageName;
    }
};

}  // namespace streams
