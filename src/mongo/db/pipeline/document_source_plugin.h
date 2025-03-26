#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/plugin/api.h"

namespace mongo {

class DocumentSourcePlugin : public DocumentSource {
public:
    DocumentSourcePlugin(StringData name,
                         boost::intrusive_ptr<ExpressionContext> exprCtx,
                         Id id,
                         MongoExtensionAggregationStage* stage,
                         BSONObj rawStage)
        : DocumentSource(name, exprCtx),
          _stage_name(name.toString()),
          _id(id),
          _plugin_stage(stage),
          _raw_stage(rawStage.getOwned()) {}

    const char* getSourceName() const override {
        return _stage_name.c_str();
    }

    Id getId() const override {
        return _id;
    }

    void setSource(DocumentSource* source) override;

    GetNextResult doGetNext() override;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override;

    void addVariableRefs(std::set<Variables::Id>* refs) const override {}

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        return StageConstraints(StreamType::kStreaming,
                                PositionRequirement::kNone,
                                HostTypeRequirement::kAnyShard,
                                DiskUseRequirement::kNoDiskUse,
                                FacetRequirement::kNotAllowed,
                                TransactionRequirement::kNotAllowed,
                                LookupRequirement::kNotAllowed,
                                UnionRequirement::kNotAllowed,
                                ChangeStreamRequirement::kDenylist);
    }

    Value serialize(const SerializationOptions& opts) const override {
        // TODO We need to call into the plugin here when we want to serialize for query shape, or
        // if optimizations change the shape of the stage definition.
        return Value(_raw_stage);
    }

private:
    // Do not support copy or move.
    DocumentSourcePlugin(const DocumentSourcePlugin&) = delete;
    DocumentSourcePlugin(DocumentSourcePlugin&&) = delete;
    DocumentSourcePlugin& operator=(const DocumentSourcePlugin&) = delete;
    DocumentSourcePlugin&& operator=(DocumentSourcePlugin&&) = delete;

    friend int source_get_next(void* source_ptr, const unsigned char** result, size_t* len);

    struct PluginStageDeleter {
        void operator()(MongoExtensionAggregationStage* stage) {
            stage->vtable->close(stage);
        }
    };

    int sourceGetNext(const unsigned char** result, size_t* len);

    // NB: the stage name could be stored statically, but I would have to add it to the C plugin
    // interface. I could re-use the name that is passed to the constructor but that is sketchy
    // because StringData/string_view is not guaranteed to contain a null terminator and we are
    // returning the value as a const char* in getSourceName().
    std::string _stage_name;
    Id _id;
    std::unique_ptr<MongoExtensionAggregationStage, PluginStageDeleter> _plugin_stage;
    // TODO: If optimizing this stage, _raw_stage will need to be updated with the new stage
    // definition, so that the optimized request is sent to shards.
    BSONObj _raw_stage;
    BSONObj _source_doc;
};

}  // namespace mongo