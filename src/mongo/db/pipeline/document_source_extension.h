#include "absl/base/nullability.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/plugin/api.h"

namespace mongo {

// A DocumentSource implementation for an extension aggregation stage.
class DocumentSourceExtension : public DocumentSource {
public:
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

    StageConstraints constraints(Pipeline::SplitState pipeState) const override;

    Value serialize(const SerializationOptions& opts) const override {
        // TODO We need to call into the plugin here when we want to serialize for query shape, or
        // if optimizations change the shape of the stage definition.
        return Value(_raw_stage);
    }

    // This method is invoked by extensions via MongoExtensionPortal.
    // TODO: make this private.
    static void registerStageDescriptor(MongoExtensionByteView name,
                                        const MongoExtensionAggregationStageDescriptor* descriptor);

private:
    struct ExtensionObjectDeleter {
        template <typename T>
        void operator()(T* obj) {
            obj->vtable->drop(obj);
        }
    };
    using BoundDescriptorPtr =
        std::unique_ptr<MongoExtensionBoundAggregationStageDescriptor, ExtensionObjectDeleter>;
    using ByteBufPtr = std::unique_ptr<MongoExtensionByteBuf, ExtensionObjectDeleter>;

    struct PluginStageDeleter {
        void operator()(MongoExtensionAggregationStage* stage) {
            stage->vtable->close(stage);
        }
    };
    using ExecutorPtr = std::unique_ptr<MongoExtensionAggregationStage, PluginStageDeleter>;

    static void registerConcreteStage(
        std::string name,
        DocumentSource::Id id,
        absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> descriptor);

    static void registerDesugarStage(
        std::string name,
        DocumentSource::Id id,
        absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> descriptor);

    DocumentSourceExtension(
        StringData name,
        boost::intrusive_ptr<ExpressionContext> exprCtx,
        Id id,
        BSONObj rawStage,
        absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> descriptor,
        BoundDescriptorPtr boundDescriptor,
        ExecutorPtr executor)
        : DocumentSource(name, exprCtx),
          _stage_name(name.toString()),
          _id(id),
          _raw_stage(rawStage.getOwned()),
          _descriptor(descriptor),
          _boundDescriptor(std::move(boundDescriptor)),
          _executor(std::move(executor)) {}

    // Do not support copy or move.
    DocumentSourceExtension(const DocumentSourceExtension&) = delete;
    DocumentSourceExtension(DocumentSourceExtension&&) = delete;
    DocumentSourceExtension& operator=(const DocumentSourceExtension&) = delete;
    DocumentSourceExtension& operator=(DocumentSourceExtension&&) = delete;

    // FFI binding for extension to use.
    static int externalSourceGetNext(void* source_ptr, const unsigned char** result, size_t* len);
    int sourceGetNext(const unsigned char** result, size_t* len);

    // NB: the stage name could be stored statically, but I would have to add it to the C plugin
    // interface. I could re-use the name that is passed to the constructor but that is sketchy
    // because StringData/string_view is not guaranteed to contain a null terminator and we are
    // returning the value as a const char* in getSourceName().
    std::string _stage_name;
    Id _id;
    // TODO: If optimizing this stage, _raw_stage will need to be updated with the new stage
    // definition, so that the optimized request is sent to shards.
    BSONObj _raw_stage;
    BSONObj _source_doc;
    absl::Nonnull<const MongoExtensionAggregationStageDescriptor*> _descriptor;
    BoundDescriptorPtr _boundDescriptor;
    ExecutorPtr _executor;
};

}  // namespace mongo
