/**
 * Copyright (C) 2016 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_source_sequential_document_cache.h"
#include "mongo/db/pipeline/document_source_unwind.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"
#include "mongo/db/pipeline/lookup_set_cache.h"
#include "mongo/db/pipeline/value_comparator.h"

namespace mongo {

/**
 * Queries separate collection for equality matches with documents in the pipeline collection.
 * Adds matching documents to a new array field in the input document.
 */
class DocumentSourceInterleave final : public DocumentSource {
public:
    static constexpr size_t kMaxSubPipelineDepth = 1000;

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec);

        LiteParsed(NamespaceString fromNss,
                   stdx::unordered_set<NamespaceString> foreignNssSet,
                   boost::optional<LiteParsedPipeline> liteParsedPipeline)
            : _fromNss{std::move(fromNss)},
              _foreignNssSet(std::move(foreignNssSet)),
              _liteParsedPipeline(std::move(liteParsedPipeline)) {}

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return {_foreignNssSet};
        }

        PrivilegeVector requiredPrivileges(bool isMongos) const final {
            PrivilegeVector requiredPrivileges;
            Privilege::addPrivilegeToPrivilegeVector(
                &requiredPrivileges,
                Privilege(ResourcePattern::forExactNamespace(_fromNss), ActionType::find));

            if (_liteParsedPipeline) {
                Privilege::addPrivilegesToPrivilegeVector(
                    &requiredPrivileges, _liteParsedPipeline->requiredPrivileges(isMongos));
            }

            return requiredPrivileges;
        }

    private:
        const NamespaceString _fromNss;
        const stdx::unordered_set<NamespaceString> _foreignNssSet;
        const boost::optional<LiteParsedPipeline> _liteParsedPipeline;
    };

    GetNextResult getNext() final;
    const char* getSourceName() const final;
    void serializeToArray(
        std::vector<Value>& array,
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        const bool mayUseDisk = std::any_of(_parsedIntrospectionPipeline->getSources().begin(),
                        _parsedIntrospectionPipeline->getSources().end(),
                        [](const auto& source) {
                            return source->constraints().diskRequirement ==
                                DiskUseRequirement::kWritesTmpData;
                        });

        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kPrimaryShard,
                                     mayUseDisk ? DiskUseRequirement::kWritesTmpData
                                                : DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kAllowed,
                                     TransactionRequirement::kAllowed);

        constraints.canSwapWithMatch = true;
        return constraints;
    }

    DepsTracker::State getDependencies(DepsTracker* deps) const final;

    BSONObjSet getOutputSorts() final {
        return pSource ? pSource->getOutputSorts()
          : SimpleBSONObjComparator::kInstance.makeBSONObjSet();
    }

    void addInvolvedCollections(std::vector<NamespaceString>* collections) const final {
        collections->push_back(_fromNs);
    }

    void detachFromOperationContext() final;

    void reattachToOperationContext(OperationContext* opCtx) final;

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    static boost::intrusive_ptr<DocumentSource> createFromBsonWithCacheSize(
        BSONElement elem,
        const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
        size_t maxCacheSizeBytes) {
        return createFromBson(elem, pExpCtx);
    }

protected:
    void doDispose() final;

    /**
     * Attempts to combine with a subsequent $unwind stage, setting the internal '_unwindSrc'
     * field.
     */
    Pipeline::SourceContainer::iterator doOptimizeAt(Pipeline::SourceContainer::iterator itr,
                                                     Pipeline::SourceContainer* container) final;

private:
    /**
     * Helpers for pulling from local and foreign pipelines in order
     */
    GetNextResult tryGetLocal();
    GetNextResult tryGetForeign();

    /**
     * The foreign pipeline
     */
    std::unique_ptr<Pipeline, PipelineDeleter> _pipeline; 

    /**
     * Constructor used for a base $interleave stage
     */
    DocumentSourceInterleave(NamespaceString fromNs,
                             const boost::intrusive_ptr<ExpressionContext>& pExpCtx);
    /**
     * Constructor used for a $interleave stage specified with a pipeline
     */
    DocumentSourceInterleave(NamespaceString fromNs,
                         std::vector<BSONObj> pipeline,
                         const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    /**
     * Should not be called; use serializeToArray instead.
     */
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final {
        MONGO_UNREACHABLE;
    }

    /**
     * Builds a parsed pipeline for introspection (e.g. constraints, dependencies). Any sub-$interleave
     * pipelines will be built recursively.
     */
    void initializeIntrospectionPipeline();

    /**
     * The pipeline supplied via the $interleave 'pipeline' argument. This may differ from pipeline that
     * is executed in that it will not include optimizations or resolved views.
     */
    std::string getUserPipelineDefinition();

    NamespaceString _fromNs;
    NamespaceString _resolvedNs;
    boost::optional<BSONObj> _additionalFilter;

    // Records which pipeline we should try to pull docs from first.
    bool _getLocalFirst;

    // Records whether we have seen the end of the foreign pipeline because the
    // Pipeline::getNext method wraps around after reaching the end.
    bool _foreignEOF;

    // The ExpressionContext used when performing aggregation pipelines against the '_resolvedNs'
    // namespace.
    boost::intrusive_ptr<ExpressionContext> _fromExpCtx;

    // The aggregation pipeline to perform against the '_resolvedNs' namespace. Referenced view
    // namespaces have been resolved. This pipeline will be the prefix we need to prepend
    // to the user pipeline if the from collection is a view.
    std::vector<BSONObj> _resolvedPipeline;
    // The aggregation pipeline defined with the user request, prior to optimization and view
    // resolution. This will be appended to the _resolvedPipeline.
    std::vector<BSONObj> _userPipeline;
    // A pipeline parsed from _resolvedPipeline at creation time, intended to support introspective
    // functions. If sub-$interleave stages are present, their pipelines are constructed recursively.
    std::unique_ptr<Pipeline, PipelineDeleter> _parsedIntrospectionPipeline;
};

}  // namespace mongo
