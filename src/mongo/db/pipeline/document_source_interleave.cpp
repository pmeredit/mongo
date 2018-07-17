/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_interleave.h"

#include "mongo/base/init.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_path_support.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/query/query_knobs.h"
#include "mongo/stdx/memory.h"

namespace mongo {

using boost::intrusive_ptr;
using std::vector;

namespace {
std::string pipelineToString(const vector<BSONObj>& pipeline) {
    StringBuilder sb;
    sb << "[";

    auto first = true;
    for (auto& stageSpec : pipeline) {
        if (!first) {
            sb << ", ";
        } else {
            first = false;
        }
        sb << stageSpec;
    }
    sb << "]";
    return sb.str();
}
}  // namespace

constexpr size_t DocumentSourceInterleave::kMaxSubPipelineDepth;

  DocumentSourceInterleave::DocumentSourceInterleave(NamespaceString fromNs,
                                             const boost::intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(pExpCtx), _fromNs(std::move(fromNs)), _getLocalFirst(true), _foreignEOF(false) {
    const auto& resolvedNamespace = pExpCtx->getResolvedNamespace(_fromNs);
    _resolvedNs = resolvedNamespace.ns;
    _resolvedPipeline = resolvedNamespace.pipeline;
    _fromExpCtx = pExpCtx->copyWith(_resolvedNs);

    _fromExpCtx->subPipelineDepth += 1;
    uassert(ErrorCodes::MaxSubPipelineDepthExceeded,
            str::stream() << "Maximum number of nested $interleave sub-pipelines exceeded. Limit is "
            << kMaxSubPipelineDepth,
            _fromExpCtx->subPipelineDepth <= kMaxSubPipelineDepth);
  }

DocumentSourceInterleave::DocumentSourceInterleave(NamespaceString fromNs,
                                           std::vector<BSONObj> pipeline,
                                           const boost::intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSourceInterleave(fromNs, pExpCtx) {
    // '_resolvedPipeline' will first be initialized by the constructor delegated to within this
    // constructor's initializer list. It will be populated with view pipeline prefix if 'fromNs'
    // represents a view. We append the user 'pipeline' to the end of '_resolvedPipeline' to ensure
    // any view prefix is not overwritten.
    _resolvedPipeline.insert(_resolvedPipeline.end(), pipeline.begin(), pipeline.end());
    _userPipeline = std::move(pipeline);
    initializeIntrospectionPipeline();
    _pipeline = uassertStatusOK(
         pExpCtx->mongoProcessInterface->makePipeline(_resolvedPipeline, _fromExpCtx));
}

std::unique_ptr<DocumentSourceInterleave::LiteParsed> DocumentSourceInterleave::LiteParsed::parse(
    const AggregationRequest& request, const BSONElement& spec) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "the $interleave stage specification must be an object, but found "
                          << typeName(spec.type()),
            spec.type() == BSONType::Object);

    auto specObj = spec.Obj();
    auto fromElement = specObj["from"];
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "missing 'from' option to $interleave stage specification: " << specObj,
            fromElement);
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "'from' option to $interleave must be a string, but was type "
                          << typeName(specObj["from"].type()),
            fromElement.type() == BSONType::String);

    NamespaceString fromNss(request.getNamespaceString().db(), fromElement.valueStringData());
    uassert(ErrorCodes::InvalidNamespace,
            str::stream() << "invalid $interleave namespace: " << fromNss.ns(),
            fromNss.isValid());

    stdx::unordered_set<NamespaceString> foreignNssSet;

    // Recursively lite parse the nested pipeline, if one exists.
    auto pipelineElem = specObj["pipeline"];
    boost::optional<LiteParsedPipeline> liteParsedPipeline;
    if (pipelineElem) {
        auto pipeline = uassertStatusOK(AggregationRequest::parsePipelineFromBSON(pipelineElem));
        AggregationRequest foreignAggReq(fromNss, std::move(pipeline));
        liteParsedPipeline = LiteParsedPipeline(foreignAggReq);

        auto pipelineInvolvedNamespaces = liteParsedPipeline->getInvolvedNamespaces();
        foreignNssSet.insert(pipelineInvolvedNamespaces.begin(), pipelineInvolvedNamespaces.end());
    }

    foreignNssSet.insert(fromNss);

    return stdx::make_unique<DocumentSourceInterleave::LiteParsed>(
        std::move(fromNss), std::move(foreignNssSet), std::move(liteParsedPipeline));
}

REGISTER_DOCUMENT_SOURCE(interleave,
                         DocumentSourceInterleave::LiteParsed::parse,
                         DocumentSourceInterleave::createFromBson);

const char* DocumentSourceInterleave::getSourceName() const {
    return "$interleave";
}

DocumentSource::GetNextResult DocumentSourceInterleave::tryGetLocal() {
    auto nextInput = pSource->getNext();
    for (; nextInput.isAdvanced(); nextInput = pSource->getNext()) {
        return nextInput;
    }
    return GetNextResult::makeEOF();
}

DocumentSource::GetNextResult DocumentSourceInterleave::tryGetForeign() {
    // Pipeline::getNext will iterate a pipeline
    // a second time, so we need to record when we reach the end to avoid
    // this behavior.
    if (_foreignEOF) {
        return GetNextResult::makeEOF();
    }
    while(auto result = _pipeline->getNext()) {
        return GetNextResult(std::move(*result));
    }
    _foreignEOF = true;
    return GetNextResult::makeEOF();
}

DocumentSource::GetNextResult DocumentSourceInterleave::getNext() {
    pExpCtx->checkForInterrupt();
    // _getLocalFirst says whether we should try to pull from the local
    // pipeline or the foreign, so that we truly interleave documents.
    if (_getLocalFirst) {
        _getLocalFirst = false;
        auto next = tryGetLocal();
        if (next.isEOF()) {
          auto ret = tryGetForeign();
          if(ret.isEOF()) doDispose();
          return ret;
        }
        return next;
    }
    else {
        _getLocalFirst = true;
        auto next = tryGetForeign();
        if (next.isEOF()) {
          auto ret = tryGetLocal();
          if(ret.isEOF()) doDispose();
          return ret;
        }
        return next;
    }
    return GetNextResult::makeEOF();
}

Pipeline::SourceContainer::iterator DocumentSourceInterleave::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    invariant(*itr == this);
    return std::next(itr);
}

std::string DocumentSourceInterleave::getUserPipelineDefinition() {
    return pipelineToString(_userPipeline);
}

void DocumentSourceInterleave::doDispose() {
    if (_pipeline) {
        _pipeline->dispose(pExpCtx->opCtx);
        _pipeline.reset();
    }
}

void DocumentSourceInterleave::initializeIntrospectionPipeline() {
    _parsedIntrospectionPipeline = uassertStatusOK(Pipeline::parse(_resolvedPipeline, _fromExpCtx));

    auto& sources = _parsedIntrospectionPipeline->getSources();

    // Ensure that the pipeline does not contain a $changeStream stage. This check will be
    // performed recursively on all sub-pipelines.
    uassert(ErrorCodes::IllegalOperation,
            "$changeStream is not allowed within a $interleave foreign pipeline",
            sources.empty() || !sources.front()->constraints().isChangeStreamStage());
}

void DocumentSourceInterleave::serializeToArray(
    std::vector<Value>& array, boost::optional<ExplainOptions::Verbosity> explain) const {
    Document doc;

    auto pipeline = _userPipeline;
    doc = Document{{getSourceName(),
                    Document{{"from", _fromNs.coll()},
                             {"pipeline", pipeline}}}};

    MutableDocument output(doc);
    array.push_back(Value(output.freeze()));
}

DepsTracker::State DocumentSourceInterleave::getDependencies(DepsTracker* deps) const {
    // We will use the introspection pipeline which we prebuilt during construction.
    invariant(_parsedIntrospectionPipeline);

    DepsTracker subDeps(deps->getMetadataAvailable());

    // Get the subpipeline dependencies. Subpipeline stages may reference
    // variables declared externally.
    for (auto&& source : _parsedIntrospectionPipeline->getSources()) {
        source->getDependencies(&subDeps);
    }

    // Add sub-pipeline variable dependencies. Do not add field dependencies, since these refer
    // to the fields from the foreign collection rather than the local collection.
    deps->vars.insert(subDeps.vars.begin(), subDeps.vars.end());
    return DepsTracker::State::SEE_NEXT;
}

void DocumentSourceInterleave::detachFromOperationContext() {
    if (_pipeline) {
        // We have a pipeline we're going to be executing across multiple calls to getNext(), so we
        // use Pipeline::detachFromOperationContext() to take care of updating '_fromExpCtx->opCtx'.
        _pipeline->detachFromOperationContext();
        invariant(_fromExpCtx->opCtx == nullptr);
    } else if (_fromExpCtx) {
        _fromExpCtx->opCtx = nullptr;
    }
}

void DocumentSourceInterleave::reattachToOperationContext(OperationContext* opCtx) {
    if (_pipeline) {
        // We have a pipeline we're going to be executing across multiple calls to getNext(), so we
        // use Pipeline::reattachToOperationContext() to take care of updating '_fromExpCtx->opCtx'.
        _pipeline->reattachToOperationContext(opCtx);
        invariant(_fromExpCtx->opCtx == opCtx);
    } else if (_fromExpCtx) {
        _fromExpCtx->opCtx = opCtx;
    }
}

intrusive_ptr<DocumentSource> DocumentSourceInterleave::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(ErrorCodes::FailedToParse,
            "the $interleave specification must be an Object",
            elem.type() == BSONType::Object);

    NamespaceString fromNs;
    std::vector<BSONObj> pipeline;
    bool hasPipeline = false;
    bool hasFrom = false;

    for (auto&& argument : elem.Obj()) {
        const auto argName = argument.fieldNameStringData();

        if (argName == "pipeline") {
            uassert(ErrorCodes::FailedToParse, str::stream() << "$interleave may have only 1 pipeline field", !hasPipeline);
            auto result = AggregationRequest::parsePipelineFromBSON(argument);
            if (!result.isOK()) {
                uasserted(ErrorCodes::FailedToParse,
                          str::stream() << "invalid $interleave pipeline definition: "
                                        << result.getStatus().toString());
            }
            pipeline = std::move(result.getValue());
            hasPipeline = true;
            continue;
        }

        uassert(ErrorCodes::FailedToParse,
                str::stream() << "$interleave argument '" << argument << "' must be a string, is type "
                              << argument.type(),
                argument.type() == BSONType::String);

        if (argName == "from") {
            uassert(ErrorCodes::FailedToParse, str::stream() << "$interleave may have only 1 from field", !hasFrom);
            fromNs = NamespaceString(pExpCtx->ns.db().toString() + '.' + argument.String());
            hasFrom = true;
        } else {
            uasserted(ErrorCodes::FailedToParse,
                      str::stream() << "unknown argument to $interleave: " << argument.fieldName());
        }
    }
    uassert(ErrorCodes::FailedToParse, str::stream() << "$interleave must have pipeline field", hasPipeline);
    uassert(ErrorCodes::FailedToParse, str::stream() << "$interleave must have from field", hasFrom);
    return new DocumentSourceInterleave(std::move(fromNs),
                                    std::move(pipeline),
                                    pExpCtx);
}
}
