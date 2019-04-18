/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "fle_pipeline.h"

#include "mongo/db/pipeline/document_source_limit.h"

namespace mongo {

namespace {

clonable_ptr<EncryptionSchemaTreeNode> propagateSchemaForStage(
    const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
    const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& children,
    const DocumentSourceLimit& source) {
    return prevSchema->clone();
}

// The 'schemaPropagatorMap' is a map of the typeid of a concrete DocumentSource class to the
// appropriate dispatch function.
static stdx::unordered_map<
    std::type_index,
    std::function<clonable_ptr<EncryptionSchemaTreeNode>(
        const clonable_ptr<EncryptionSchemaTreeNode>& prevSchema,
        const std::vector<clonable_ptr<EncryptionSchemaTreeNode>>& subPipelineSchemas,
        const DocumentSource& source)>>
    schemaPropagatorMap;

#define REGISTER_DOCUMENT_SOURCE_PROPAGATOR(className)                                       \
    MONGO_INITIALIZER(schemaPropagateFor_##className)(InitializerContext*) {                 \
        invariant(schemaPropagatorMap.find(typeid(className)) == schemaPropagatorMap.end()); \
        schemaPropagatorMap[typeid(className)] =                                             \
            [&](const auto& prevSchema,                                                      \
                const auto& subPipelineSchemas,                                              \
                const auto& source) -> clonable_ptr<EncryptionSchemaTreeNode> {              \
            return propagateSchemaForStage(                                                  \
                prevSchema, subPipelineSchemas, static_cast<const className&>(source));      \
        };                                                                                   \
        return Status::OK();                                                                 \
    }

// Whitelisted set of DocumentSource classes which are supported and/or require action for
// encryption. Adding to this list assumes that there exists an appropriate overload for the
// 'propagateSchemaForStage' method.
REGISTER_DOCUMENT_SOURCE_PROPAGATOR(DocumentSourceLimit);

}  // namespace

FLEPipeline::FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                         std::unique_ptr<EncryptionSchemaTreeNode> schema)
    : FLEPipeline([
          schema{std::move(schema)},
          _parsedPipeline{std::move(pipeline)}
      ]()->MetadataTreeWithFinalSchema {
          // Method for propagating a schema from one stage to the next by dynamically
          // dispatching based on the runtime-type of 'source'. The 'prevSchema' represents
          // the schema of the document flowing into 'source', and the 'subPipelineSchemas'
          // represent the input schemas of any sub-pipelines for the given source.
          const auto& propagateSchemaFunction = [&](
              const auto& prevSchema, const auto& subPipelineSchemas, const auto& source) {
              uassert(31011,
                      str::stream() << "Aggregation stage " << source.getSourceName()
                                    << " is not allowed or supported with encryption.",
                      schemaPropagatorMap.find(typeid(source)) != schemaPropagatorMap.end());
              return schemaPropagatorMap[typeid(source)](prevSchema, subPipelineSchemas, source);
          };

          return pipeline_metadata_tree::makeTree<clonable_ptr<EncryptionSchemaTreeNode>>(
              {schema->clone()}, *_parsedPipeline.get(), propagateSchemaFunction);
      }()) {}

}  // namespace mongo
