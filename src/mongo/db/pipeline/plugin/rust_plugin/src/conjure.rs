//! Sample implementation of custom transform and de-sugaring stages.
//!
//! * *Transform*: [`AddSomeConjureDescriptor`] implements `$conjure`.
//! * *Desugar*: [`ConjureDescriptor`] implements `$echoWithSomeCrabs`.

use crate::echo::EchoOxideDescriptor;
use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageExecutor,
    AggregationStageProperties, DesugarAggregationStageDescriptor, Error, GetNextResult,
    HostAggregationStageExecutor, TransformAggregationStageDescriptor,
    TransformBoundAggregationStageDescriptor,
};

use ast::definitions::{Pipeline, Stage};
use babelfish::*;
use bson::{doc, to_raw_document_buf, Document, Bson, RawBsonRef, RawDocument};
use serde::{Deserialize, Deserializer, Serialize};

/// Implements the `$echoWithSomeConjure` de-sugared stage.
///
/// The stage definition is of the form:
/// ```
/// $echoWithSomeConjure: {
///   document: <bson doc>
///   numConjure: <non-negative number>
/// }
/// ```
///
/// This will de-sugar into an `$echoOxide, $conjure` sequence, although the latter may be
/// omitted if `numConjure: 0`.
pub struct ConjureDescriptor;

impl AggregationStageDescriptor for ConjureDescriptor {
    fn name() -> &'static str {
        "$conjure"
    }

    fn properties(&self) -> AggregationStageProperties {
        EchoOxideDescriptor.properties()
    }
}

impl DesugarAggregationStageDescriptor for ConjureDescriptor {
    fn desugar(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Vec<Document>, Error> {
        let input_stage_def: ConjureStageDefinition =
            bson::from_bson(stage_definition.try_into().map_err(|e| {
                Error::with_source(1, "Could not convert $conjure to bson", e)
            })?)
            .map_err(|e| Error::with_source(1, "Could not parse $conjure", e))?;
        let pipeline = Pipeline {
            pipeline: vec![Stage::Conjure(input_stage_def.0)],
        };
        let pipeline = conjure_rewrite::rewrite_pipeline(pipeline)
            .map_err(|e| Error::with_source(1, "Could not rewrite $conjure", e))?;
        let pipeline = join_rewrite::rewrite_pipeline(pipeline)
            .map_err(|e| Error::with_source(1, "Could not rewrite $join", e))?;
        let pipeline = match_movement_rewrite::rewrite_match_move(pipeline);
        let stages = pipeline.pipeline
            .into_iter()
            .map(|stage| bson::to_document(&stage))
            .collect::<Result<Vec<Document>, _>>()
            .map_err(|e| Error::with_source(1, "Could not convert stage to bson", e))?;
        Ok(stages)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConjureStageDefinition(Vec<String>);
