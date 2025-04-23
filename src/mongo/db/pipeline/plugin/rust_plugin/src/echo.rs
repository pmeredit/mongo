//! Sample implementation of a custom source stage.
//!
//! `$echoOxide` is crude reimplementation of the builtin stage `$documents`. It accepts a single
//! document as the stage definition and produces that document exactly once.

use bson::{RawBsonRef, RawDocument, RawDocumentBuf};

use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageExecutor,
    AggregationStageProperties, Error, GetNextResult, SourceAggregationStageDescriptor,
    SourceBoundAggregationStageDescriptor,
};

pub struct EchoOxideDescriptor;

impl AggregationStageDescriptor for EchoOxideDescriptor {
    fn name() -> &'static str {
        "$echoOxide"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::First,
            host_type: stage_constraints::HostTypeRequirement::LocalOnly,
            can_run_on_shards_pipeline: false,
        }
    }
}

impl SourceAggregationStageDescriptor for EchoOxideDescriptor {
    type BoundDescriptor = EchoOxideBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$echoOxide stage definition must contain a document.",
                ))
            }
        };
        Ok(EchoOxideBoundDescriptor(document))
    }
}

pub struct EchoOxideBoundDescriptor(RawDocumentBuf);

impl SourceBoundAggregationStageDescriptor for EchoOxideBoundDescriptor {
    type Executor = EchoOxideExecutor;

    fn create_executor(&self) -> Result<Self::Executor, Error> {
        Ok(EchoOxideExecutor(Some(self.0.clone())))
    }
}

pub struct EchoOxideExecutor(Option<RawDocumentBuf>);

impl AggregationStageExecutor for EchoOxideExecutor {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        Ok(if let Some(doc) = self.0.take() {
            GetNextResult::Advanced(doc.into())
        } else {
            GetNextResult::EOF
        })
    }
}
