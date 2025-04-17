use bson::{RawBsonRef, RawDocument, RawDocumentBuf};

use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageProperties, Error,
    SourceAggregationStageDescriptor, SourceBoundAggregationStageDescriptor,
};
use crate::{AggregationStage, GetNextResult};

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

impl AggregationStage for EchoOxideExecutor {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        Ok(if let Some(doc) = self.0.take() {
            GetNextResult::Advanced(doc.into())
        } else {
            GetNextResult::EOF
        })
    }
}
