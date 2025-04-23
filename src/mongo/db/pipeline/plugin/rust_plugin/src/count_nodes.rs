/**
 * The $countNodes stage will generate a single document with the count of how many data-bearing
 * nodes were involved in executing this query. This is used as a toy example for testing
 * $betaMultiStream, where the secondary stream is expected to produce a single document to populate
 * the $$SEARCH_META variable.
 */
use bson::{doc, to_raw_document_buf, Bson::Null, Document, RawBsonRef, RawDocument};

use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageExecutor,
    AggregationStageProperties, Error, GetNextResult, SourceAggregationStageDescriptor,
    SourceBoundAggregationStageDescriptor,
};

pub struct CountNodesDescriptor;

impl AggregationStageDescriptor for CountNodesDescriptor {
    fn name() -> &'static str {
        "$countNodes"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::First,
            host_type: stage_constraints::HostTypeRequirement::None,
            can_run_on_shards_pipeline: true,
        }
    }
}

impl SourceAggregationStageDescriptor for CountNodesDescriptor {
    type BoundDescriptor = CountNodesBoundDescriptor;

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
                    "$countNodes stage definition must contain a document.",
                ))
            }
        };
        match document.is_empty() {
            true => Ok(CountNodesBoundDescriptor),
            false => Err(Error::new(
                1,
                "$countNodes stage definition must be an empty document.",
            )),
        }
    }
}

pub struct CountNodesBoundDescriptor;

impl SourceBoundAggregationStageDescriptor for CountNodesBoundDescriptor {
    type Executor = CountNodesExecutor;

    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        Ok(vec![
            doc! {"$group": {"_id": Null, "count": {"$sum": "$count"}}},
            doc! {"$project": {"_id": 0}},
        ])
    }

    fn create_executor(&self) -> Result<Self::Executor, Error> {
        Ok(CountNodesExecutor(true))
    }
}

pub struct CountNodesExecutor(bool);

impl AggregationStageExecutor for CountNodesExecutor {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        // On the first call, returns {count: 1}; on the second call, returns EOF.
        Ok(if self.0 {
            self.0 = false;
            GetNextResult::Advanced(to_raw_document_buf(&doc! {"count": 1}).unwrap().into())
        } else {
            GetNextResult::EOF
        })
    }
}
