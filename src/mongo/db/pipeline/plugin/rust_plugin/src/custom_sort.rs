use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageProperties, Error,
    HostAggregationStageExecutor, TransformAggregationStageDescriptor,
    TransformBoundAggregationStageDescriptor,
};
use crate::{AggregationStage, GetNextResult};
use bson::{doc, to_raw_document_buf};
use bson::{Document, RawBsonRef, RawDocument};
use std::collections::VecDeque;

pub struct PluginSortDescriptor;

impl AggregationStageDescriptor for PluginSortDescriptor {
    fn name() -> &'static str {
        "$pluginSort"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Blocking,
            position: stage_constraints::PositionRequirement::None,
            host_type: stage_constraints::HostTypeRequirement::None,
            can_run_on_shards_pipeline: true
        }
    }
}

impl TransformAggregationStageDescriptor for PluginSortDescriptor {
    type BoundDescriptor = PluginSortBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        let field = match stage_definition {
            RawBsonRef::String(i) => i.to_string(),
            _ => {
                return Err(Error::new(
                    1,
                    "$pluginSort should be followed with a string.",
                ))
            }
        };
        Ok(PluginSortBoundDescriptor(field))
    }
}

pub struct PluginSortBoundDescriptor(String);

impl TransformBoundAggregationStageDescriptor for PluginSortBoundDescriptor {
    type Executor = PluginSort;

    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        Ok(vec![doc! {"$sort": {self.0.clone(): 1}}])
    }

    fn create_executor(
        &self,
        source: HostAggregationStageExecutor,
    ) -> Result<Self::Executor, Error> {
        Ok(PluginSort::from_bound_descriptor(self, source))
    }
}

pub struct PluginSort {
    field: String,
    source: HostAggregationStageExecutor,
    docs: Option<VecDeque<Document>>,
}

impl PluginSort {
    fn from_bound_descriptor(
        descriptor: &PluginSortBoundDescriptor,
        source: HostAggregationStageExecutor,
    ) -> Self {
        PluginSort {
            field: descriptor.0.clone(),
            source,
            docs: None,
        }
    }
}

/**
 * $pluginSort accepts one field by which to sort the document; the field values must be numeric.
 */
impl AggregationStage for PluginSort {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.docs.is_none() {
            Self::populate_and_sort_documents(self)?;
        }

        match self.docs.as_mut() {
            Some(documents) => {
                if documents.is_empty() {
                    return Ok(GetNextResult::EOF);
                }

                let next = documents.pop_front().unwrap();
                Ok(GetNextResult::Advanced(
                    to_raw_document_buf(&next).unwrap().into(),
                ))
            }
            None => Ok(GetNextResult::EOF),
        }
    }
}

impl PluginSort {
    fn populate_and_sort_documents(&mut self) -> Result<(), Error> {
        // Retrieve all results from the source stage.
        let mut documents = VecDeque::new();
        while let GetNextResult::Advanced(input_doc) = self.source.get_next()? {
            let doc = Document::try_from(input_doc.as_ref()).unwrap();
            documents.push_back(doc);
        }

        // Sort the documents by the provided field.
        documents.make_contiguous().sort_by(|doc_a, doc_b| {
            // TODO Properly handle errors when field has non-numeric value.
            // TODO This could be more efficient by retrieving and converting the values in advance..
            let val_a = doc_a.get(self.field.clone()).unwrap().as_f64().unwrap();
            let val_b = doc_b.get(self.field.clone()).unwrap().as_f64().unwrap();
            val_a.total_cmp(&val_b)
        });

        self.docs = Some(documents);
        Ok(())
    }
}
