use std::collections::VecDeque;
use std::sync::Arc;

use bson::{doc, to_raw_document_buf, RawArray, RawDocument};
use bson::{Document, RawBsonRef};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageProperties, Error,
    HostAggregationStageExecutor, TransformAggregationStageDescriptor,
    TransformBoundAggregationStageDescriptor,
};
use crate::{AggregationStage, GetNextResult, LazyRuntime};

static VOYAGE_API_URL: &str = "https://api.voyageai.com/v1/rerank";
static VOYAGE_SCORE_FIELD: &str = "$voyageRerankScore";

// TODO: consider requiring descriptors are Arc wrapped so that they may be easily carried to
// the descriptor or an executor. Alternatives to the current shape and Arc wrapper:
// * Rc. Host doesn't provide many guarantees about threading so unwise.
// * Reference to descriptor and lifetime. This would bleed into the trait interfaces.
// * Raw pointers and an initialization function. Descriptor would need explicit initialization.
struct RerankState {
    runtime: LazyRuntime,
    api_key: String,
}

impl RerankState {
    pub fn runtime(&self) -> &Runtime {
        self.runtime.get()
    }
}

pub struct VoyageRerankDescriptor(Arc<RerankState>);

impl VoyageRerankDescriptor {
    pub fn new(runtime_threads: usize, api_key: String) -> Self {
        Self(Arc::new(RerankState {
            runtime: LazyRuntime::new("search-extension-voyage", runtime_threads),
            api_key,
        }))
    }
}

impl AggregationStageDescriptor for VoyageRerankDescriptor {
    fn name() -> &'static str {
        "$voyageRerank"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Blocking,
            position: stage_constraints::PositionRequirement::None,
            host_type: stage_constraints::HostTypeRequirement::Router,
        }
    }
}

impl TransformAggregationStageDescriptor for VoyageRerankDescriptor {
    type BoundDescriptor = VoyageRerankBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        VoyageRerankBoundDescriptor::new(Arc::clone(&self.0), stage_definition)
    }
}

#[derive(Clone)]
pub struct VoyageRerankBoundDescriptor {
    state: Arc<RerankState>,
    query: String,
    fields: Vec<String>,
    model: String,
    limit: i64,
}

impl VoyageRerankBoundDescriptor {
    fn new(state: Arc<RerankState>, stage_definition: RawBsonRef<'_>) -> Result<Self, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$voyageRerank stage definition must contain a document.",
                ))
            }
        };

        let query = document
            .get_str("query")
            .map_err(|_| Error::new(1, "Missing 'query' field"))?
            .to_owned();

        let model = document
            .get_str("model")
            .map_err(|_| Error::new(1, "Missing 'model' field"))?
            .to_string();

        let fields_arr: &RawArray = document
            .get_array("fields")
            .map_err(|_| Error::new(1, "Missing 'fields' field"))?;

        let mut fields = Vec::new();
        for field in fields_arr {
            fields.push(
                field
                    .unwrap()
                    .as_str()
                    .expect("fields should contain strings")
                    .to_string(),
            );
        }

        let limit = document
            .get_f64("limit")
            .map_err(|_| Error::new(1, "Missing 'limit' field"))? as i64;

        Ok(Self {
            state,
            query,
            fields,
            model,
            limit,
        })
    }
}

impl TransformBoundAggregationStageDescriptor for VoyageRerankBoundDescriptor {
    type Executor = VoyageRerank;

    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        // TODO This stage should be forced to run on the merging half of the pipeline.
        Ok(vec![])
    }

    fn create_executor(
        &self,
        source: HostAggregationStageExecutor,
    ) -> Result<Self::Executor, Error> {
        Ok(VoyageRerank::with_descriptor(self.clone(), source))
    }
}

pub struct VoyageRerank {
    descriptor: VoyageRerankBoundDescriptor,
    source: HostAggregationStageExecutor,
    documents: Option<VecDeque<Document>>,
}

impl VoyageRerank {
    fn with_descriptor(
        descriptor: VoyageRerankBoundDescriptor,
        source: HostAggregationStageExecutor,
    ) -> Self {
        Self {
            descriptor,
            source,
            documents: None,
        }
    }
}

impl AggregationStage for VoyageRerank {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.documents.is_none() {
            let accumulated = Self::accumulate_documents(self)?;
            let reranked = Self::rerank(self, accumulated)?;
            self.documents = Some(reranked);
        }

        match self.documents.as_mut() {
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

impl VoyageRerank {
    fn accumulate_documents(&mut self) -> Result<Vec<Document>, Error> {
        let mut accumulated: Vec<Document> = Vec::new();

        while let GetNextResult::Advanced(input_doc) = self.source.get_next()? {
            let doc = Document::try_from(input_doc.as_ref()).unwrap();
            accumulated.push(doc);
        }

        Ok(accumulated)
    }

    fn rerank(&mut self, input: Vec<Document>) -> Result<VecDeque<Document>, Error> {
        let client = Client::new();

        let projected_input: Vec<String> = input
            .iter()
            .map(|doc| {
                self.descriptor
                    .fields
                    .iter()
                    .fold(String::new(), |s, field| {
                        format!("{} {}:{},", s, field, doc.get_str(field).unwrap_or(""))
                    })
            })
            .collect();

        let payload = RerankRequest {
            query: self.descriptor.query.clone(),
            documents: projected_input,
            model: self.descriptor.model.clone(),
            top_k: self.descriptor.limit,
        };

        let response = self
            .descriptor
            .state
            .runtime()
            .block_on(async {
                let response = client
                    .post(VOYAGE_API_URL)
                    .header(
                        "Authorization",
                        format!("Bearer {}", self.descriptor.state.api_key),
                    )
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .send()
                    .await?;
                response.json::<RerankResponse>().await
            })
            .map_err(|e| Error::new(0, e.to_string()))?;

        let mut result = VecDeque::new();

        for relevance_doc in response.data {
            let mut next: Document = input
                .get(relevance_doc.index)
                .expect("voyage provided index is not present in the original input")
                .clone();

            next.insert(VOYAGE_SCORE_FIELD, relevance_doc.relevance_score);
            result.push_back(next);
        }

        Ok(result)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct RerankRequest {
    query: String,
    documents: Vec<String>,
    model: String,
    top_k: i64,
}

#[derive(Deserialize, Debug)]
struct RerankResponse {
    object: String,
    data: Vec<RankedDocument>,
    model: String,
}

#[derive(Deserialize, Debug)]
struct RankedDocument {
    relevance_score: f64,
    index: usize,
}
