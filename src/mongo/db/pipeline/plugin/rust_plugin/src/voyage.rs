use std::collections::VecDeque;
use std::env;
use std::num::NonZero;
use std::sync::OnceLock;

use bson::{doc, to_raw_document_buf, RawArray, RawDocument};
use bson::{Document, RawBsonRef};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};

use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageProperties,
    TransformAggregationStageDescriptor, TransformBoundAggregationStageDescriptor,
};
use crate::{AggregationSource, AggregationStage, Error, GetNextResult};

static VOYAGE_API_URL: &str = "https://api.voyageai.com/v1/rerank";
static VOYAGE_SCORE_FIELD: &str = "$voyageRerankScore";
static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static RUNTIME_THREADS: usize = 4;

// TODO: API key should be read in the stage descriptor.
// This will require re-plumbing ExtensionPortal and StageDescriptor interfaces to allow accessing
// &self to get this information.
pub struct VoyageRerankDescriptor;

impl AggregationStageDescriptor for VoyageRerankDescriptor {
    fn name() -> &'static str {
        "$voyageRerank"
    }

    fn properties() -> AggregationStageProperties {
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
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        VoyageRerankBoundDescriptor::from_stage_definition(stage_definition)
    }
}

#[derive(Clone)]
pub struct VoyageRerankBoundDescriptor {
    query: String,
    fields: Vec<String>,
    model: String,
    limit: i64,
    api_key: String,
}

impl VoyageRerankBoundDescriptor {
    fn from_stage_definition(stage_definition: RawBsonRef<'_>) -> Result<Self, Error> {
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
            .map_err(|_| Error {
                code: NonZero::new(1).unwrap(),
                message: String::from("Missing 'query' field"),
                source: None,
            })?
            .to_owned();

        let model = document
            .get_str("model")
            .map_err(|_| Error {
                code: NonZero::new(1).unwrap(),
                message: String::from("Missing 'model' field"),
                source: None,
            })?
            .to_string();

        let fields_arr: &RawArray = document
            .get_array("fields")
            .map_err(|_| Error::new(1, String::from("Missing 'fields' field")))?;

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

        let limit = document.get_f64("limit").map_err(|_| Error {
            code: NonZero::new(1).unwrap(),
            message: String::from("Missing 'limit' field"),
            source: None,
        })? as i64;

        // TODO implement plugin config management to access settings and secrets
        let api_key = env::var("VOYAGE_API_KEY").expect("$VOYAGE_API_KEY not set");

        Ok(Self {
            query,
            fields,
            model,
            limit,
            api_key,
        })
    }
}

impl TransformBoundAggregationStageDescriptor for VoyageRerankBoundDescriptor {
    type Executor = VoyageRerank;

    /// Create a new executor based on bound state from creation.
    // TODO: this should accept a source to read from.
    fn create_executor(&self) -> Result<Self::Executor, Error> {
        Ok(VoyageRerank::with_descriptor(self.clone()))
    }
}

pub struct VoyageRerank {
    descriptor: VoyageRerankBoundDescriptor,
    source: Option<AggregationSource>,
    documents: Option<VecDeque<Document>>,
}

impl VoyageRerank {
    fn with_descriptor(descriptor: VoyageRerankBoundDescriptor) -> Self {
        // TODO init only once at stage registration
        RUNTIME.get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(RUNTIME_THREADS)
                .thread_name("search-extension")
                .enable_time()
                .enable_io()
                .build()
                .unwrap()
        });

        Self {
            descriptor,
            source: None,
            documents: None,
        }
    }
}

impl AggregationStage for VoyageRerank {
    fn name() -> &'static str {
        "$voyageRerank"
    }

    fn set_source(&mut self, source: AggregationSource) {
        self.source = Some(source);
    }

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

    fn get_merging_stages(&mut self) -> Result<Vec<Document>, Error> {
        // TODO This stage should be forced to run on the merging half of the pipeline.
        Ok(vec![])
    }
}

impl VoyageRerank {
    fn accumulate_documents(&mut self) -> Result<Vec<Document>, Error> {
        let source = self.source.as_mut().expect("source should be present");
        let mut accumulated: Vec<Document> = Vec::new();

        while let GetNextResult::Advanced(input_doc) = source.get_next()? {
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

        let response = RUNTIME
            .get()
            .unwrap()
            .block_on(async {
                let response = client
                    .post(VOYAGE_API_URL)
                    .header(
                        "Authorization",
                        format!("Bearer {}", self.descriptor.api_key),
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
