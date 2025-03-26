use crate::{AggregationSource, AggregationStage, Error, GetNextResult};
use bson::{doc, to_raw_document_buf, RawArray, RawDocument};
use bson::{Document, RawBsonRef, RawDocumentBuf};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::env;
use std::num::NonZero;
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

static VOYAGE_API_URL: &str = "https://api.voyageai.com/v1/rerank";
static VOYAGE_SCORE_FIELD: &str = "$voyageRerankScore";
static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static RUNTIME_THREADS: usize = 4;

pub struct VoyageRerank {
    source: Option<AggregationSource>,
    documents: Option<VecDeque<Document>>,
    last_document: RawDocumentBuf,
    query: String,
    fields: Vec<String>,
    model: String,
    limit: i64,
    api_key: String,
}

impl AggregationStage for VoyageRerank {
    fn name() -> &'static str {
        "$voyageRerank"
    }

    fn new(stage_definition: RawBsonRef<'_>, _context: &RawDocument) -> Result<Self, Error> {
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

        Ok(Self {
            source: None,
            documents: None,
            last_document: RawDocumentBuf::new(),
            query,
            fields,
            model,
            limit,
            api_key,
        })
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
                self.last_document = to_raw_document_buf(&next).unwrap();
                Ok(GetNextResult::Advanced(self.last_document.as_ref()))
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
            let doc = Document::try_from(input_doc).unwrap();
            accumulated.push(doc);
        }

        Ok(accumulated)
    }

    fn rerank(&mut self, input: Vec<Document>) -> Result<VecDeque<Document>, Error> {
        let client = Client::new();

        let projected_input: Vec<String> = input
            .iter()
            .map(|doc| {
                self.fields
                    .iter()
                    .fold(String::new(), |s, field|
                        format!("{} {}:{},", s, field, doc.get_str(field).unwrap_or(""))
                    )
            })
            .collect();

        let payload = RerankRequest {
            query: self.query.clone(),
            documents: projected_input,
            model: self.model.clone(),
            top_k: self.limit,
        };

        let response = RUNTIME
            .get()
            .unwrap()
            .block_on(async {
                let response = client
                    .post(VOYAGE_API_URL)
                    .header("Authorization", format!("Bearer {}", self.api_key))
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
