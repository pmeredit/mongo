use crate::command_service::command_service_client::CommandServiceClient;
use crate::desugar::DesugarAggregationStage;
use crate::mongot_client::{
    MongotCursorBatch, VectorSearchCommand, MONGOT_ENDPOINT, RUNTIME, RUNTIME_THREADS,
};
use crate::search::PluginSearch;
use crate::{AggregationSource, AggregationStage, Error, GetNextResult};
use bson::{doc, to_raw_document_buf, RawArrayBuf, Uuid};
use bson::{Document, RawBsonRef, RawDocumentBuf};
use bytes::Buf;
use std::collections::VecDeque;
use std::num::NonZero;
use tokio::runtime::Builder;
use tonic::codec::{Codec, Decoder, Encoder};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response};

pub struct InternalPluginVectorSearch {
    client: CommandServiceClient<Channel>,
    source: Option<AggregationSource>,
    documents: Option<VecDeque<Document>>,
    last_document: RawDocumentBuf,
    index: String,
    query_vector: RawArrayBuf,
    path: String,
    num_candidates: i64,
    limit: i64,
}

impl AggregationStage for InternalPluginVectorSearch {
    fn name() -> &'static str {
        "$_internalPluginVectorSearch"
    }

    fn new(stage_definition: RawBsonRef<'_>) -> Result<Self, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    format!("$_internalPluginVectorSearch stage definition must contain a document."),
                ))
            }
        };

        let query_vector = document
            .get_array("queryVector")
            .map_err(|_| Error {
                code: NonZero::new(1).unwrap(),
                message: String::from("Vector field is expected to be an array"),
                source: None,
            })?
            .to_owned();

        let path = document
            .get_str("path")
            .map_err(|_| Error {
                code: NonZero::new(1).unwrap(),
                message: String::from("Missing 'path' field"),
                source: None,
            })?
            .to_string();

        let index = document.get_str("index").map_err(|_| Error {
            code: NonZero::new(1).unwrap(),
            message: String::from("Missing 'limit' field"),
            source: None,
        })?;

        let num_candidates = document.get_f64("numCandidates").map_err(|_| Error {
            code: NonZero::new(1).unwrap(),
            message: String::from("Missing 'numCandidates' field"),
            source: None,
        })? as i64;

        let limit = document.get_f64("limit").map_err(|_| Error {
            code: NonZero::new(1).unwrap(),
            message: String::from("Missing 'limit' field"),
            source: None,
        })? as i64;

        let client = RUNTIME
            .get_or_init(|| {
                Builder::new_multi_thread()
                    .worker_threads(RUNTIME_THREADS)
                    .thread_name("search-extension")
                    .enable_io()
                    .build()
                    .unwrap()
            })
            .block_on(CommandServiceClient::connect(MONGOT_ENDPOINT))
            .expect("Failed to connect to CommandService");

        Ok(Self {
            client,
            source: None,
            documents: None,
            last_document: RawDocumentBuf::new(),
            index: index.to_string(),
            query_vector,
            path,
            num_candidates,
            limit,
        })
    }

    fn set_source(&mut self, source: AggregationSource) {
        self.source = Some(source);
    }

    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.documents.is_none() {
            Self::populate_documents(self)?;
        }

        match self.documents.as_mut() {
            Some(documents) => {
                if documents.is_empty() {
                    return Ok(GetNextResult::EOF);
                }

                let next = documents.pop_front();
                self.last_document = to_raw_document_buf(&next).unwrap();
                Ok(GetNextResult::Advanced(self.last_document.as_ref()))
            }
            None => Ok(GetNextResult::EOF),
        }
    }
}

impl InternalPluginVectorSearch {
    fn populate_documents(&mut self) -> Result<(), Error> {
        let result = RUNTIME
            .get()
            .unwrap()
            .block_on(async { Self::query_mongot(self).await });

        let binding =
            result.map_err(|e| Error::new(1, format!("Error executing search query: {}", e)))?;
        let batch = binding.get_ref();

        if batch.ok == 0 {
            return Err(Error::new(
                1,
                format!(
                    "Error executing search query: {}",
                    batch.errmsg.as_deref().unwrap_or("unknown error")
                ),
            ));
        }

        let results: Option<VecDeque<Document>> = batch.cursor.as_ref().map(|cursor| {
            cursor
                .next_batch
                .iter()
                .map(|result| doc!("_id": result.id.clone(), "$vectorSearchScore": result.score))
                .collect()
        });

        match results {
            Some(docs) => self.documents = Some(docs),
            None => self.documents = Some(VecDeque::new()),
        }

        Ok(())
    }

    async fn query_mongot(
        &mut self,
    ) -> Result<Response<MongotCursorBatch>, Box<dyn std::error::Error>> {
        let request: Request<VectorSearchCommand> = Request::new(VectorSearchCommand {
            vector_search: String::from("test"), // TODO pass this to plugin during stage creation
            db: String::from("test"),            // TODO pass this to plugin during stage creation
            collection_uuid: Uuid::parse_str("e954c0a6-61b3-477d-859c-2bac22e865a2").unwrap(), // TODO pass this to plugin during stage creation
            index: String::from(self.index.clone()), // TODO avoid clone
            path: String::from(self.path.clone()),
            query_vector: self.query_vector.clone(),
            num_candidates: self.num_candidates,
            limit: self.limit,
        });

        let result = self.client.vectorSearch(request).await?;

        Ok(result)
    }
}

pub struct PluginVectorSearch;

impl DesugarAggregationStage for PluginVectorSearch {
    fn name() -> &'static str {
        "$pluginVectorSearch"
    }

    fn desugar(stage_definition: RawBsonRef<'_>) -> Result<Vec<Document>, Error> {
        let query = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$pluginVectorSearch stage definition must contain a document.".to_string(),
                ))
            }
        }
        .to_document()
        .unwrap();

        Ok(vec![
            doc! {"$_internalPluginVectorSearch": query},
            doc! {"$_internalSearchIdLookup": doc!{}},
        ])
    }
}
