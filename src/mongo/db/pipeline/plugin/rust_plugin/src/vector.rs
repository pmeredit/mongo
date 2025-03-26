use std::collections::VecDeque;

use bson::{
    doc, to_raw_document_buf, Document, RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf,
};
use tokio::runtime::Builder;
use tonic::transport::Channel;
use tonic::{Request, Response};

use crate::command_service::command_service_client::CommandServiceClient;
use crate::desugar::DesugarAggregationStage;
use crate::mongot_client::{
    MongotCursorBatch, VectorSearchCommand, MONGOT_ENDPOINT, RUNTIME, RUNTIME_THREADS,
};
use crate::{AggregationSource, AggregationStage, AggregationStageContext, Error, GetNextResult};

pub struct InternalPluginVectorSearch {
    client: CommandServiceClient<Channel>,
    context: AggregationStageContext,
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

    fn new(stage_definition: RawBsonRef<'_>, context: &RawDocument) -> Result<Self, Error> {
        let document = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$_internalPluginVectorSearch stage definition must contain a document.",
                ))
            }
        };

        let context = AggregationStageContext::try_from(context)?;
        if context.collection.is_none() {
            return Err(Error::new(
                1,
                "$pluginVectorSearch context must contain a collection name",
            ));
        }
        if context.collection_uuid.is_none() {
            return Err(Error::new(
                1,
                "$pluginVectorSearch context must contain a collection UUID",
            ));
        }

        let query_vector = document
            .get_array("queryVector")
            .map_err(|_| Error::new(1, "Vector field is expected to be an array"))?
            .to_owned();

        let path = document
            .get_str("path")
            .map_err(|_| Error::new(1, "Missing 'path' field"))?
            .to_string();

        let index = document
            .get_str("index")
            .map_err(|_| Error::new(1, "Missing 'limit' field"))?;

        let num_candidates = document
            .get_f64("numCandidates")
            .map_err(|_| Error::new(1, "Missing 'numCandidates' field"))?
            as i64;

        let limit = document
            .get_f64("limit")
            .map_err(|_| Error::new(1, "Missing 'limit' field"))? as i64;

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
            context,
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

    fn get_merging_stages(&mut self) -> Result<Vec<Document>, Error> {
        Ok(vec![
            doc! {"$sort": {"$meta": "vectorSearchScore"}}
        ])
    }
}

impl InternalPluginVectorSearch {
    fn populate_documents(&mut self) -> Result<(), Error> {
        let result = RUNTIME
            .get()
            .unwrap()
            .block_on(async { self.query_mongot().await });

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
            vector_search: self
                .context
                .collection
                .clone()
                .expect("init verified collection exists"),
            db: self.context.db.clone(),
            collection_uuid: self
                .context
                .collection_uuid
                .expect("init verified collectionUUID exists"),
            index: self.index.clone(),
            path: self.path.clone(),
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
