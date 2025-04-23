//! Extension implementation of `$vectorSearch`.
//!
//! The extension itself contains stages:
//! * [`PluginVectorSearchDescriptor`] implements the `$pluginVectorSearch` desugaring stage.
//! * [`InternalPluginVectorSearchDescriptor`] implements the `$_internalPluginVectorSearch` stage.
//!
//! `$pluginVectorSearch` always desugars to at least a `$_internalPluginVectorSearch` stage, but
//! may use the host provided stage `_internalSearchIdLookup` to complete queries.
//!
//! `_internalPluginVectorSearch` maintains an asynchronous threaded runtime and makes gRPC calls to
//! a remote `mongot` host that server search queries.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::command_service::command_service_client::CommandServiceClient;
use crate::mongot_client::{
    MongotClientState, MongotCursorBatch, MongotResult, VectorSearchCommand,
};
use crate::sdk::{
    stage_constraints, AggregationStageContext, AggregationStageDescriptor,
    AggregationStageExecutor, AggregationStageProperties, DesugarAggregationStageDescriptor, Error,
    GetNextResult, SourceAggregationStageDescriptor, SourceBoundAggregationStageDescriptor,
};

use bson::{
    doc, from_document, to_raw_document_buf, Document, RawArrayBuf, RawBsonRef, RawDocument,
};
use tonic::transport::Channel;
use tonic::{Request, Response};

/// Descriptor for `$_internalVectorSearch`.
///
/// This stage uses a provided tokio `Runtime` to execute remote gRPC queries against `mongot`.
/// Remote fetching is less complicated than for text search stages as vector search does not
/// yield cursors -- all the results appear in the first batch.
///
/// The target host is passed in context during descriptor binding, although this mechanism is
/// likely to change in the future.
pub struct InternalPluginVectorSearchDescriptor(Arc<MongotClientState>);

impl AggregationStageDescriptor for InternalPluginVectorSearchDescriptor {
    fn name() -> &'static str {
        "$_internalPluginVectorSearch"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::First,
            host_type: stage_constraints::HostTypeRequirement::AnyShard,
            can_run_on_shards_pipeline: true,
        }
    }
}

impl InternalPluginVectorSearchDescriptor {
    pub fn new(client_state: Arc<MongotClientState>) -> Self {
        Self(client_state)
    }
}

impl SourceAggregationStageDescriptor for InternalPluginVectorSearchDescriptor {
    type BoundDescriptor = InternalPluginVectorSearchBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        InternalPluginVectorSearchBoundDescriptor::new(
            Arc::clone(&self.0),
            stage_definition,
            context,
        )
    }
}

#[derive(Clone)]
pub struct InternalPluginVectorSearchBoundDescriptor {
    client_state: Arc<MongotClientState>,
    index: String,
    query_vector: RawArrayBuf,
    path: String,
    num_candidates: i64,
    limit: i64,
    context: AggregationStageContext,
}

impl InternalPluginVectorSearchBoundDescriptor {
    fn new(
        client_state: Arc<MongotClientState>,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self, Error> {
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

        if context.mongot_host.is_none() {
            return Err(Error::new(
                1,
                "$pluginVectorSearch context must contain a mongot host",
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
            .map(str::to_owned)
            .map_err(|_| Error::new(1, "Missing 'limit' field"))?;

        let num_candidates = document
            .get_f64("numCandidates")
            .map_err(|_| Error::new(1, "Missing 'numCandidates' field"))?
            as i64;

        let limit = document
            .get_f64("limit")
            .map_err(|_| Error::new(1, "Missing 'limit' field"))? as i64;

        Ok(Self {
            client_state,
            index,
            query_vector,
            path,
            num_candidates,
            limit,
            context,
        })
    }
}

impl SourceBoundAggregationStageDescriptor for InternalPluginVectorSearchBoundDescriptor {
    type Executor = InternalPluginVectorSearch;

    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        Ok(vec![
            doc! {"$sort": {"score": {"$meta": "vectorSearchScore"}}},
        ])
    }

    fn create_executor(&self) -> Result<Self::Executor, Error> {
        Ok(InternalPluginVectorSearch::with_descriptor(self.clone()))
    }
}

pub struct InternalPluginVectorSearch {
    client: CommandServiceClient<Channel>,
    documents: Option<VecDeque<Document>>,
    descriptor: InternalPluginVectorSearchBoundDescriptor,
}

impl InternalPluginVectorSearch {
    fn with_descriptor(descriptor: InternalPluginVectorSearchBoundDescriptor) -> Self {
        let mongot_host = format!(
            "http://{}",
            descriptor
                .context
                .mongot_host
                .clone()
                .expect("mongot host should be present")
        );

        let client = descriptor
            .client_state
            .runtime()
            .block_on(CommandServiceClient::connect(mongot_host))
            .expect("Failed to connect to CommandService");

        Self {
            client,
            documents: None,
            descriptor,
        }
    }
}

impl AggregationStageExecutor for InternalPluginVectorSearch {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.descriptor.context.collection_uuid.is_none() {
            return Err(Error::new(
                1,
                "$pluginVectorSearch context must contain a collection UUID",
            ));
        }

        if self.documents.is_none() {
            Self::populate_documents(self)?;
        }

        match self.documents.as_mut() {
            Some(documents) => {
                if documents.is_empty() {
                    return Ok(GetNextResult::EOF);
                }

                let next = documents.pop_front();
                Ok(GetNextResult::Advanced(
                    to_raw_document_buf(&next).unwrap().into(),
                ))
            }
            None => Ok(GetNextResult::EOF),
        }
    }
}

impl InternalPluginVectorSearch {
    fn populate_documents(&mut self) -> Result<(), Error> {
        let client_state = Arc::clone(&self.descriptor.client_state);
        let result = client_state
            .runtime()
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
                .map(|doc| from_document::<MongotResult>(doc.clone()).unwrap())
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
                .descriptor
                .context
                .collection
                .clone()
                .expect("init verified collection exists"),
            db: self.descriptor.context.db.clone(),
            collection_uuid: self
                .descriptor
                .context
                .collection_uuid
                .expect("init verified collectionUUID exists"),
            index: self.descriptor.index.clone(),
            path: self.descriptor.path.clone(),
            query_vector: self.descriptor.query_vector.clone(),
            num_candidates: self.descriptor.num_candidates,
            limit: self.descriptor.limit,
        });

        let result = self.client.vectorSearch(request).await?;

        Ok(result)
    }
}

/// Descriptor for the `$pluginVectorSearch` desugaring stage.
///
/// This stage unconditionally de-sugars into a remote vector fetch and id lookup.
///
/// This stage interacts with the pipeline differently from the linked-in `$Vector search` stage in
/// a few important ways:
/// * De-sugaring is performed through a generic mechanism rather than a hard-coded call invoked
///   during the creation of an aggregation pipeline.
/// * The internal vector search stage and `_internalSearchIdLookup` must be run together on the
///   shard host during sharded queries. In the regular pipeline this implemented using
///   generic-looking stage constraints (`needsSplit` and `canMovePast`), but here we use
///   `$betaMultiStream` to create a sub-pipeline which forces this grouping to occur.
pub struct PluginVectorSearchDescriptor;

impl AggregationStageDescriptor for PluginVectorSearchDescriptor {
    fn name() -> &'static str {
        "$pluginVectorSearch"
    }

    fn properties(&self) -> AggregationStageProperties {
        // TODO: this should return the value value as the internal remote stage.
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::First,
            host_type: stage_constraints::HostTypeRequirement::AnyShard,
            can_run_on_shards_pipeline: true,
        }
    }
}

impl DesugarAggregationStageDescriptor for PluginVectorSearchDescriptor {
    fn desugar(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Vec<Document>, Error> {
        let query = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$pluginVectorSearch stage definition must contain a document.",
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
