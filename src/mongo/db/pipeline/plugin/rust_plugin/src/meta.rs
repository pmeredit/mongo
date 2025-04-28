//! Extension implementation of `$searchMeta`.
//!
//! This consists of two stages:
//! * [`PluginMetaDescriptor`] implements the` $pluginMeta` de-sugaring stage.
//! * [`InternalPluginMetaDescriptor`] implements the `$_internalPluginMeta` source stage.
//!
//! The former typically de-sugars into the latter, assuming that the stage definition is structured
//! in a way that would actually produce meta facet results.
//!
//! These stages may also be composed as part of `$search` queries if those requests would perform
//! both document retrieval and faceting.

use std::sync::Arc;

use bson::{doc, to_raw_document_buf};
use bson::{Bson, Document, RawBsonRef, RawDocument};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, watch};
use tonic::codegen::tokio_stream;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Status, Streaming};

use crate::command_service::command_service_client::CommandServiceClient;
use crate::mongot_client::{
    CursorOptions, GetMoreSearchCommand, InitialSearchCommand, MetadataMode, MongotClientState,
    MongotCursorBatch, ResultType, SearchCommand,
};
use crate::sdk::{
    stage_constraints, AggregationStageContext, AggregationStageDescriptor,
    AggregationStageExecutor, AggregationStageProperties, DesugarAggregationStageDescriptor, Error,
    GetNextResult, SourceAggregationStageDescriptor, SourceBoundAggregationStageDescriptor,
};

// roughly two 16 MB batches of id+score payload
static CHANNEL_BUFFER_SIZE: usize = 1_000_000;

/// Descriptor for `$_internalPluginMeta`.
///
/// This stage uses a provided tokio `Runtime` to execute remote gRPC queries against `mongot`.
/// Using an asynchronous runtime allows us to fetch and buffer batches of documents in the
/// background rather than synchronously fetching when we run out of documents.
///
/// The target host is passed in context during descriptor binding, although this mechanism is
/// likely to change in the future.
pub struct InternalPluginMetaDescriptor(Arc<MongotClientState>);

impl InternalPluginMetaDescriptor {
    pub fn new(client_state: Arc<MongotClientState>) -> Self {
        Self(client_state)
    }
}

impl AggregationStageDescriptor for InternalPluginMetaDescriptor {
    fn name() -> &'static str {
        "$_internalPluginMeta"
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

impl SourceAggregationStageDescriptor for InternalPluginMetaDescriptor {
    type BoundDescriptor = InternalPluginMetaBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        InternalPluginMetaBoundDescriptor::new(Arc::clone(&self.0), stage_definition, context)
    }
}

#[derive(Clone)]
pub struct InternalPluginMetaBoundDescriptor {
    client_state: Arc<MongotClientState>,
    query: Document,
    context: AggregationStageContext,
}

impl InternalPluginMetaBoundDescriptor {
    fn new(
        client_state: Arc<MongotClientState>,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self, Error> {
        let query = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$_internalPluginMeta stage definition must contain a document.".to_string(),
                ));
            }
        }
        .to_document()
        .unwrap();

        let context = AggregationStageContext::try_from(context)?;
        if context.collection.is_none() {
            return Err(Error::new(
                1,
                "$_internalPluginMeta context must contain a collection name",
            ));
        }

        if context.mongot_host.is_none() {
            return Err(Error::new(
                1,
                "$_internalPluginMeta context must contain a mongot host",
            ));
        }

        Ok(Self {
            client_state,
            query,
            context,
        })
    }
}

impl SourceBoundAggregationStageDescriptor for InternalPluginMetaBoundDescriptor {
    type Executor = InternalPluginMeta;

    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        let query = self
            .query
            .get_document("definition")
            .map_err(|_| Error::new(1, "Missing or invalid 'definition'".to_string()))?;

        sharded_planner::build_meta_pipeline(query)
    }

    fn create_executor(&self) -> Result<Self::Executor, Error> {
        Ok(InternalPluginMeta::with_descriptor(self.clone()))
    }
}

pub struct InternalPluginMeta {
    descriptor: InternalPluginMetaBoundDescriptor,
    initialized: bool,
    client: CommandServiceClient<Channel>,
    result_tx: Sender<Payload>,
    result_rx: Receiver<Payload>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

type Payload = Option<Document>;

impl Drop for InternalPluginMeta {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl InternalPluginMeta {
    fn with_descriptor(descriptor: InternalPluginMetaBoundDescriptor) -> Self {
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

        // bounded channel used for async result push / sync result poll
        let (result_tx, result_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        // watch channel used for shutdown signal to stop async getmore polling
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Self {
            descriptor,
            initialized: false,
            client,
            result_tx,
            result_rx,
            shutdown_tx,
            shutdown_rx,
        }
    }
}

impl AggregationStageExecutor for InternalPluginMeta {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.descriptor.context.collection_uuid.is_none() {
            return Err(Error::new(
                1,
                "$pluginSearch context must contain a collection UUID",
            ));
        }

        if !self.initialized {
            let client_state = Arc::clone(&self.descriptor.client_state);
            client_state
                .runtime()
                .block_on(async { Self::start_fetching_results(self).await })?;
            self.initialized = true;
        }

        let result = self.result_rx.blocking_recv().unwrap();

        match result {
            Some(doc) => Ok(GetNextResult::Advanced(
                to_raw_document_buf(&doc).unwrap().into(),
            )),
            None => Ok(GetNextResult::EOF),
        }
    }
}

impl InternalPluginMeta {
    async fn start_fetching_results(&mut self) -> Result<(), Error> {
        // mongot expects all requests within stage execution
        // to be sent via a single bidirectional stream
        let (sender, receiver) = mpsc::channel(1);

        let is_sharded = self.descriptor.context.sharded_query;

        let is_collector = self
            .descriptor
            .query
            .get("definition")
            .unwrap()
            .as_document()
            .map_or(false, |search| {
                search.get("facet").is_some() || search.get("count").is_some()
            });

        let intermediate = if is_collector { Some(1) } else { None };
        let metadata = if is_collector {
            if is_sharded {
                MetadataMode::ALL
            } else {
                MetadataMode::ACCUMULATED
            }
        } else {
            MetadataMode::NONE
        };

        let lookup_token = self
            .descriptor
            .query
            .get("lookup_token")
            .map(|bson| bson.as_object_id().unwrap());

        // execute the initial request to fetch first batch and obtain the cursor id
        sender
            .send(SearchCommand::Initial(InitialSearchCommand {
                search: self
                    .descriptor
                    .context
                    .collection
                    .clone()
                    .expect("init verified collection name is present"),
                db: self.descriptor.context.db.clone(),
                collection_uuid: self
                    .descriptor
                    .context
                    .collection_uuid
                    .expect("verified collection UUID present at initialization"),
                query: self
                    .descriptor
                    .query
                    .get("definition")
                    .and_then(Bson::as_document)
                    .cloned()
                    .unwrap(),
                // small batch_size is for test purposes, this allows us to send getMores
                cursor_options: Some(CursorOptions {
                    batch_size: 5,
                    lookup_token,
                    metadata,
                }),
                intermediate,
            }))
            .await
            .unwrap();

        let outbound_stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let response = self.client.search(outbound_stream).await.unwrap();
        let mut inbound_stream: Streaming<MongotCursorBatch> = response.into_inner();

        let cursor_id: u64 = if let Some(received) = inbound_stream.next().await {
            InternalPluginMeta::flush_batch_into_channel(received, self.result_tx.clone()).await
        } else {
            0
        };

        if cursor_id == 0 {
            // if we have exhausted the cursor in the initial query, flush EOF and return
            InternalPluginMeta::flush_eof_into_channel(self.result_tx.clone()).await;
            return Ok(());
        }

        // init an async getmore prefetch loop
        let mut shutdown_rx = self.shutdown_rx.clone();
        let result_tx = self.result_tx.clone();

        // spawn a background task that is terminated on the stage drop
        tokio::spawn(async move {
            let mut exhausted = false;

            loop {
                select! {
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                    _ = async {
                        let request = SearchCommand::GetMore(GetMoreSearchCommand {
                            cursor_id,
                            // small batch_size is for test purposes, this allows us to send many getMores
                            cursor_options: Some(CursorOptions { batch_size: 5, lookup_token: None, metadata: MetadataMode::NONE }),
                        });

                        if let Err(err) = sender.send(request).await {
                            eprintln!("Failed to send request: {:?}", err);
                            InternalPluginMeta::flush_eof_into_channel(result_tx.clone()).await;
                            return;
                        }

                        if let Some(received) = inbound_stream.next().await {
                            exhausted = InternalPluginMeta::flush_batch_into_channel(
                                received, result_tx.clone()).await == 0;
                        }
                    } => {
                        if exhausted {
                            InternalPluginMeta::flush_eof_into_channel(result_tx.clone()).await;
                            return;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn flush_batch_into_channel(
        received: Result<MongotCursorBatch, Status>,
        result_tx: Sender<Payload>,
    ) -> u64 {
        let batch = received.unwrap();

        // initial intermediate query
        if let Some(cursors) = batch.cursors {
            let meta = cursors
                .iter()
                .find(|batch| {
                    batch
                        .cursor
                        .as_ref()
                        .and_then(|cursor| cursor.r#type.as_ref())
                        .is_some_and(|cursor_type| *cursor_type == ResultType::Meta)
                })
                .unwrap();

            let cursor = meta.cursor.as_ref().unwrap();
            if !cursor.next_batch.is_empty() {
                panic!("Initial response with a lookup token should never return results");
            }

            cursor.id

            // initial non-intermediate query or getmore
        } else if let Some(cursor) = batch.cursor {
            for next in &cursor.next_batch {
                let doc = next.clone();
                result_tx.send(Some(doc)).await.unwrap_or_else(|err| {
                    eprintln!("Failed to flush result: {:?}", err);
                });
            }
            cursor.id
        } else {
            0
        }
    }

    async fn flush_eof_into_channel(result_tx: Sender<Payload>) {
        let _ = result_tx.send(None).await;
    }
}

/// Descriptor for the `$pluginMeta` desugaring stage.
pub struct PluginMetaDescriptor;

impl AggregationStageDescriptor for PluginMetaDescriptor {
    fn name() -> &'static str {
        "$pluginMeta"
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

impl DesugarAggregationStageDescriptor for PluginMetaDescriptor {
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
                    "$_internalPluginMeta stage definition must contain a document.".to_string(),
                ));
            }
        }
        .to_document()
        .unwrap();

        // if it's a collector query, run the meta stage
        if let Some(search) = query.get("definition").unwrap().as_document() {
            if search.get("facet").is_some() || search.get("count").is_some() {
                return Ok(vec![doc! {"$_internalPluginMeta": query}]);
            }
        }

        // otherwise, erase itself as operator queries do not output metadata
        // TODO replace $countNodes with an empty pipeline once $betaMultiStream supports that
        Ok(vec![doc! {"$countNodes": {}}])
    }
}

/// Generates meta results merging logic as an MQL pipeline, similar to the existing
/// planShardedSearch mongot command. That command was originally implemented in mongot to keep
/// search-specific logic out of the server and allow easier iteration without server releases.
/// Extension can cover both points without the latency tradeoff, so the logic now lives here.
mod sharded_planner {
    use crate::sdk::Error;
    use bson::{doc, Bson, Document};
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    enum FacetDefinition {
        String(StringFacetDefinition),
        Date(DateFacetDefinition),
        Numeric(NumericFacetDefinition),
    }

    #[derive(Debug, Clone)]
    struct StringFacetDefinition {
        path: String,
        num_buckets: i32,
    }

    #[derive(Debug, Clone)]
    struct DateFacetDefinition {
        path: String,
        boundaries: Vec<Bson>,
    }

    #[derive(Debug, Clone)]
    struct NumericFacetDefinition {
        path: String,
        boundaries: Vec<Bson>,
    }

    const FACET_TYPE: &str = "facet";

    pub fn build_meta_pipeline(query: &Document) -> Result<Vec<Document>, Error> {
        if query.get(FACET_TYPE).is_none() {
            return Ok(vec![]);
        }

        let facet_definitions = extract_facet_definitions(query)?;

        Ok(vec![
            get_group_stage(),
            get_facet_stage(&facet_definitions),
            get_replace_with_stage(&facet_definitions),
        ])
    }

    fn get_group_stage() -> Document {
        doc! {
            "$group": {
                "_id": {
                    "type": "$type",
                    "tag": "$tag",
                    "bucket": "$bucket"
                },
                "value": { "$sum": "$count" }
            }
        }
    }

    fn get_facet_stage(facet_definitions: &HashMap<String, FacetDefinition>) -> Document {
        let match_doc = doc! {
            "$match": {
                "_id.type": { "$eq": "count" }
            }
        };

        let count_doc = vec![match_doc];
        let mut facet_doc = Document::new();
        facet_doc.insert("count", count_doc);

        for (name, definition) in facet_definitions {
            facet_doc.insert(name, get_facet_buckets(definition, name));
        }

        doc! {
            "$facet": facet_doc
        }
    }

    fn get_replace_with_stage(facet_definitions: &HashMap<String, FacetDefinition>) -> Document {
        let count_doc = doc! {
            "total": { "$first": "$count.value" }
        };

        let mut replace_with_doc = Document::new();
        replace_with_doc.insert("count", count_doc);

        if !facet_definitions.is_empty() {
            let mut facet_doc = Document::new();
            for name in facet_definitions.keys() {
                facet_doc.insert(
                    name,
                    doc! {
                        "buckets": {
                            "$map": {
                                "input": format!("${}", name),
                                "as": "bucket",
                                "in": {
                                    "_id": "$$bucket._id.bucket",
                                    "count": "$$bucket.value"
                                }
                            }
                        }
                    },
                );
            }
            replace_with_doc.insert("facet", facet_doc);
        }

        doc! {
            "$replaceWith": replace_with_doc
        }
    }

    fn get_facet_buckets(facet: &FacetDefinition, facet_name: &str) -> Vec<Document> {
        match facet {
            FacetDefinition::String(string_facet) => {
                build_string_facet_bucket(string_facet, facet_name)
            }
            FacetDefinition::Date(_facet) => build_numeric_or_date_facet_bucket(facet_name),
            FacetDefinition::Numeric(_facet) => build_numeric_or_date_facet_bucket(facet_name),
        }
    }

    fn build_string_facet_bucket(facet: &StringFacetDefinition, facet_name: &str) -> Vec<Document> {
        vec![
            doc! {
                "$match": {
                    "_id.type": { "$eq": FACET_TYPE },
                    "_id.tag": { "$eq": facet_name }
                }
            },
            doc! {
                "$sort": {
                    "value": -1,
                    "_id": 1
                }
            },
            doc! {
                "$limit": facet.num_buckets
            },
        ]
    }

    fn build_numeric_or_date_facet_bucket(facet_name: &str) -> Vec<Document> {
        vec![
            doc! {
                "$match": {
                    "_id.type": { "$eq": FACET_TYPE },
                    "_id.tag": { "$eq": facet_name }
                }
            },
            doc! {
                "$sort": {
                    "_id.bucket": 1
                }
            },
        ]
    }

    fn extract_facet_definitions(
        definition: &Document,
    ) -> Result<HashMap<String, FacetDefinition>, Error> {
        let facet = definition
            .get_document("facet")
            .map_err(|_| Error::new(1, "Missing or invalid 'definition.facet'".to_string()))?;

        let facets = facet.get_document("facets").map_err(|_| {
            Error::new(
                1,
                "Missing or invalid 'definition.facet.facets'".to_string(),
            )
        })?;

        let mut result = HashMap::new();

        for (facet_name, facet_value) in facets.iter() {
            if let Some(facet_doc) = facet_value.as_document() {
                let facet_type = facet_doc.get_str("type").map_err(|_| {
                    Error::new(
                        1,
                        format!("Missing or invalid 'type' field in facet '{}'", facet_name),
                    )
                })?;

                let path = facet_doc.get_str("path").map_err(|_| {
                    Error::new(
                        1,
                        format!("Missing or invalid 'path' field in facet '{}'", facet_name),
                    )
                })?;

                let facet_definition = match facet_type {
                    "string" => {
                        let num_buckets = facet_doc.get_i32("numBuckets").unwrap_or(10);
                        FacetDefinition::String(StringFacetDefinition {
                            path: path.to_string(),
                            num_buckets,
                        })
                    }
                    "date" => {
                        let boundaries = extract_boundaries(facet_doc)?;
                        FacetDefinition::Date(DateFacetDefinition {
                            path: path.to_string(),
                            boundaries,
                        })
                    }
                    "number" => {
                        let boundaries = extract_boundaries(facet_doc)?;
                        FacetDefinition::Numeric(NumericFacetDefinition {
                            path: path.to_string(),
                            boundaries,
                        })
                    }
                    _ => {
                        return Err(Error::new(
                            1,
                            format!(
                                "Unknown facet type '{}' for facet '{}'",
                                facet_type, facet_name
                            ),
                        ));
                    }
                };

                result.insert(facet_name.to_string(), facet_definition);
            }
        }

        Ok(result)
    }

    fn extract_boundaries(facet_doc: &Document) -> Result<Vec<Bson>, Error> {
        let boundaries_array = facet_doc.get_array("boundaries").map_err(|_| {
            Error::new(1, format!("Missing or invalid 'boundaries' field in facet"))
        })?;

        Ok(boundaries_array.clone())
    }
}
