use crate::command_service::command_service_client::CommandServiceClient;
use crate::mongot_client::{
    CursorOptions, GetMoreSearchCommand, InitialSearchCommand, MongotCursorBatch, SearchCommand,
    MONGOT_ENDPOINT, RUNTIME, RUNTIME_THREADS,
};
use crate::sdk::{AggregationStageDescriptor, AggregationStageProperties, DesugarAggregationStageDescriptor, stage_constraints};
use crate::{AggregationSource, AggregationStage, AggregationStageContext, Error, GetNextResult};

use bson::{doc, to_raw_document_buf};
use bson::{Document, RawBsonRef, RawDocument};
use tokio::runtime::Builder;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, watch};
use tonic::codegen::tokio_stream;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Status, Streaming};

// roughly two 16 MB batches of id+score payload
static CHANNEL_BUFFER_SIZE: usize = 1_000_000;

pub struct InternalPluginSearch {
    initialized: bool,
    client: CommandServiceClient<Channel>,
    context: AggregationStageContext,
    source: Option<AggregationSource>,
    query: Document,
    stored_source: bool,
    result_tx: Sender<Payload>,
    result_rx: Receiver<Payload>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

type Payload = Option<Document>;

impl Drop for InternalPluginSearch {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl AggregationStage for InternalPluginSearch {
    fn name() -> &'static str {
        "$_internalPluginSearch"
    }

    fn new(stage_definition: RawBsonRef<'_>, context: &RawDocument) -> Result<Self, Error> {
        let query = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$_internalPluginSearch stage definition must contain a document.".to_string(),
                ))
            }
        }
        .to_document()
        .unwrap();

        let context = AggregationStageContext::try_from(context)?;
        if context.collection.is_none() {
            return Err(Error::new(1, "$pluginSearch context must contain a collection name"));
        }
        if context.collection_uuid.is_none() {
            return Err(Error::new(1, "$pluginSearch context must contain a collection UUID"));
        }

        let stored_source = query.get_bool("returnStoredSource").unwrap_or(false);

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

        // bounded channel used for async result push / sync result poll
        let (result_tx, result_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        // watch channel used for shutdown signal to stop async getmore polling
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Ok(Self {
            initialized: false,
            context,
            client,
            source: None,
            query,
            stored_source,
            result_tx,
            result_rx,
            shutdown_tx,
            shutdown_rx,
        })
    }

    fn set_source(&mut self, source: AggregationSource) {
        self.source = Some(source);
    }

    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if !self.initialized {
            // TODO figure out if start_fetching_results should be executed
            // earlier at stage creation
            RUNTIME
                .get()
                .unwrap()
                .block_on(async { Self::start_fetching_results(self).await })?;
            self.initialized = true;
        }

        let result = self.result_rx.blocking_recv().unwrap();

        match result {
            Some(doc) => {
                Ok(GetNextResult::Advanced(to_raw_document_buf(&doc).unwrap().into()))
            }
            None => Ok(GetNextResult::EOF),
        }
    }

    fn get_merging_stages(&mut self) -> Result<Vec<Document>, Error> {
        Ok(vec![
            doc! {"$sort": {"$meta": "searchScore"}}
        ])
    }
}

impl InternalPluginSearch {
    /// Performs an initial query to mongot and if results are not exhausted in the first batch,
    /// starts a background getmore loop that is later terminated either when mongot returns
    /// cursor_id == 0 or when the shutdown signal comes from mongod when the pipeline
    /// limit is satisfied
    async fn start_fetching_results(&mut self) -> Result<(), Error> {
        // mongot expects all requests within stage execution
        // to be sent via a single bidirectional stream
        let (sender, receiver) = mpsc::channel(1);

        // execute the initial request to fetch first batch and obtain the cursor id
        sender
            .send(SearchCommand::Initial(InitialSearchCommand {
                search: self.context.collection.clone().expect("init verified collection name is present"),
                db: self.context.db.clone(),
                collection_uuid: self.context.collection_uuid.expect("verified collection UUID present at initialization"),
                query: self.query.clone(),
                // small batch_size is for test purposes, this allows us to send getMores
                cursor_options: Some(CursorOptions { batch_size: 5 }),
            }))
            .await
            .unwrap();

        let outbound_stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let response = self.client.search(outbound_stream).await.unwrap();
        let mut inbound_stream: Streaming<MongotCursorBatch> = response.into_inner();

        let cursor_id: u64 = if let Some(received) = inbound_stream.next().await {
            InternalPluginSearch::flush_batch_into_channel(
                received,
                self.result_tx.clone(),
                self.stored_source,
            )
            .await
        } else {
            0
        };

        if cursor_id == 0 {
            // if we have exhausted the cursor in the initial query, flush EOF and return
            InternalPluginSearch::flush_eof_into_channel(self.result_tx.clone()).await;
            return Ok(());
        }

        // init an async getmore prefetch loop
        let mut shutdown_rx = self.shutdown_rx.clone();
        let result_tx = self.result_tx.clone();
        let stored_source = self.stored_source;

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
                            cursor_options: Some(CursorOptions { batch_size: 5 }),
                        });

                        if let Err(err) = sender.send(request).await {
                            eprintln!("Failed to send request: {:?}", err);
                            InternalPluginSearch::flush_eof_into_channel(result_tx.clone()).await;
                            return;
                        }

                        if let Some(received) = inbound_stream.next().await {
                            exhausted = InternalPluginSearch::flush_batch_into_channel(
                                received, result_tx.clone(), stored_source).await == 0;
                        }
                    } => {
                        if exhausted {
                            InternalPluginSearch::flush_eof_into_channel(result_tx.clone()).await;
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
        stored_source: bool,
    ) -> u64 {
        if let Some(cursor) = received.unwrap().cursor {
            for result in &cursor.next_batch {
                let doc = if stored_source {
                    result.stored_source.clone().unwrap()
                } else {
                    doc! { "_id": result.id.clone(), "$searchScore": result.score }
                };
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

pub struct PluginSearchDescriptor;

impl AggregationStageDescriptor for PluginSearchDescriptor {
    fn name() -> &'static str {
        "$pluginSearch"
    }

    fn properties() -> AggregationStageProperties {
        // TODO: this should return the value value as the internal remote stage.
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::First,
            host_type: stage_constraints::HostTypeRequirement::AnyShard,
        }
    }
}

impl DesugarAggregationStageDescriptor for PluginSearchDescriptor {
    fn desugar(
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Vec<Document>, Error> {
        let query = match stage_definition {
            RawBsonRef::Document(doc) => doc.to_owned(),
            _ => {
                return Err(Error::new(
                    1,
                    "$pluginSearch stage definition must contain a document.".to_string(),
                ))
            }
        }
        .to_document()
        .unwrap();

        if query.get_bool("returnStoredSource").unwrap_or(false) {
            return Ok(vec![doc! {"$_internalPluginSearch": query}]);
        }

        Ok(vec![
            doc! {"$_internalPluginSearch": query},
            doc! {"$_internalSearchIdLookup": doc!{}},
        ])
    }
}