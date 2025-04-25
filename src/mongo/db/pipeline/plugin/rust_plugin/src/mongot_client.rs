//! Utilities and protocol structures for talking to the search service, `mongot`.
use std::io::Write;
use std::marker::PhantomData;

use bson::oid::ObjectId;
use bson::{Bson, Document, RawArrayBuf, Uuid};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

use crate::LazyRuntime;

/// State needed to talk to a remote `mongot` service. This may be shared among stages that need to
/// talk to `mongot`.
pub struct MongotClientState {
    // TODO: consider initializing runtime up-front rather than deferring until first use.
    //
    // Stage descriptors are registered by mongo global initializers, which are run before the
    // signal processing thread is setup. This causes threads created during global initializers to
    // have an incorrect signal mask, so they may capture SIGTERM and other signals that they should
    // not, which may cause the wrong signal handler to be invoked. This may be fixed by dynamic
    // loading as we would likely choose to perform that task after the signal handling thread is
    // started. Lazy initialization does have an upside in that nodes that do not execute queries
    // will not create threads, which is not possible to avoid otherwise with current APIs.
    runtime: LazyRuntime,
    // TODO: this should also contain a client connection/channel, but not until client state is
    // optional in the descriptor. Ideally we would only create mongot connections from hosts that
    // are executing queries; some hosts only participate in planning.
}

impl MongotClientState {
    pub fn new(runtime_threads: usize) -> Self {
        Self {
            runtime: LazyRuntime::new("search-extension", runtime_threads),
        }
    }

    pub fn runtime(&self) -> &Runtime {
        self.runtime.get()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MongotCursorBatch {
    pub ok: u8,
    pub errmsg: Option<String>,
    pub cursor: Option<MongotCursorResult>,
    pub cursors: Option<Vec<MongotCursorBatch>>,
    pub explain: Option<Bson>,
    pub vars: Option<Bson>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MongotCursorResult {
    pub id: u64,
    pub next_batch: Vec<Document>,
    pub ns: String,
    pub r#type: Option<ResultType>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ResultType {
    Results,
    Meta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MongotResult {
    #[serde(rename = "_id")]
    pub id: Option<Bson>,
    #[serde(alias = "$searchScore", rename = "$vectorSearchScore")]
    #[serde(skip_serializing)]
    pub score: f32,
    #[serde(rename = "storedSource")]
    pub stored_source: Option<Document>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchCommand {
    pub vector_search: String,
    #[serde(rename = "$db")]
    pub db: String,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Uuid,
    pub path: String,
    pub query_vector: RawArrayBuf,
    pub index: String,
    pub limit: i64,
    pub num_candidates: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SearchCommand {
    Initial(InitialSearchCommand),
    GetMore(GetMoreSearchCommand),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct InitialSearchCommand {
    pub search: String,
    #[serde(rename = "$db")]
    pub db: String,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Uuid,
    pub query: Document,
    pub cursor_options: Option<CursorOptions>,
    pub intermediate: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetMoreSearchCommand {
    #[serde(rename = "getMore")]
    pub cursor_id: u64,
    pub cursor_options: Option<CursorOptions>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CursorOptions {
    pub batch_size: i64,
    // Allows synchronizing between stages by passing a shared token in independent requests
    // to mongot. This ensures that lucene query is executed only once, allowing mongot cursors
    // to be reused for fetching documents and metadata in separate mongod stages.
    pub lookup_token: Option<ObjectId>,
     // This flag is potentially a future replacement of the intermediate flag. It signals mongot to
     // always deliver metadata in a separate cursor (even on non-sharded when meta is accumulated 
     // into one doc), because that's the conventient way for extensions to set $$SEARCH_META
    pub metadata: MetadataMode,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum MetadataMode {
    // No metadata is expected, mongot should return only documents
    NONE,
    // All metadata should be returned via a separate cursor, accumulation should happen on mongod
    // side (used in sharded setup)
    ALL,
    // Metadata should be accumulated and returned as a separate cursor. That cursor must return
    // only one document.
    ACCUMULATED
}

#[derive(Debug)]
pub struct BsonEncoder<T>(PhantomData<T>);

impl<T: serde::Serialize> Encoder for BsonEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let bytes: Vec<u8> = bson::to_vec(&item).map_err(|e| Status::internal(e.to_string()))?;
        buf.writer()
            .write_all(&bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct BsonDecoder<U>(PhantomData<U>);

impl<U: serde::de::DeserializeOwned> Decoder for BsonDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        let item: Self::Item =
            bson::from_reader(buf.reader()).map_err(|e| Status::internal(e.to_string()))?;

        Ok(Some(item))
    }
}

#[derive(Debug, Clone)]
pub struct BsonCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for BsonCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for BsonCodec<T, U>
where
    T: serde::Serialize + Send + 'static,
    U: serde::de::DeserializeOwned + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = BsonEncoder<T>;
    type Decoder = BsonDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        BsonEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        BsonDecoder(PhantomData)
    }
}
