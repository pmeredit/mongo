use bson::{Bson, Document, RawArrayBuf, Uuid};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::marker::PhantomData;
use std::sync::OnceLock;
use tokio::runtime::Runtime;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::Status;

pub(crate) static MONGOT_ENDPOINT: &str = "http://localhost:27030";
pub(crate) static RUNTIME_THREADS: usize = 4;
pub(crate) static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Serialize, Deserialize, Debug)]
pub struct MongotCursorBatch {
    pub ok: u8,
    pub errmsg: Option<String>,
    pub cursor: Option<MongotCursorResult>,
    pub explain: Option<Bson>,
    pub vars: Option<Bson>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MongotCursorResult {
    pub id: u64,
    #[serde(rename = "nextBatch")]
    pub next_batch: Vec<MongotResult>,
    ns: String,
    r#type: Option<ResultType>,
}

#[derive(Serialize, Deserialize, Debug)]
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
pub struct VectorSearchCommand {
    #[serde(rename = "vectorSearch")]
    pub vector_search: String,
    #[serde(rename = "$db")]
    pub db: String,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Uuid,
    pub path: String,
    #[serde(rename = "queryVector")]
    pub query_vector: RawArrayBuf,
    pub index: String,
    pub limit: i64,
    #[serde(rename = "numCandidates")]
    pub num_candidates: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SearchCommand {
    Initial(InitialSearchCommand),
    GetMore(GetMoreSearchCommand),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InitialSearchCommand {
    #[serde(rename = "search")]
    pub search: String,
    #[serde(rename = "$db")]
    pub db: String,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Uuid,
    #[serde(rename = "query")]
    pub query: Document,
    #[serde(rename = "cursorOptions")]
    pub cursor_options: Option<CursorOptions>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetMoreSearchCommand {
    #[serde(rename = "getMore")]
    pub cursor_id: u64,
    pub cursor_options: Option<CursorOptions>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CursorOptions {
    #[serde(rename = "batchSize")]
    pub batch_size: i64,
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
