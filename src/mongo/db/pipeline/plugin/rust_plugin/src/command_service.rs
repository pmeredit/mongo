//! Bindings for tonic/gRPC communication with `mongot`.
//!
//! This code is mostly generated but lightly modified since `mongot` does not use a pure
//! gRPC + protobuf interface.

/**
The file below is generated using tonic-build:

fn main() -> () {
    let service = tonic_build::manual::Service::builder()
        .package("mongodb")
        .name("CommandService")
        .method(
            tonic_build::manual::Method::builder()
                .name("vectorSearch")
                .route_name("vectorSearch")
                .input_type("VectorSearchCommand")
                .output_type("MongotCursorBatch")
                .codec_path("BsonCodec")
                .build(),
        )
        .method(
            tonic_build::manual::Method::builder()
                .name("search")
                .route_name("search")
                .input_type("SearchCommand")
                .output_type("MongotCursorBatch")
                .codec_path("BsonCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .build();
    tonic_build::manual::Builder::new()
        .out_dir("src")
        .compile(&\[service\]);
}

Generated client implementations.
 */
pub mod command_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        non_snake_case,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    use crate::mongot_client::{BsonCodec, MongotCursorBatch, SearchCommand, VectorSearchCommand};

    #[derive(Debug, Clone)]
    pub struct CommandServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CommandServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CommandServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CommandServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            CommandServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn vectorSearch(
            &mut self,
            request: impl tonic::IntoRequest<VectorSearchCommand>,
        ) -> std::result::Result<tonic::Response<MongotCursorBatch>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = BsonCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/mongodb.BsonCommandService/vectorSearch",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("mongodb.BsonCommandService", "vectorSearch"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn search(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = SearchCommand>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<MongotCursorBatch>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = BsonCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/mongodb.BsonCommandService/search",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("mongodb.BsonCommandService", "search"));
            self.inner.streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod command_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        non_snake_case,
        non_camel_case_types,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use crate::mongot_client::{BsonCodec, MongotCursorBatch, SearchCommand, VectorSearchCommand};

    /// Generated trait containing gRPC methods that should be implemented for use with CommandServiceServer.
    #[async_trait]
    pub trait CommandService: std::marker::Send + std::marker::Sync + 'static {
        async fn vectorSearch(
            &self,
            request: tonic::Request<VectorSearchCommand>,
        ) -> std::result::Result<tonic::Response<MongotCursorBatch>, tonic::Status>;
        /// Server streaming response type for the search method.
        type searchStream: tonic::codegen::tokio_stream::Stream<
            Item = std::result::Result<MongotCursorBatch, tonic::Status>,
        >
        + std::marker::Send
        + 'static;
        async fn search(
            &self,
            request: tonic::Request<tonic::Streaming<SearchCommand>>,
        ) -> std::result::Result<tonic::Response<Self::searchStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct CommandServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> CommandServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CommandServiceServer<T>
    where
        T: CommandService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/mongodb.CommandService/vectorSearch" => {
                    #[allow(non_camel_case_types)]
                    struct vectorSearchSvc<T: CommandService>(pub Arc<T>);
                    impl<
                        T: CommandService,
                    > tonic::server::UnaryService<VectorSearchCommand>
                    for vectorSearchSvc<T> {
                        type Response = MongotCursorBatch;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<VectorSearchCommand>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CommandService>::vectorSearch(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = vectorSearchSvc(inner);
                        let codec = BsonCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/mongodb.CommandService/search" => {
                    #[allow(non_camel_case_types)]
                    struct searchSvc<T: CommandService>(pub Arc<T>);
                    impl<
                        T: CommandService,
                    > tonic::server::StreamingService<SearchCommand> for searchSvc<T> {
                        type Response = MongotCursorBatch;
                        type ResponseStream = T::searchStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<SearchCommand>>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CommandService>::search(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = searchSvc(inner);
                        let codec = BsonCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for CommandServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "mongodb.CommandService";
    impl<T> tonic::server::NamedService for CommandServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
