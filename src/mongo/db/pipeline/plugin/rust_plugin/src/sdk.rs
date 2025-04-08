// TODO: break up sdk into sub-modules.

use std::ptr::NonNull;

use crate::{AggregationStage, Error, GetNextResult, PluginAggregationStage, VecByteBuf};

use bson::{
    doc, to_raw_document_buf, to_vec, Bson, Document, RawBsonRef, RawDocument, RawDocumentBuf,
};
use plugin_api_bindgen::{
    MongoExtensionAggregationStage, MongoExtensionAggregationStageDescriptor,
    MongoExtensionAggregationStageDescriptorVTable, MongoExtensionAggregationStageType,
    MongoExtensionBoundAggregationStageDescriptor,
    MongoExtensionBoundAggregationStageDescriptorVTable, MongoExtensionByteBuf,
    MongoExtensionByteView, MongoExtensionPortal,
};
use serde::{Deserialize, Serialize};

pub mod stage_constraints {
    use serde::{Deserialize, Serialize};

    /// How does this stage emit data?
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum StreamType {
        /// This stage generates output document-by-document, possibly filtering some documents.
        ///
        /// Source stages should always be `Streaming`.
        Streaming,
        /// This stage must consume all input from the previous stage before emitting any output.
        ///
        /// Examples of this type of stage are `$sort` and `$group`.
        Blocking,
    }

    /// Where is this stage allowed to appear in the pipeline?
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum PositionRequirement {
        /// This stage may appear anywhere in the pipeline.
        None,
        /// This must be the first stage in the pipeline.
        First,
        /// This must be the last stage in the pipeline.
        Last,
    }

    /// Where is this stage allowed to be executed?
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub enum HostTypeRequirement {
        /// This stage may be scheduled on any host.
        None,
        /// This stage must run on the host to which it was originally sent and cannot be forwarded
        /// to any remote host.
        LocalOnly,
        /// This stage must run exactly once but it can be forwarded to another host so long as
        /// this invariant is maintained.
        RunOnceAnyNode,
        /// This stage must run on a participating shard/data node.
        AnyShard,
        /// This stage must run in the router for the query.
        Router,
        /// This stage should be run on all participating data node hosts, primary and secondary.
        AllShards,
    }
}

/// Properties of an aggregation stage that are independent of the stage definition and pipeline
/// context.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregationStageProperties {
    pub stream_type: stage_constraints::StreamType,
    pub position: stage_constraints::PositionRequirement,
    pub host_type: stage_constraints::HostTypeRequirement,
}

/// Parent trait for an aggregation stage descriptor.
pub trait AggregationStageDescriptor {
    /// Return the name for this stage. Must begin with '$'.
    fn name() -> &'static str;

    /// Return the properties for this desugar stage.
    ///
    /// This value will be cached by the SDK so calls should be idempotent.
    fn properties(&self) -> AggregationStageProperties;
}

/// A trait for implementing a de-sugaring stage that expands a stage into one or more other stages
/// but does not have a concrete execution implementation of its own.
pub trait DesugarAggregationStageDescriptor: AggregationStageDescriptor {
    /// Desugar the stage definition into one or more stages using additional pipeline `context`.
    // TODO: consider returning Vec<RawDocumentBuf> or otherwise allowing rawdoc!() to define expansions.
    fn desugar(
        &self,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Vec<Document>, Error>;
}

/// A trait for implementing a concrete source aggregation stage that does not accept input docs.
pub trait SourceAggregationStageDescriptor: AggregationStageDescriptor {
    /// Associated type to describe a stage bound to the stage definition and pipeline context.
    type BoundDescriptor: SourceBoundAggregationStageDescriptor + Sized;

    /// Bind this stage to a stage definition and pipeline context.
    ///
    /// This step should parse and validate the stage definition so that it is prepared to optimize
    /// the query and/or create execution objects.
    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error>;
}

/// A trait for implementing a concrete transform aggregation stage that reads input documents.
pub trait TransformAggregationStageDescriptor: AggregationStageDescriptor {
    /// Associated type to describe a stage bound to the stage definition and pipeline context.
    type BoundDescriptor: TransformBoundAggregationStageDescriptor + Sized;

    /// Bind this stage to a stage definition and pipeline context.
    ///
    /// This step should parse and validate the stage definition so that it is prepared to optimize
    /// the query and/or create execution objects.
    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error>;
}

/// A descriptor for a source stage bound to stage definition and pipeline context.
pub trait SourceBoundAggregationStageDescriptor {
    /// Associated type for the execution object.
    ///
    /// This object will be wrapped so that it can be used by the extension host.
    type Executor: AggregationStage + Sized;

    /// Return a pipeline fragment used to merge the output of this stage in a sharded query.
    ///
    /// If this returns an empty list of stages then result documents are merged by a well defined
    /// shard key. This is the average case; most won't need to override this.
    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        Ok(vec![])
    }

    /// Create a new executor based on bound state from creation.
    fn create_executor(&self) -> Result<Self::Executor, Error>;
}

/// A descriptor for a transform stage bound to stage definition and pipeline context.
pub trait TransformBoundAggregationStageDescriptor {
    /// Associated type for the execution object.
    ///
    /// This object will be wrapped so that it can be used by the extension host.
    type Executor: AggregationStage + Sized;

    /// Return a pipeline fragment used to merge the output of this stage in a sharded query.
    ///
    /// If this returns an empty list of stages then result documents are merged by a well defined
    /// shard key. This is the average case; most won't need to override this.
    fn get_merging_stages(&self) -> Result<Vec<Document>, Error> {
        Ok(vec![])
    }

    /// Create a new executor based on bound state from creation.
    fn create_executor(
        &self,
        source: HostAggregationStageExecutor,
    ) -> Result<Self::Executor, Error>;
}

/// Bindings for `MongoExtensionPortal`.
pub struct ExtensionPortal(NonNull<MongoExtensionPortal>);

impl ExtensionPortal {
    /// Create a new `ExtensionPortal` from a raw pointer. Does not take ownership of the pointer.
    pub fn from_raw(p: *mut MongoExtensionPortal) -> Option<Self> {
        NonNull::new(p).map(Self)
    }

    pub fn register_desugar_aggregation_stage<D: DesugarAggregationStageDescriptor>(
        &mut self,
        descriptor: D,
    ) {
        self.register_stage(
            ExtensionAggregationStageDescriptor::from_desugar_descriptor(descriptor),
        );
    }

    pub fn register_source_aggregation_stage<D: SourceAggregationStageDescriptor>(
        &mut self,
        descriptor: D,
    ) {
        self.register_stage(ExtensionAggregationStageDescriptor::from_source_descriptor(
            descriptor,
        ));
    }

    pub fn register_transform_aggregation_stage<D: TransformAggregationStageDescriptor>(
        &mut self,
        descriptor: D,
    ) {
        self.register_stage(
            ExtensionAggregationStageDescriptor::from_transform_descriptor(descriptor),
        );
    }

    fn register_stage<D: AggregationStageDescriptor>(
        &mut self,
        descriptor: ExtensionAggregationStageDescriptor<D>,
    ) {
        let descriptor = Box::new(descriptor);
        unsafe {
            self.0
                .as_ref()
                .registerStageDescriptor
                .expect("registerStageDescriptor")(
                MongoExtensionByteView {
                    data: D::name().as_ptr(),
                    len: D::name().len(),
                },
                Box::into_raw(descriptor) as *const MongoExtensionAggregationStageDescriptor,
            );
        }
    }
}

mod ffi_utils {
    use crate::Error;

    use bson::{RawBsonRef, RawDocument};
    use plugin_api_bindgen::MongoExtensionByteView;

    pub fn view_to_raw_doc<'a>(
        view: MongoExtensionByteView,
        context: &str,
    ) -> Result<&'a RawDocument, Error> {
        // TODO: we should be able to attach context to an error at the caller.
        RawDocument::from_bytes(unsafe { std::slice::from_raw_parts(view.data, view.len) })
            .map_err(|e| Error::with_source(1, format!("Error parsing {} document", context), e))
    }

    pub fn view_to_raw_first_value<'a>(
        view: MongoExtensionByteView,
        context: &str,
    ) -> Result<RawBsonRef<'a>, Error> {
        let doc = view_to_raw_doc(view, context)?;
        doc.iter()
            .next()
            .map(|elem| {
                elem.map(|(_, value)| value).map_err(|e| {
                    Error::with_source(
                        1,
                        format!("Error parsing first {} document element", context),
                        e,
                    )
                })
            })
            .unwrap_or(Err(Error::new(1, format!("{} document is empty", context))))
    }
}

// TODO: "Extension" structs are an implementation detail for ffi bindings.
// These could be hidden in another module, which might help with the java-esque names.

#[repr(C)]
struct ExtensionAggregationStageDescriptor<D: AggregationStageDescriptor> {
    vtable: &'static MongoExtensionAggregationStageDescriptorVTable,
    stage_type: MongoExtensionAggregationStageType,
    properties: RawDocumentBuf,
    descriptor: D,
}

impl<D: AggregationStageDescriptor> ExtensionAggregationStageDescriptor<D> {
    unsafe extern "C-unwind" fn external_stage_type(
        descp: *const MongoExtensionAggregationStageDescriptor,
    ) -> MongoExtensionAggregationStageType {
        let desc = (descp as *const Self)
            .as_ref()
            .expect("descriptor non-null");
        desc.stage_type
    }

    unsafe extern "C-unwind" fn external_properties(
        descp: *const MongoExtensionAggregationStageDescriptor,
    ) -> MongoExtensionByteView {
        let desc = (descp as *const Self)
            .as_ref()
            .expect("descriptor non-null");
        MongoExtensionByteView {
            data: desc.properties.as_bytes().as_ptr(),
            len: desc.properties.as_bytes().len(),
        }
    }

    fn serialized_properties(descriptor: &D) -> RawDocumentBuf {
        to_raw_document_buf(&descriptor.properties())
            .expect("stage properties serialize successfully")
    }
}

impl<D: DesugarAggregationStageDescriptor> ExtensionAggregationStageDescriptor<D> {
    const DESUGAR_VTABLE: MongoExtensionAggregationStageDescriptorVTable =
        MongoExtensionAggregationStageDescriptorVTable {
            type_: Some(Self::external_stage_type),
            properties: Some(Self::external_properties),
            // TODO bind should return an unimplemented error.
            bind: None,
            desugar: Some(Self::external_desugar),
        };

    fn from_desugar_descriptor(descriptor: D) -> Self {
        Self {
            vtable: &Self::DESUGAR_VTABLE,
            stage_type: plugin_api_bindgen::MongoExtensionAggregationStageType_kDesugar,
            properties: Self::serialized_properties(&descriptor),
            descriptor,
        }
    }

    unsafe extern "C-unwind" fn external_desugar(
        descriptor: *const MongoExtensionAggregationStageDescriptor,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
        result: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ext_descriptor = (descriptor as *const Self)
            .as_ref()
            .expect("descriptor ptr non-null");
        let (buf, code) = match ext_descriptor.desugar_internal(stage_bson, context_bson) {
            Ok(d) => (VecByteBuf::from_vec(d.into_bytes()), 0),
            Err(e) => (VecByteBuf::from_string(e.to_string()), e.code.into()),
        };
        *result = buf.into_byte_buf();
        code
    }

    fn desugar_internal(
        &self,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
    ) -> Result<RawDocumentBuf, Error> {
        let name = D::name();
        let stage_bson = ffi_utils::view_to_raw_first_value(stage_bson, "stage definition")?;
        let context_bson = ffi_utils::view_to_raw_doc(context_bson, "context")?;
        let mut desugared_stages = self.descriptor.desugar(stage_bson, context_bson)?;
        let desugared_doc = match desugared_stages.len() {
            0 => Err(Error::new(
                1,
                format!("desugaring stage [{}] resulted in empty expansion", name),
            )),
            1 => Ok(Document::from_iter([(
                name.into(),
                desugared_stages.pop().unwrap().into(),
            )])),
            _ => Ok(Document::from_iter([(
                name.into(),
                Bson::Array(desugared_stages.into_iter().map(Bson::from).collect()),
            )])),
        }?;
        to_raw_document_buf(&desugared_doc).map_err(|e| {
            Error::with_source(
                1,
                format!("Error serializing desugared stages from {}", name),
                e,
            )
        })
    }
}

impl<D: SourceAggregationStageDescriptor> ExtensionAggregationStageDescriptor<D> {
    const SOURCE_VTABLE: MongoExtensionAggregationStageDescriptorVTable =
        MongoExtensionAggregationStageDescriptorVTable {
            type_: Some(Self::external_stage_type),
            properties: Some(Self::external_properties),
            bind: Some(Self::external_bind_source),
            // TODO: desugar should return an unimplemented error.
            desugar: None,
        };

    fn from_source_descriptor(descriptor: D) -> Self {
        Self {
            vtable: &Self::SOURCE_VTABLE,
            stage_type: plugin_api_bindgen::MongoExtensionAggregationStageType_kSource,
            properties: Self::serialized_properties(&descriptor),
            descriptor,
        }
    }

    unsafe extern "C-unwind" fn external_bind_source(
        descriptor: *const MongoExtensionAggregationStageDescriptor,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
        bound_stage: *mut *mut MongoExtensionBoundAggregationStageDescriptor,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ext_descriptor = (descriptor as *const Self)
            .as_ref()
            .expect("descriptor ptr non-null");
        match ext_descriptor.bind_source_internal(stage_bson, context_bson) {
            Ok(d) => {
                *bound_stage = Box::new(ExtensionSourceBoundAggregationStageDescriptor::new(d))
                    .into_raw_interface();
                0
            }
            Err(e) => {
                *error = VecByteBuf::from_string(e.to_string()).into_byte_buf();
                e.code.into()
            }
        }
    }

    fn bind_source_internal(
        &self,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
    ) -> Result<D::BoundDescriptor, Error> {
        let stage_bson = ffi_utils::view_to_raw_first_value(stage_bson, "stage definition")?;
        let context_bson = ffi_utils::view_to_raw_doc(context_bson, "context")?;
        self.descriptor.bind(stage_bson, context_bson)
    }
}

impl<D: TransformAggregationStageDescriptor> ExtensionAggregationStageDescriptor<D> {
    const TRANSFORM_VTABLE: MongoExtensionAggregationStageDescriptorVTable =
        MongoExtensionAggregationStageDescriptorVTable {
            type_: Some(Self::external_stage_type),
            properties: Some(Self::external_properties),
            bind: Some(Self::external_bind_transform),
            // TODO: desugar should return an unimplemented error.
            desugar: None,
        };

    fn from_transform_descriptor(descriptor: D) -> Self {
        Self {
            vtable: &Self::TRANSFORM_VTABLE,
            stage_type: plugin_api_bindgen::MongoExtensionAggregationStageType_kTransform,
            properties: Self::serialized_properties(&descriptor),
            descriptor,
        }
    }

    unsafe extern "C-unwind" fn external_bind_transform(
        descriptor: *const MongoExtensionAggregationStageDescriptor,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
        bound_stage: *mut *mut MongoExtensionBoundAggregationStageDescriptor,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ext_descriptor = (descriptor as *const Self)
            .as_ref()
            .expect("descriptor ptr non-null");
        match ext_descriptor.bind_transform_internal(stage_bson, context_bson) {
            Ok(d) => {
                *bound_stage = Box::new(ExtensionTransformBoundAggregationStageDescriptor::new(d))
                    .into_raw_interface();
                0
            }
            Err(e) => {
                *error = VecByteBuf::from_string(e.to_string()).into_byte_buf();
                e.code.into()
            }
        }
    }

    fn bind_transform_internal(
        &self,
        stage_bson: MongoExtensionByteView,
        context_bson: MongoExtensionByteView,
    ) -> Result<D::BoundDescriptor, Error> {
        let stage_bson = ffi_utils::view_to_raw_first_value(stage_bson, "stage definition")?;
        let context_bson = ffi_utils::view_to_raw_doc(context_bson, "context")?;
        self.descriptor.bind(stage_bson, context_bson)
    }
}

fn serialize_merging_stages(stages: Vec<Document>) -> Result<Vec<u8>, Error> {
    to_vec(&doc! {
        "mergingStages":
        Bson::Array(
            stages
                .into_iter()
                .map(Bson::from)
                .collect(),
        ),
    })
    .map_err(|e| Error::with_source(1, "failed to serialize merging stages", e))
}

fn process_merging_stages(
    result: Result<Vec<Document>, Error>,
) -> (std::os::raw::c_int, Box<VecByteBuf>) {
    match result.and_then(serialize_merging_stages) {
        Ok(s) => (0, VecByteBuf::from_vec(s)),
        Err(e) => (e.code.into(), VecByteBuf::from_string(e.to_string())),
    }
}

#[repr(C)]
struct ExtensionSourceBoundAggregationStageDescriptor<D> {
    vtable: &'static MongoExtensionBoundAggregationStageDescriptorVTable,
    descriptor: D,
}

impl<D: SourceBoundAggregationStageDescriptor + Sized>
    ExtensionSourceBoundAggregationStageDescriptor<D>
{
    const VTABLE: MongoExtensionBoundAggregationStageDescriptorVTable =
        MongoExtensionBoundAggregationStageDescriptorVTable {
            drop: Some(Self::drop),
            getMergingStages: Some(Self::get_merging_stages),
            createExecutor: Some(Self::create_executor),
        };

    fn new(descriptor: D) -> Self {
        Self {
            vtable: &Self::VTABLE,
            descriptor,
        }
    }

    fn into_raw_interface(self: Box<Self>) -> *mut MongoExtensionBoundAggregationStageDescriptor {
        Box::into_raw(self) as *mut MongoExtensionBoundAggregationStageDescriptor
    }

    unsafe extern "C-unwind" fn drop(descp: *mut MongoExtensionBoundAggregationStageDescriptor) {
        let _ = Box::from_raw(descp as *mut Self);
    }

    unsafe extern "C-unwind" fn get_merging_stages(
        descp: *const MongoExtensionBoundAggregationStageDescriptor,
        result: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ffi_desc = (descp as *const Self)
            .as_ref()
            .expect("descriptor non-null");
        let (code, buf) = process_merging_stages(ffi_desc.descriptor.get_merging_stages());
        *result = buf.into_byte_buf();
        code
    }

    unsafe extern "C-unwind" fn create_executor(
        descp: *mut MongoExtensionBoundAggregationStageDescriptor,
        source: *mut MongoExtensionAggregationStage,
        executor: *mut *mut MongoExtensionAggregationStage,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ext_descriptor = (descp as *mut Self).as_mut().expect("descriptor non-null");
        match ext_descriptor
            .create_executor_internal(HostAggregationStageExecutor::from_raw(source))
        {
            Ok(e) => {
                *executor = Box::new(PluginAggregationStage::new(e)).into_raw_interface();
                0
            }
            Err(e) => {
                *error = VecByteBuf::from_string(e.to_string()).into_byte_buf();
                e.code.into()
            }
        }
    }

    fn create_executor_internal(
        &mut self,
        source: Option<HostAggregationStageExecutor>,
    ) -> Result<D::Executor, Error> {
        if source.is_some() {
            return Err(Error::new(
                1,
                "create_executor on source bound descriptor received a source stage",
            ));
        }

        self.descriptor.create_executor()
    }
}

#[repr(C)]
struct ExtensionTransformBoundAggregationStageDescriptor<D> {
    vtable: &'static MongoExtensionBoundAggregationStageDescriptorVTable,
    descriptor: D,
}

impl<D: TransformBoundAggregationStageDescriptor + Sized>
    ExtensionTransformBoundAggregationStageDescriptor<D>
{
    const VTABLE: MongoExtensionBoundAggregationStageDescriptorVTable =
        MongoExtensionBoundAggregationStageDescriptorVTable {
            drop: Some(Self::drop),
            getMergingStages: Some(Self::get_merging_stages),
            createExecutor: Some(Self::create_executor),
        };

    fn new(descriptor: D) -> Self {
        Self {
            vtable: &Self::VTABLE,
            descriptor,
        }
    }

    fn into_raw_interface(self: Box<Self>) -> *mut MongoExtensionBoundAggregationStageDescriptor {
        Box::into_raw(self) as *mut MongoExtensionBoundAggregationStageDescriptor
    }

    unsafe extern "C-unwind" fn drop(descp: *mut MongoExtensionBoundAggregationStageDescriptor) {
        let _ = Box::from_raw(descp as *mut Self);
    }

    unsafe extern "C-unwind" fn get_merging_stages(
        descp: *const MongoExtensionBoundAggregationStageDescriptor,
        result: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ffi_desc = (descp as *const Self)
            .as_ref()
            .expect("descriptor non-null");
        let (code, buf) = process_merging_stages(ffi_desc.descriptor.get_merging_stages());
        *result = buf.into_byte_buf();
        code
    }

    unsafe extern "C-unwind" fn create_executor(
        descp: *mut MongoExtensionBoundAggregationStageDescriptor,
        source: *mut MongoExtensionAggregationStage,
        executor: *mut *mut MongoExtensionAggregationStage,
        error: *mut *mut MongoExtensionByteBuf,
    ) -> std::os::raw::c_int {
        let ext_descriptor = (descp as *mut Self).as_mut().expect("descriptor non-null");
        match ext_descriptor
            .create_executor_internal(HostAggregationStageExecutor::from_raw(source))
        {
            Ok(e) => {
                *executor = Box::new(PluginAggregationStage::new(e)).into_raw_interface();
                0
            }
            Err(e) => {
                *error = VecByteBuf::from_string(e.to_string()).into_byte_buf();
                e.code.into()
            }
        }
    }

    fn create_executor_internal(
        &mut self,
        source: Option<HostAggregationStageExecutor>,
    ) -> Result<D::Executor, Error> {
        let source = source.ok_or_else(|| {
            Error::new(
                1,
                "create_executor on transform bound descriptor must receive a source stage",
            )
        })?;
        self.descriptor.create_executor(source)
    }
}

/// Interface to an stage owned by the host, used by transform stages.
pub struct HostAggregationStageExecutor(NonNull<MongoExtensionAggregationStage>);

impl HostAggregationStageExecutor {
    pub fn from_raw(ptr: *mut MongoExtensionAggregationStage) -> Option<Self> {
        NonNull::new(ptr).map(Self)
    }
}

impl AggregationStage for HostAggregationStageExecutor {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        let mut doc_view = MongoExtensionByteView {
            data: std::ptr::null(),
            len: 0,
        };
        match unsafe {
            self.0
                .as_mut()
                .vtable
                .as_ref()
                .expect("non-nullptr vtable")
                .get_next
                .expect("non-nullptr executor get_next")(self.0.as_mut(), &mut doc_view)
        } {
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_ADVANCED => {
                let raw_doc = RawDocument::from_bytes(unsafe {
                    std::slice::from_raw_parts(doc_view.data, doc_view.len)
                })
                .map_err(|e| {
                    Error::with_source(1, "Host stage provided invalid BSON document", e)
                })?;
                Ok(GetNextResult::Advanced(raw_doc.into()))
            }
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_EOF => Ok(GetNextResult::EOF),
            plugin_api_bindgen::mongodb_get_next_result_GET_NEXT_PAUSE_EXECUTION => {
                Ok(GetNextResult::PauseExecution)
            }
            code => {
                // TODO: this should pass through an external error without coercing it to an
                // internal type to better handle host-generated exceptions. This requires changes
                // across the extension API, host code, and SDK.
                Err(Error::new(
                    code,
                    String::from_utf8_lossy(unsafe {
                        std::slice::from_raw_parts(doc_view.data, doc_view.len)
                    }),
                ))
            }
        }
    }
}

impl Drop for HostAggregationStageExecutor {
    fn drop(&mut self) {
        unsafe {
            self.0
                .as_mut()
                .vtable
                .as_ref()
                .expect("non-nullptr vtable")
                .close
                .expect("non-nullptr executor close")(self.0.as_mut());
        }
    }
}
