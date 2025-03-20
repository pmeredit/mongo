use std::ffi::c_int;
use std::marker::PhantomData;

use super::{Error, VecByteBuf};
use bson::{to_vec, Bson, Document, RawBsonRef, RawDocument};
use plugin_api_bindgen::{MongoExtensionByteBuf, MongoExtensionByteView, MongoExtensionPortal};
use serde::{Deserialize, Deserializer, Serialize};

/// Trait for implementing de-sugaring of a stage.
pub trait DesugarAggregationStage {
    /// Name to register the stage as. Must begin with $.
    fn name() -> &'static str;

    /// Expand `stage_definition` into one or more other stages.
    // TODO: consider adjusting the API so that you must provide _at least_ one output stage.
    fn desugar(stage_definition: RawBsonRef<'_>) -> Result<Vec<Document>, Error>;
}

/// Concrete wrapper for a desugar stage.
///
/// This struct generates C bindings for plugin registration.
pub struct PluginDesugarAggregationStage<D>(PhantomData<D>);

impl<D: DesugarAggregationStage> PluginDesugarAggregationStage<D> {
    unsafe extern "C-unwind" fn parse_external(
        stage_bson: MongoExtensionByteView,
        result: *mut *mut MongoExtensionByteBuf,
    ) -> c_int {
        match Self::parse(std::slice::from_raw_parts(stage_bson.data, stage_bson.len)) {
            Ok(buf) => {
                *result = buf.into_byte_buf();
                0
            }
            Err(e) => {
                *result = VecByteBuf::from_string(e.to_string()).into_byte_buf();
                e.code.into()
            }
        }
    }

    fn parse(stage_bson_bytes: &[u8]) -> Result<Box<VecByteBuf>, Error> {
        let name = D::name();
        let stage_bson = RawDocument::from_bytes(stage_bson_bytes).map_err(|e| {
            Error::with_source(
                1,
                format!("Could not parse raw stage definition for [{}]", name),
                Box::new(e),
            )
        })?;
        // Note that it is impossible to trigger the expect: we could not invoke the plugin if there
        // isn't a single element containing the name.
        let stage_element = stage_bson
            .iter()
            .next()
            .expect("cannot desugar an empty stage definition")
            .map_err(|e| {
                Error::with_source(
                    1,
                    format!("Could not decode stage definition for [{}]", name),
                    e,
                )
            })?;
        let mut desugared_stages = D::desugar(stage_element.1)?;
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
        to_vec(&desugared_doc)
            .map(VecByteBuf::from_vec)
            .map_err(|e| {
                Error::with_source(
                    1,
                    format!("Could not serialize desugared stage for [{}]", name),
                    e,
                )
            })
    }

    pub fn register(portal: *mut MongoExtensionPortal) {
        let name = D::name();
        let name_view = MongoExtensionByteView {
            data: name.as_bytes().as_ptr(),
            len: name.as_bytes().len(),
        };
        unsafe {
            (*portal).add_desugar_stage.expect("add_desugar_stage")(
                name_view,
                Some(Self::parse_external),
            )
        }
    }
}

#[derive(Serialize, Deserialize)]
struct EchoWithSomeCrabsStageDefinition {
    document: Document,
    #[serde(rename = "numCrabs", deserialize_with = "numeric_to_usize")]
    num_crabs: usize,
}

fn numeric_to_usize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let value = f64::deserialize(deserializer)?;
    if value >= 0.0 && value.fract() == 0.0 && value < usize::MAX as f64 {
        Ok(value as usize)
    } else {
        use serde::de::Error;
        Err(D::Error::custom(format!(
            "Could not coerce float value {} to usize",
            value
        )))
    }
}

pub struct EchoWithSomeCrabs;

impl DesugarAggregationStage for EchoWithSomeCrabs {
    fn name() -> &'static str {
        "$echoWithSomeCrabs"
    }

    fn desugar(stage_definition: RawBsonRef<'_>) -> Result<Vec<Document>, Error> {
        let input_stage_def: EchoWithSomeCrabsStageDefinition =
            bson::from_bson(stage_definition.try_into().map_err(|e| {
                Error::with_source(1, "Could not convert $echoWithSomeCrabs to bson", e)
            })?)
            .map_err(|e| Error::with_source(1, "Could not parse $echoWithSomeCrabs", e))?;

        let mut stages = vec![Document::from_iter([(
            "$echoOxide".into(),
            input_stage_def.document.clone().into(),
        )])];
        if input_stage_def.num_crabs > 0 {
            stages.push(Document::from_iter([(
                "$addSomeCrabs".into(),
                (input_stage_def.num_crabs as i64).into(),
            )]))
        }
        Ok(stages)
    }
}
