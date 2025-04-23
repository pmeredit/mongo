use crate::echo::EchoOxideDescriptor;
use crate::sdk::{
    stage_constraints, AggregationStageDescriptor, AggregationStageExecutor,
    AggregationStageProperties, DesugarAggregationStageDescriptor, Error, GetNextResult,
    HostAggregationStageExecutor, TransformAggregationStageDescriptor,
    TransformBoundAggregationStageDescriptor,
};

use bson::{doc, to_raw_document_buf, Document, RawBsonRef, RawDocument};
use serde::{Deserialize, Deserializer, Serialize};

/// Descriptor for the `$addSomeCrabs` transform stage.
///
/// The stage definition is just a number that can be losslessly converted to a non-zero integer, eg
///   {$addSomeCrabs: 2}
/// The stage will then add a "someCrabs" field containing that number of crab emojis.
pub struct AddSomeCrabsDescriptor;

impl AggregationStageDescriptor for AddSomeCrabsDescriptor {
    fn name() -> &'static str {
        "$addSomeCrabs"
    }

    fn properties(&self) -> AggregationStageProperties {
        AggregationStageProperties {
            stream_type: stage_constraints::StreamType::Streaming,
            position: stage_constraints::PositionRequirement::None,
            host_type: stage_constraints::HostTypeRequirement::None,
            can_run_on_shards_pipeline: true,
        }
    }
}

impl TransformAggregationStageDescriptor for AddSomeCrabsDescriptor {
    type BoundDescriptor = AddSomeCrabsBoundDescriptor;

    fn bind(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Self::BoundDescriptor, Error> {
        Ok(AddSomeCrabsBoundDescriptor {
            num_crabs: AddSomeCrabs::parse_stage_definition(stage_definition)?,
        })
    }
}

pub struct AddSomeCrabsBoundDescriptor {
    num_crabs: usize,
}

impl TransformBoundAggregationStageDescriptor for AddSomeCrabsBoundDescriptor {
    type Executor = AddSomeCrabs;

    fn create_executor(
        &self,
        source: HostAggregationStageExecutor,
    ) -> Result<Self::Executor, Error> {
        Ok(AddSomeCrabs::with_crabs(self.num_crabs, source))
    }
}

pub struct AddSomeCrabs {
    crabs: String,
    source: HostAggregationStageExecutor,
}

impl AddSomeCrabs {
    fn parse_stage_definition(stage_definition: RawBsonRef<'_>) -> Result<usize, Error> {
        match stage_definition {
            RawBsonRef::Int32(i) if i > 0 => Ok(i as usize),
            RawBsonRef::Int64(i) if i > 0 => Ok(i as usize),
            RawBsonRef::Double(i) if i >= 1.0 && i.fract() == 0.0 => Ok(i as usize),
            _ => Err(Error::new(
                1,
                "$addSomeCrabs should be followed with a positive integer value.",
            )),
        }
    }

    fn with_crabs(num_crabs: usize, source: HostAggregationStageExecutor) -> Self {
        Self {
            crabs: String::from_iter(std::iter::repeat('🦀').take(num_crabs)),
            source,
        }
    }
}

impl AggregationStageExecutor for AddSomeCrabs {
    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        match self.source.get_next()? {
            GetNextResult::Advanced(input_doc) => {
                let mut doc = Document::try_from(input_doc.as_ref()).unwrap();
                doc.insert("someCrabs", self.crabs.clone());
                Ok(GetNextResult::Advanced(
                    to_raw_document_buf(&doc).unwrap().into(),
                ))
            }
            source_result => Ok(source_result),
        }
    }
}

pub struct EchoWithSomeCrabsDescriptor;

impl AggregationStageDescriptor for EchoWithSomeCrabsDescriptor {
    fn name() -> &'static str {
        "$echoWithSomeCrabs"
    }

    fn properties(&self) -> AggregationStageProperties {
        EchoOxideDescriptor.properties()
    }
}

impl DesugarAggregationStageDescriptor for EchoWithSomeCrabsDescriptor {
    fn desugar(
        &self,
        stage_definition: RawBsonRef<'_>,
        _context: &RawDocument,
    ) -> Result<Vec<Document>, Error> {
        let input_stage_def: EchoWithSomeCrabsStageDefinition =
            bson::from_bson(stage_definition.try_into().map_err(|e| {
                Error::with_source(1, "Could not convert $echoWithSomeCrabs to bson", e)
            })?)
            .map_err(|e| Error::with_source(1, "Could not parse $echoWithSomeCrabs", e))?;

        let mut stages = vec![doc! { "$echoOxide": input_stage_def.document.clone() }];
        if input_stage_def.num_crabs > 0 {
            stages.push(doc! { "$addSomeCrabs": input_stage_def.num_crabs as i64 });
        }
        Ok(stages)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EchoWithSomeCrabsStageDefinition {
    document: Document,
    #[serde(deserialize_with = "numeric_to_usize")]
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
