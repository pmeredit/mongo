use crate::{AggregationSource, AggregationStage, Error, GetNextResult};
use bson::{doc, to_raw_document_buf};
use bson::{Document, RawBsonRef, RawDocument};
use std::collections::VecDeque;

pub struct PluginSort {
    field: String,
    source: Option<AggregationSource>,
    docs: Option<VecDeque<Document>>,
}

/**
 * $pluginSort accepts one field by which to sort the document; the field values must be numeric.
 */
impl AggregationStage for PluginSort {
    fn name() -> &'static str {
        "$pluginSort"
    }

    fn new(stage_definition: RawBsonRef<'_>, _context: &RawDocument) -> Result<Self, Error> {
        let field = match stage_definition {
            RawBsonRef::String(i) => i.to_string(),
            _ => {
                return Err(Error::new(
                    1,
                    "$pluginSort should be followed with a string.",
                ))
            }
        };
        Ok(Self {
            field,
            source: None,
            docs: None,
        })
    }

    fn set_source(&mut self, source: AggregationSource) {
        self.source = Some(source);
    }

    fn get_next(&mut self) -> Result<GetNextResult<'_>, Error> {
        if self.docs.is_none() {
            Self::populate_and_sort_documents(self)?;
        }

        match self.docs.as_mut() {
            Some(documents) => {
                if documents.is_empty() {
                    return Ok(GetNextResult::EOF);
                }

                let next = documents.pop_front().unwrap();
                Ok(GetNextResult::Advanced(
                    to_raw_document_buf(&next).unwrap().into(),
                ))
            }
            None => Ok(GetNextResult::EOF),
        }
    }

    fn get_merging_stages(&mut self) -> Result<Vec<Document>, Error> {
        Ok(vec![doc! {"$sort": {self.field.clone(): 1}}])
    }
}

impl PluginSort {
    fn populate_and_sort_documents(&mut self) -> Result<(), Error> {
        let source = self
            .source
            .as_mut()
            .expect("intermediate stage must have source");

        // Retrieve all results from the source stage.
        let mut documents = VecDeque::new();
        while let GetNextResult::Advanced(input_doc) = source.get_next()? {
            let doc = Document::try_from(input_doc.as_ref()).unwrap();
            documents.push_back(doc);
        }

        // Sort the documents by the provided field.
        documents.make_contiguous().sort_by(|doc_a, doc_b| {
            // TODO Properly handle errors when field has non-numeric value.
            // TODO This could be more efficient by retrieving and converting the values in advance..
            let val_a = doc_a.get(self.field.clone()).unwrap().as_f64().unwrap();
            let val_b = doc_b.get(self.field.clone()).unwrap().as_f64().unwrap();
            val_a.total_cmp(&val_b)
        });

        self.docs = Some(documents);
        Ok(())
    }
}
