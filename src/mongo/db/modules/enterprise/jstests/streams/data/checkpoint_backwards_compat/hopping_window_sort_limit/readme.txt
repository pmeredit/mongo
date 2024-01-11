This directory contains a checkpoint and other relevant info that was taken from a sort-limit hopping window pipeline.
- inputDocs.bson: The input docs used in the test
- expectedResults.bson: Expected results when the test resumes from the saved checkpoint and finishes
- jstests-tenant/resume_from_checkpoint_test_spid/1704760144085 : This is the checkpoint state that the test will be resuming from
- manifest.bson: This is a copy of the checkpoint MANIFEST file with the 4 byte chksum removed. (Our javascript test environment can only read bson files). We do some basic sanity testing by verifying some fields in the manifest
