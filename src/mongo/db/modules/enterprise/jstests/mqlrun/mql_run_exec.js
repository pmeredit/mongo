// Given 'inputFileName', a path to a BSON file, and the array 'pipeline', executes the pipeline
// against the BSON file using mqlrun and returns the result set.
//
// If 'outputBson' is false (the default), then the results are returned as an array of relaxed JSON
// strings, one per document in the result set. These strings are relaxed in that they do not have
// quotes around property names, and may use strings like "ObjectId('5bd761dcae323e45a93ccfe8') or
// "new Date(1427144809506)" to represent special BSON types. As a future improvement (see
// SERVER-45247), we could change mqlrun to produce output as strict extended JSON strings.
//
// If 'outputBson' is true, instructs mqlrun to return the result set as BSON.
//
// If 'allowSpillToDisk' is true, then mqlrun is configured with the temp file directory for
// spilling to disk.
//
// Asserts that mqlrun returns with exit code 'expectedReturnCode'. Returns null if 'outputBson' is
// true, and an array of strings with mqlrun output otherwise. As a future improvement, we could
// improve the test machinery to be able to process BSON-formatted output produced by mqlrun.
export function mqlrunExec(
    inputFileName,
    pipeline,
    {outputBson = false, allowSpillToDisk = true, expectedReturnCode = 0} = {}) {
    clearRawMongoProgramOutput();

    // Temp directory for mqlrun.
    const tempDir = MongoRunner.dataPath + '_mqlrun';
    resetDbpath(tempDir);

    // Convert 'pipeline' to a JSON string so that it can be passed to mqlrun as command line
    // argument. On Windows, the shell isn't happy with double quotes, so we replace them with
    // single quotes.
    const stringifiedPipeline = JSON.stringify(pipeline).replace(/"/g, "'");

    const mqlrunExecutableName = _isWindows() ? "mqlrun.exe" : "mqlrun";
    const outputFormatFlag = outputBson ? "-b" : "-j";
    let mqlrunArguments = [mqlrunExecutableName, outputFormatFlag, "-e", stringifiedPipeline, "-f"];
    if (allowSpillToDisk) {
        mqlrunArguments.push("-t");
        mqlrunArguments.push(tempDir);
    }
    mqlrunArguments.push(inputFileName);
    assert.eq(expectedReturnCode,
              runMongoProgram(...mqlrunArguments),
              expectedReturnCode === 0 ? "mqlrun exited with non-ok status"
                                       : "mqlrun exited with unexpected status");

    if (outputBson) {
        return null;
    }

    const rawOutput = rawMongoProgramOutput();

    // Split the raw output into an array containing the output of each line. We expect the output
    // to end in a newline, so we need to remove the final array element after splitting on
    // newlines.
    const outputByLine = rawOutput.split("\n");
    assert.eq("", outputByLine.pop(), rawOutput);

    // Each line has a prefix like "sh2311702|" which needs to be stripped.
    return outputByLine.map(function(textOfLine) {
        return textOfLine.substr(textOfLine.indexOf("|") + 1).trim();
    });
}
