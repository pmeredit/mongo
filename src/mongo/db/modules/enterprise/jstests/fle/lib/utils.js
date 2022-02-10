const kRandomAlgo = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";
const kDeterministicAlgo = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";

function fle2Enabled() {
    return TestData.useFle2Protocol && TestData.setParameters.featureFlagFLE2;
}

/**
 * Helper method to generate an appropriate structure to mark fields as encrypted. This method
 * abstracts the protocol for mongocryptd (FLE 1) and mongocsfle (FLE 2), allowing the caller to get
 * coverage in both test suites by blindly appending the return value to the command object to
 * analyze. Should not be used if the test requires FLE 1 and/or FLE 2 specific functionality.
 *
 * @param {Object} fieldMap A map of field path to encryption metadata. The field path may contain
 *     dots, which are always treated as accessing a field of a nested document. The metadata can be
 *     in v1 or v2 form.
 * @returns {Object} Returns an object that describes the encrypted fields, either in V1's format as
 *     a JSON Schema or V2's format with encryptionInformation.
 */
function generateSchema(fieldMap) {
    assert.eq(typeof fieldMap, 'object');
    if (!fle2Enabled()) {
        return generateSchemaV1(fieldMap);
    }

    return generateSchemaV2(fieldMap);
}

function generateSchemaV1(fieldMap) {
    let jsonSchema = {type: "object"};
    let properties = {};
    // Convert each dotted path spec to explicit objects with 'properties' for each component.
    for (let path in fieldMap) {
        const pathElements = path.split('.');
        let currentLevel = properties;
        for (let idx = 0; idx < pathElements.length - 1; idx++) {
            const pathElem = pathElements[idx];
            if (!currentLevel.hasOwnProperty(pathElem)) {
                currentLevel[pathElem] = {type: "object", properties: {}};
            }
            currentLevel = currentLevel[pathElem]["properties"];
        }

        currentLevel[pathElements[pathElements.length - 1]] = fieldMap[path];
    }
    jsonSchema["properties"] = properties;
    return {jsonSchema: jsonSchema};
}

function generateSchemaV2(fieldMap) {
    // TODO SERVER-63275: build encryptionInformation from fieldMap
    return generateSchemaV1(fieldMap);
}