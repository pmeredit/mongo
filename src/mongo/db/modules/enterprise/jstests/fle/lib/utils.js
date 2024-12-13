export const kRandomAlgo = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";
export const kDeterministicAlgo = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";

export function fle2Enabled() {
    return TestData.useFle2Protocol;
}

/**
 * Helper method to generate an appropriate structure to mark fields as encrypted. This method
 * abstracts the protocol for mongocryptd (FLE 1) and mongocsfle (FLE 2), allowing the caller to get
 * coverage in both test suites by blindly appending the return value to the command object to
 * analyze. Should not be used if the test requires FLE 1 and/or FLE 2 specific functionality.
 *
 * @param {Object} fieldMap A map of field path to encryption metadata. The field path may contain
 *     dots, which are always treated as accessing a field of a nested document. The metadata can be
 *     in v1 or v2 form. That is, the following are both examples of valid fieldMaps:
 *     1. {
 *          a.b.c: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}},
 *          d.e.f: {bsonType: "string"}
 *        }
 *     2. {
 *          a.b.c: {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"}},
 *          d.e.f: {keyId: UUID(), bsonType: "string"}
 *        }
 * @param {string} namespace The namespace of the collection that the command is being run against.
 * @returns {Object} Returns an object that describes the encrypted fields, either in V1's format as
 *     a JSON Schema or V2's format with encryptionInformation.
 */
export function generateSchema(fieldMap, namespace) {
    assert.eq(typeof fieldMap, 'object');
    if (!fle2Enabled()) {
        return generateSchemaV1(fieldMap);
    }

    return generateSchemaV2(fieldMap, namespace);
}

export function generateSchemaV1(fieldMap) {
    let jsonSchema = {type: "object"};
    let properties = {};
    // Convert each dotted path spec to explicit objects with 'properties' for each component.
    for (const [path, pathSpec] of Object.entries(fieldMap)) {
        const pathElements = path.split('.');
        let currentLevel = properties;
        for (let idx = 0; idx < pathElements.length - 1; idx++) {
            const pathElem = pathElements[idx];
            if (!currentLevel.hasOwnProperty(pathElem)) {
                currentLevel[pathElem] = {type: "object", properties: {}};
            }
            currentLevel = currentLevel[pathElem]["properties"];
        }

        // If pathSpec uses FLE 1 syntax, pass it on to the final schema directly.
        if (pathSpec.hasOwnProperty("encrypt") || !pathSpec.hasOwnProperty("keyId")) {
            currentLevel[pathElements[pathElements.length - 1]] = pathSpec;
            continue;
        }

        // Convert indexed FLE 2 encryption to kDeterministicAlgo and unindexed FLE 2 encryption to
        // kRandomAlgo.
        let fle1Spec = {
            keyId: [pathSpec["keyId"]],
            bsonType: pathSpec["bsonType"],
            algorithm: kRandomAlgo
        };
        if (pathSpec.hasOwnProperty("queries") && pathSpec.queries !== []) {
            fle1Spec.algorithm = kDeterministicAlgo;
        }
        currentLevel[pathElements[pathElements.length - 1]] = {encrypt: fle1Spec};
    }
    jsonSchema["properties"] = properties;
    return {jsonSchema: jsonSchema, isRemoteSchema: false};
}

export function generateSchemaV2(fieldMap, namespace) {
    assert(namespace != undefined, "Missing required namespace for generateSchema()");
    let encryptionInformation = {type: 1, schema: {}};
    addSchemaToEncryptionInformation(encryptionInformation, namespace, fieldMap);
    return {encryptionInformation: encryptionInformation};
}

export function addSchemaToEncryptionInformation(encryptionInformation, namespace, schemaSpec) {
    assert(namespace != undefined, "Missing required namespace for generateSchema()");
    let fields = [];
    for (const [path, pathSpec] of Object.entries(schemaSpec)) {
        // If pathSpec uses FLE 2 syntax, pass it on to the final schema directly.
        if (!pathSpec.hasOwnProperty("encrypt") && pathSpec.hasOwnProperty("keyId")) {
            let encryptedField = {path: path};
            Object.assign(encryptedField, pathSpec);
            fields.push(encryptedField);
            continue;
        }

        // If pathSpec uses FLE 1 syntax but no algorithm was specified, then the field is not
        // encrypted and we should not add it to the encryption information.
        if (!pathSpec.hasOwnProperty("encrypt")) {
            continue;
        }

        let encryptedField = {
            path: path,
            keyId: pathSpec["encrypt"]["keyId"][0],
            bsonType: pathSpec["encrypt"]["bsonType"]
        };

        // Convert kDeterministicAlgo to the indexed FLE 2 encryption and kRandomAlgo to the
        // unindexed FLE 2 encryption.
        if (pathSpec["encrypt"]["algorithm"] === kDeterministicAlgo) {
            encryptedField["queries"] = [{"queryType": "equality"}];
        }

        fields.push(encryptedField);
    }

    encryptionInformation["schema"][namespace] = {fields: fields};
}

export function generateSchemasFromSchemaMap(schemaMap) {
    assert.eq(typeof schemaMap, 'object');
    if (!fle2Enabled()) {
        return generateSchemaV1_SchemaMap(schemaMap);
    }

    return generateSchemaV2_SchemaMap(schemaMap);
}

export function generateSchemaV1_SchemaMap(schemaMap) {
    let csfleEncryptionSchemas = {};

    for (const [namespace, schemaSpec] of Object.entries(schemaMap)) {
        csfleEncryptionSchemas[namespace] = generateSchemaV1(schemaSpec);
    }

    return {csfleEncryptionSchemas: csfleEncryptionSchemas};
}

export function generateSchemaV2_SchemaMap(schemaMap) {
    let encryptionInformation = {type: 1, schema: {}};
    for (const [namespace, schemaSpec] of Object.entries(schemaMap)) {
        addSchemaToEncryptionInformation(encryptionInformation, namespace, schemaSpec);
    }
    return {encryptionInformation: encryptionInformation};
}
