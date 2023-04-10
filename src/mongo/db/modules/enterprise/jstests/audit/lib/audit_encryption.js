// Helper library for testing at-rest audit log encryption

'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
load('src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js');

const AUDIT_KEYSTORE_TYPES = Object.freeze({
    LOCAL: "local",
    KMIP_GET: "kmip_get",
    KMIP_ENCRYPT: "kmip_encrypt",
});

const AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE =
    "src/mongo/db/modules/enterprise/jstests/audit/lib/localKey";

const AUDIT_KMIP_SERVER_NAME = "127.0.0.1";
const AUDIT_KMIP_SERVER_CAFILE = "jstests/libs/trusted-ca.pem";
const AUDIT_KMIP_CLIENT_CERTFILE = "jstests/libs/trusted-client.pem";

class LocalFixture {
    /**
     * Returns the string name of this fixture
     */
    getKeyStoreType() {
        return AUDIT_KEYSTORE_TYPES.LOCAL;
    }

    getName() {
        return this.getKeyStoreType();
    }

    startKeyServer() {
    }
    stopKeyServer() {
    }

    /**
     * Returns the default options object for testing audit encryption with local key file
     */
    generateOptsWithDefaults(enableCompression) {
        let opts = {auditLocalKeyFile: AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE};
        if (enableCompression) {
            opts.auditCompressionMode = "zstd";
        }
        return opts;
    }

    /**
     * Runs the audit decryptor with appropriate parameters for decrypting an audit
     * log file that was generated using this key manager fixture.
     */
    runDecryptor(auditPath, outputFile) {
        jsTest.log("Running mongoauditdecrypt");
        jsTest.log("Decrypting " + auditPath);
        jsTest.log("Saving json file to " + outputFile);
        return _startMongoProgram("mongoauditdecrypt",
                                  "--inputPath",
                                  auditPath,
                                  "--outputPath",
                                  outputFile,
                                  "--noConfirm");
    }
}

class KMIPFixture {
    /**
     * The port number must be unique among audit tests that use this KMIP fixture
     * so that they can run in parallel without EADDRINUSE issues.
     */
    constructor(kmipServerPort, useLegacyProtocol) {
        this._kmipPort = kmipServerPort;
        this._kmipUID = "";
        this._kmipPID = -1;
        this._useLegacy = useLegacyProtocol;
    }

    /**
     * Starts the KMIP server process. No-op if the process has already started.
     */
    startKeyServer() {
        if (this._kmipPID === -1) {
            this._kmipPID = startPyKMIPServer(this._kmipPort, this._useLegacy);
            this._kmipUID = createPyKMIPKey(this._kmipPort, this._useLegacy);
        }
    }

    /**
     * Stops the KMIP server process, if started using startKeyServer()
     */
    stopKeyServer() {
        if (this._kmipPID !== -1) {
            killPyKMIPServer(this._kmipPID);
            this._kmipPID = -1;
            this._kmipUID = "";
        }
    }

    /**
     * Returns the default options object for testing audit encryption with KMIP
     */
    generateOptsWithDefaults(enableCompression) {
        let opts = {
            kmipServerName: AUDIT_KMIP_SERVER_NAME,
            kmipPort: this._kmipPort,
            kmipServerCAFile: AUDIT_KMIP_SERVER_CAFILE,
            kmipClientCertificateFile: AUDIT_KMIP_CLIENT_CERTFILE,
            kmipUseLegacyProtocol: this._useLegacy,
            auditEncryptionKeyUID: this._kmipUID,
        };
        if (enableCompression) {
            opts.auditCompressionMode = "zstd";
        }
        return opts;
    }

    /**
     * Runs the audit decryptor with appropriate parameters for decrypting an audit
     * log file that was generated using this key manager fixture.
     */
    runDecryptor(auditPath, outputFile) {
        jsTest.log("Running mongoauditdecrypt");
        jsTest.log("Decrypting " + auditPath);
        jsTest.log("Saving json file to " + outputFile);
        return _startMongoProgram("mongoauditdecrypt",
                                  "--inputPath",
                                  auditPath,
                                  "--outputPath",
                                  outputFile,
                                  "--noConfirm",
                                  "--kmipServerName",
                                  AUDIT_KMIP_SERVER_NAME,
                                  "--kmipPort",
                                  this._kmipPort,
                                  "--kmipServerCAFile",
                                  AUDIT_KMIP_SERVER_CAFILE,
                                  "--kmipClientCertificateFile",
                                  AUDIT_KMIP_CLIENT_CERTFILE,
                                  "--kmipUseLegacyProtocol",
                                  this._useLegacy.toString());
    }
}

class KMIPGetFixture extends KMIPFixture {
    /**
     * Returns the string name of this fixture
     */
    getKeyStoreType() {
        return AUDIT_KEYSTORE_TYPES.KMIP_GET;
    }

    getName() {
        return AUDIT_KEYSTORE_TYPES.KMIP_GET + "_" + (this._useLegacy ? "legacy" : "new");
    }

    /**
     * Returns the default options object for testing audit encryption with KMIP Encrypt
     */
    generateOptsWithDefaults(enableCompression) {
        let opts = KMIPFixture.prototype.generateOptsWithDefaults.call(this, enableCompression);
        opts.setParameter = {auditEncryptKeyWithKMIPGet: "true"};
        return opts;
    }
}

class KMIPEncryptFixture extends KMIPFixture {
    /**
     * Returns the string name of this fixture
     */
    getKeyStoreType() {
        return AUDIT_KEYSTORE_TYPES.KMIP_ENCRYPT;
    }

    getName() {
        return AUDIT_KEYSTORE_TYPES.KMIP_ENCRYPT + "_" + (this._useLegacy ? "legacy" : "new");
    }
}

/**
 * Returns true if the given key store type is KMIP Get or Encrypt.
 */
function isKMIP(keyStoreType) {
    return keyStoreType !== AUDIT_KEYSTORE_TYPES.LOCAL;
}

/**
 * Tries parsing a header line and checking it contains all properties,
 * will return true if successful
 */
function isValidEncryptedAuditLogHeader(json, keyStoreType) {
    try {
        const fileheader = JSON.parse(json);
        const properties = [
            "ts",
            "version",
            "compressionMode",
            "keyStoreIdentifier",
            "encryptedKey",
            "MAC",
            "auditRecordType"
        ];

        for (let prop of properties) {
            if (!fileheader.hasOwnProperty(prop)) {
                return false;
            }
        }

        // Verify that the audit file header contains no unknown properties.
        if (Object.keys(fileheader).filter((k) => !properties.includes(k)).length) {
            return false;
        }

        return isValidKeyStoreIdentifier(fileheader.keyStoreIdentifier, keyStoreType);
    } catch (e) {
        return false;
    }
}

/**
 * Verifies that the input keyStoreId object contains only the
 * properties that must be present for the given the keyStoreType.
 * Returns true if successful.
 */
function isValidKeyStoreIdentifier(keyStoreId, keyStoreType) {
    let properties;
    let provider;
    let keyWrapMethod = undefined;
    if (isKMIP(keyStoreType)) {
        properties = [
            "provider",
            "keyWrapMethod",
            "kmipServerName",
            "kmipPort",
            "uid",
        ];
        provider = "kmip";
        keyWrapMethod = (keyStoreType === AUDIT_KEYSTORE_TYPES.KMIP_GET) ? "get" : "encrypt";
    } else {
        properties = [
            "provider",
            "filename",
        ];
        provider = "local";
    }

    for (let prop of properties) {
        if (!keyStoreId.hasOwnProperty(prop)) {
            return false;
        }
    }

    // Verify that the keyStoreID has no unknown properties.
    if (Object.keys(keyStoreId).filter((k) => !properties.includes(k)).length) {
        return false;
    }

    // Verify the field values are correct
    if (provider !== keyStoreId.provider || keyWrapMethod !== keyStoreId.keyWrapMethod) {
        return false;
    }

    return true;
}