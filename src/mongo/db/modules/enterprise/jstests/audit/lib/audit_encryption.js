// Helper library for testing at-rest audit log encryption

'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

/**
 * Tries parsing a header line and checking it contains all properties,
 * will return true if successful
 */
function isValidEncryptedAuditLogHeader(json) {
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
        return true;
    } catch (e) {
        return false;
    }
}