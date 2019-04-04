// This contains some helper routines and variables that get used across several different
// ESE tests

function determineEncryptionProvider() {
    'use strict';
    const info = getBuildInfo();
    const ssl = (info.openssl === undefined) ? '' : info.openssl.running;
    if (/OpenSSL/.test(ssl)) {
        return 'openssl';
    } else if (/Apple/.test(ssl)) {
        return 'apple';
    } else if (/Windows/.test(ssl)) {
        return 'windows';
    } else {
        return null;
    }
}

const platformSupportsGCM = determineEncryptionProvider() === 'openssl';
