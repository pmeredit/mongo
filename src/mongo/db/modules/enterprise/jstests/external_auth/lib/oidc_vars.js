// clang-format off
// Generated from oidc_vars.yml by oidc_vars_gen.py
import {
  OIDCgenerateBSON,
  OIDCsignJWT
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";

export function OIDCVars(issuerPrefix) {
  let obj = {};
///////////////////////////////////////////////////////////
// Signed Tokens
///////////////////////////////////////////////////////////

  obj.kOIDCTokens = {
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS384"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_RS384": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS384"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS512"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_RS512": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS512"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-2",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_custom_key_2": OIDCsignJWT({kid: "custom-key-2"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-2.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user2@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadWriteRole"
    //     ]
    // }
    "Token_OIDCAuth_user2": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user2@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadWriteRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer2",
    //     "sub": "user1@10gen.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1@10gen": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer2`,"sub": `user1@10gen.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-2",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer2",
    //     "sub": "user1@10gen.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1@10gen_custom_key_2": OIDCsignJWT({kid: "custom-key-2"},{"iss": `${issuerPrefix}/issuer2`,"sub": `user1@10gen.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-2.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 1671637210,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_expired": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 1671637210,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 2147000000,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_not_yet_valid": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 2147000000,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_no_nbf_field": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.10gen.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_alt_audience": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.10gen.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-2",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.10gen.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_alt_audience_custom_key_2": OIDCsignJWT({kid: "custom-key-2"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.10gen.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-2.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer2",
    //     "sub": "user1@10gen.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.10gen.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1@10gen_alt_audience": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer2`,"sub": `user1@10gen.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.10gen.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-2",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer2",
    //     "sub": "user1@10gen.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.10gen.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1@10gen_alt_audience_custom_key_2": OIDCsignJWT({kid: "custom-key-2"},{"iss": `${issuerPrefix}/issuer2`,"sub": `user1@10gen.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.10gen.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-2.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.10gen.com",
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_multiple_audiences": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.10gen.com', 'jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_empty_audiences": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": [],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "https://test.kernel.10gen.com/oidc/issuerX",
    //     "sub": "user1@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadRole"
    //     ]
    // }
    "Token_OIDCAuth_user1_wrong_issuer": OIDCsignJWT({kid: "custom-key-1"},{"iss": `https://test.kernel.10gen.com/oidc/issuerX`,"sub": `user1@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-3",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user3@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadWriteRole"
    //     ]
    // }
    "Token_OIDCAuth_user3": OIDCsignJWT({kid: "custom-key-3"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user3@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadWriteRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-3.priv", "RS256"),
    // Header: {
    //     "typ": "JWT",
    //     "kid": "custom-key-1",
    //     "alg": "RS256"
    // }
    // Body: {
    //     "iss": "${issuerPrefix}/issuer1",
    //     "sub": "user4@mongodb.com",
    //     "nbf": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "mongodb-roles": [
    //         "myReadWriteRole"
    //     ],
    //     "mongodb-roles2": [
    //         "myUserAdminRole"
    //     ]
    // }
    "Token_OIDCAuth_user4": OIDCsignJWT({kid: "custom-key-1"},{"iss": `${issuerPrefix}/issuer1`,"sub": `user4@mongodb.com`,"nbf": 1661374077,"exp": 2147483647,"aud": ['jwt@kernel.mongodb.com'],"nonce": `gdfhjj324ehj23k4`,"mongodb-roles": ['myReadWriteRole'],"mongodb-roles2": ['myUserAdminRole'],}, "src/mongo/db/modules/enterprise/jstests/external_auth/lib/custom-key-1.priv", "RS256"),
  };

///////////////////////////////////////////////////////////
// BSON Payloads
///////////////////////////////////////////////////////////


  obj.kOIDCPayloads = {
    // Payload: "{}"
    "emptyObject": OIDCgenerateBSON({}),
    // Payload: "{\"n\": \"user1@mongodb.com\",}"
    "Advertize_OIDCAuth_user1": OIDCgenerateBSON({"n": "user1@mongodb.com",}),
    // Payload: "{\"n\": \"user2@mongodb.com\",}"
    "Advertize_OIDCAuth_user2": OIDCgenerateBSON({"n": "user2@mongodb.com",}),
    // Payload: "{\"n\": \"user1@10gen.com\",}"
    "Advertize_OIDCAuth_user1@10gen": OIDCgenerateBSON({"n": "user1@10gen.com",}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1']
    "Authenticate_OIDCAuth_user1": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_RS384\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_RS384']
    "Authenticate_OIDCAuth_user1_RS384": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_RS384"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_RS512\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_RS512']
    "Authenticate_OIDCAuth_user1_RS512": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_RS512"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_custom_key_2\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_custom_key_2']
    "Authenticate_OIDCAuth_user1_custom_key_2": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_custom_key_2"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user2\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user2']
    "Authenticate_OIDCAuth_user2": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user2"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1@10gen\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1@10gen']
    "Authenticate_OIDCAuth_user1@10gen": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1@10gen"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1@10gen_custom_key_2\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1@10gen_custom_key_2']
    "Authenticate_OIDCAuth_user1@10gen_custom_key_2": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1@10gen_custom_key_2"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_expired\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_expired']
    "Authenticate_OIDCAuth_user1_expired": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_expired"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_not_yet_valid\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_not_yet_valid']
    "Authenticate_OIDCAuth_user1_not_yet_valid": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_not_yet_valid"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_no_nbf_field\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_no_nbf_field']
    "Authenticate_OIDCAuth_user1_no_nbf_field": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_no_nbf_field"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_alt_audience\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_alt_audience']
    "Authenticate_OIDCAuth_user1_alt_audience": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_alt_audience"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_multiple_audiences\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_multiple_audiences']
    "Authenticate_OIDCAuth_user1_multiple_audiences": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_multiple_audiences"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_empty_audiences\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_empty_audiences']
    "Authenticate_OIDCAuth_user1_empty_audiences": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_empty_audiences"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user1_wrong_issuer\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user1_wrong_issuer']
    "Authenticate_OIDCAuth_user1_wrong_issuer": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user1_wrong_issuer"],}),
    // Payload: "{\"jwt\": obj[\"kOIDCTokens\"][\"Token_OIDCAuth_user3\"],}"
    // Referenced tokens: ['Token_OIDCAuth_user3']
    "Authenticate_OIDCAuth_user3": OIDCgenerateBSON({"jwt": obj["kOIDCTokens"]["Token_OIDCAuth_user3"],}),
    };

  return obj;
}
