// clang-format off
// Generated from oidc_vars.yml by oidc_vars_gen.py



///////////////////////////////////////////////////////////
// Signed Tokens
///////////////////////////////////////////////////////////

const kOIDCTokens = {
    // Header: {
    //     "typ": "JWT",
    //     "alg": "RS256",
    //     "kid": "custom-key-1"
    // }
    // Body: {
    //     "iss": "custom-key-1-issuer",
    //     "sub": "jwsParserTest1",
    //     "iat": 1661374077,
    //     "exp": 2147483647,
    //     "aud": [
    //         "jwt@kernel.mongodb.com"
    //     ],
    //     "nonce": "gdfhjj324ehj23k4",
    //     "auth_time": 1661374077
    // }
    "jwsParserTest1": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSJ9.eyJpc3MiOiJjdXN0b20ta2V5LTEtaXNzdWVyIiwic3ViIjoiandzUGFyc2VyVGVzdDEiLCJpYXQiOjE2NjEzNzQwNzcsImV4cCI6MjE0NzQ4MzY0NywiYXVkIjpbImp3dEBrZXJuZWwubW9uZ29kYi5jb20iXSwibm9uY2UiOiJnZGZoamozMjRlaGoyM2s0IiwiYXV0aF90aW1lIjoxNjYxMzc0MDc3fQ.NllNpXv6hCorpTPcHcvJllkPAAYn-LnA5aUFSi9g9lP1H5Y3Q0p2GNuTrJkuCG7-0BxbLRLBzaC-3Ys0ePRjc94sXFqCce1tbBrItl2qzG77xtnW7ovDU9k3YYR5FMggQqWwDzboKEogYrwj5VHnCfJ1JHmhH1QONdB5Ni8uN2i0QPP87BT05EglPktTktUZRyK5IyCjHwxCO6Nkb_LtCFC4p0uZL0I2-XtYcDRJZIQR1rluBEBMTJcShfUhPoA9ehqOLuP80mKKWtp6qeA9MTModuWetWhDZCDJ3csSHBoNR4gw3TmuSuGLO5bX_UYbEoyNMYQ8EV_eS_-RKZrJEA",

};

///////////////////////////////////////////////////////////
// BSON Payloads
///////////////////////////////////////////////////////////

const kOIDCPayloads = {
    // Payload: {}
    "emptyObject": "BQAAAAA=",

    // Payload: {
    //     "authorizeEndpoint": "https://localhost:1234/authorize",
    //     "tokenEndpoint": "https://localhost:1234/token",
    //     "clientId": "deadbeefcafe",
    //     "clientSecret": "hunter2",
    //     "requestScopes": [
    //         "scope1",
    //         "scope2"
    //     ]
    // }
    "SASLServerStep1Reply": "0gAAAAJhdXRob3JpemVFbmRwb2ludAAhAAAAaHR0cHM6Ly9sb2NhbGhvc3Q6MTIzNC9hdXRob3JpemUAAnRva2VuRW5kcG9pbnQAHQAAAGh0dHBzOi8vbG9jYWxob3N0OjEyMzQvdG9rZW4AAmNsaWVudElkAA0AAABkZWFkYmVlZmNhZmUAAmNsaWVudFNlY3JldAAIAAAAaHVudGVyMgAEcmVxdWVzdFNjb3BlcwAhAAAAAjAABwAAAHNjb3BlMQACMQAHAAAAc2NvcGUyAAAA",

    // Payload: {
    //     "n": "jwsParserTest1"
    // }
    "SASLAdvertizeJWSParserTest1": "GwAAAAJuAA8AAABqd3NQYXJzZXJUZXN0MQAA",

    // Payload: {
    //     "n": "jwsParserTest2"
    // }
    "SASLAdvertizeJWSParserTest2": "GwAAAAJuAA8AAABqd3NQYXJzZXJUZXN0MgAA",

    // Payload: {
    //     "jwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSJ9.eyJpc3MiOiJjdXN0b20ta2V5LTEtaXNzdWVyIiwic3ViIjoiandzUGFyc2VyVGVzdDEiLCJpYXQiOjE2NjEzNzQwNzcsImV4cCI6MjE0NzQ4MzY0NywiYXVkIjpbImp3dEBrZXJuZWwubW9uZ29kYi5jb20iXSwibm9uY2UiOiJnZGZoamozMjRlaGoyM2s0IiwiYXV0aF90aW1lIjoxNjYxMzc0MDc3fQ.NllNpXv6hCorpTPcHcvJllkPAAYn-LnA5aUFSi9g9lP1H5Y3Q0p2GNuTrJkuCG7-0BxbLRLBzaC-3Ys0ePRjc94sXFqCce1tbBrItl2qzG77xtnW7ovDU9k3YYR5FMggQqWwDzboKEogYrwj5VHnCfJ1JHmhH1QONdB5Ni8uN2i0QPP87BT05EglPktTktUZRyK5IyCjHwxCO6Nkb_LtCFC4p0uZL0I2-XtYcDRJZIQR1rluBEBMTJcShfUhPoA9ehqOLuP80mKKWtp6qeA9MTModuWetWhDZCDJ3csSHBoNR4gw3TmuSuGLO5bX_UYbEoyNMYQ8EV_eS_-RKZrJEA"
    // }
    // Referenced tokens: ['jwsParserTest1']
    "SASLAuthenticateAsJWSParserTest1": "iQIAAAJqd3QAewIAAGV5SjBlWEFpT2lKS1YxUWlMQ0poYkdjaU9pSlNVekkxTmlJc0ltdHBaQ0k2SW1OMWMzUnZiUzFyWlhrdE1TSjkuZXlKcGMzTWlPaUpqZFhOMGIyMHRhMlY1TFRFdGFYTnpkV1Z5SWl3aWMzVmlJam9pYW5kelVHRnljMlZ5VkdWemRERWlMQ0pwWVhRaU9qRTJOakV6TnpRd056Y3NJbVY0Y0NJNk1qRTBOelE0TXpZME55d2lZWFZrSWpwYkltcDNkRUJyWlhKdVpXd3ViVzl1WjI5a1lpNWpiMjBpWFN3aWJtOXVZMlVpT2lKblpHWm9hbW96TWpSbGFHb3lNMnMwSWl3aVlYVjBhRjkwYVcxbElqb3hOall4TXpjME1EYzNmUS5ObGxOcFh2NmhDb3JwVFBjSGN2Smxsa1BBQVluLUxuQTVhVUZTaTlnOWxQMUg1WTNRMHAyR051VHJKa3VDRzctMEJ4YkxSTEJ6YUMtM1lzMGVQUmpjOTRzWEZxQ2NlMXRiQnJJdGwycXpHNzd4dG5XN292RFU5azNZWVI1Rk1nZ1FxV3dEemJvS0VvZ1lyd2o1VkhuQ2ZKMUpIbWhIMVFPTmRCNU5pOHVOMmkwUVBQODdCVDA1RWdsUGt0VGt0VVpSeUs1SXlDakh3eENPNk5rYl9MdENGQzRwMHVaTDBJMi1YdFljRFJKWklRUjFybHVCRUJNVEpjU2hmVWhQb0E5ZWhxT0x1UDgwbUtLV3RwNnFlQTlNVE1vZHVXZXRXaERaQ0RKM2NzU0hCb05SNGd3M1RtdVN1R0xPNWJYX1VZYkVveU5NWVE4RVZfZVNfLVJLWnJKRUEAAA==",

};
