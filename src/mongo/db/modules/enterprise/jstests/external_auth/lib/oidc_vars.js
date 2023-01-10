// clang-format off
// Generated from oidc_vars.yml by oidc_vars_gen.py



///////////////////////////////////////////////////////////
// Signed Tokens
///////////////////////////////////////////////////////////

const kOIDCTokens = {
    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.JHc9hOplK7bmkKv8EP2LRvpXFEWrkDNUXAAL-SJOrt2gHCLrvo8vY6IzKlqDcmiuQr3PBENziVV6Ys6Zbr6Pg3nc5Y2-7VYPTRYQPK3BEyJjxvg-96ocd7AiTRVn2vKLNruaIDivZ82asExO21LsUwaLc2bASl8L0nIzq9SbkNjrImd2hkXYsivrH342FddKwbzCINkNfBjc5DMSljamHdMvlJr5o-oei634pKfHHUKu4RP1PMSaM461ZBtz5Zse0vwMqjkEcc3SIhZYRrw8rLJAJd1YdtBlu4-rOAPWlIIAc_lbYopgeEJDgSarywNAkFy2LrLXjysa29CFHXvO2Q",

    // Header: {
    //     "alg": "RS384",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_RS384": "eyJhbGciOiJSUzM4NCIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.bqPFUS064dpczLH3Y9q4uT6D7S1VOJ9vsF6U4Itwbn1P2XPJqSyBw8zdbEeVjFHnicT-T1O_mOJrf3b9lOAt3yhtgK7A0JqDDS0znjxx_iyZ5MnrbDOOkDVf5ZYxdtYhomzDaJxSJgrkHjQzWZGptBp5-Abq24gnNSOcuoCNet-Es7rXlzs17oubFdufQOGReoQ6v2AzzZx1AIFy4fYAWAbgfx3rNZRD2UTl71uozrRdaRUc3QC2Si1JVq3jzZixE6c6pjVR5yax9GbxhfKpJSkz-g1O55AM-CJWHjn3RMAoR3q50jkQ7--Ukwm3rDgAJeTj7VRnhZ81ikvnBsQuhw",

    // Header: {
    //     "alg": "RS512",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_RS512": "eyJhbGciOiJSUzUxMiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.Xo_hMRuKmSiJMx9yRR7Xam-kc_8MXViEcsQe_86wg2wpsCXnWI3bzomz-fdYL88d3GI7S-nOaK9g1dyR4BESkLp4O1FdBtYBPNevPVXAVm3fPme2kQUeicnZFdBJiHEDXOG_iCFMFyZyTQsY2SpgF6uY1Y4NLvZ8kItFJDUrrpfLwzDrpU6JJNUG-LtbRbej4Kj6zaDu70FpOAevDY4utB174ss5gvsIFUxm9tw73zPkVJhWAVRxCrfKijHi4BnKTzlRzoalAy2XWG_1E7CcAgpKoZWath9VnG_85jc3DM4KWsjlbf7qGm1aabOVf2w_DgfLWF7FsT9VhvmYGfWZgg",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user2": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIyQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkV3JpdGVSb2xlIl19.Ga06JUo1FC5eN0Z3CvnyAA47QDGNuKxt10KOaSN5I3Sq0QWveopSUgUJCVvdC2tLQnX4J1KfJ8yhY3RzW32bD0WFGpcUaX6yr-38-_9abJdPC-3Ws1xgE6nYnCA0yoxrfRCsVv3uu_SPpLw7ZGZNOdcVqU1a3hCv0KkRc59edsZMW5qTs5UkcWes6egRE_c73iZT5UfcFL9tiNqGMreatj5U6s5aF9nCvBmM3GyMwKjY0qDriQIp3P-VZHEzfzP6FFIpeITzwHes3tqhvJ7Env1YqNIbpMysT76k0rCaTWR_0u9t1-ciF6NASOjrP5UDEMVts90ujNZjs_JcYKB8qQ",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-2",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer2",
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
    "Token_OIDCAuth_user1@10gen": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMiIsInN1YiI6InVzZXIxQDEwZ2VuLmNvbSIsIm5iZiI6MTY2MTM3NDA3NywiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.TnxpFikGfEkDPuql58lLK_JRbiic5NYmMcczGiwdmuYxSoZS986dXJDkeplirYuLFvQXibNsYsIar9oNEWF7Ilmvy9i8BUfc1PRGRoP70L1ZKRZlo9u0Oa7wn3ckgyQdR8IE37uy4Zt_j1pBoL2StvRjlpMjpG4ZzBmqjoyac-o3-QahngH_cpSMaqus7h8xGszAVsTkK2Y_rZnFH0jcEQUfZYTTolgkORhIS--DbhkDsK5P851caAHHbQeGPsf_DQDxGTlUZkgHGhhevjlHK7IsNomGYJVDI_HBBA2VZ9iTnFYJA7gRPOsSuMaIdmuVfq-Aw2WcR3OXESeaMmuCow",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_expired": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjE2NzE2MzcyMTAsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.GZPXCQY3Xz38ptI-nk8NP9AR7esWFVNngfkFYfia1kekDSB0g1S_APwwtUvzQes1Ul6hRnuhxa9kNCqXWKA6hKVEmKR1qNJ0fvj0FuW0hxdHl0bBR2rcU3xJEueJEw_7almuaZcquAw0dsL3IgtiydwiQYq48niW0N6ht-hcqWFCUTlBpSedHIw6pVbQvRgVPEEAT3vlwUFB2FgjzGO-3qmFqlOcbxfzKdu2eW3LAmXKRatOIQ6X-0zLy1Y0Ci_v2hab0B7vcq-SpJ7jq_XD3_Q-LPW44_faxsEhsAFhFPA97IsTrEd79MZeBeCyanwNM4zRTvMm_9jISJxLd1ZnNQ",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_not_yet_valid": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoyMTQ3MDAwMDAwLCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.jOZgwgJ5SkGMkxfmn9jIgU2Wpq6S9X4k8gis0E1e7OdwHnJ5oNvlWdErvM8dFUPWMssyJNcNA7dbI9U19GUKSo0MSnh7HExFl9-1lnrPe7mBOlrQnHUaH3mo_ZRHgR-ZpBwkoCbRaQdemsZOwh6VlNcNjheK3FygG7cTlDHg3eTOI4KcolVc6zYwL0fitSM7QhtyrYJD1PsGHqAPWojSDn7qT4kIAyE2CdhnFbSF0Kq_jtST3Keq_6st9O85750tUjSYiVtZ6JAr9_03xAy139eaU4-5SdCxovDowU745n86GOw-4yOMCcY5EwncEWIPMypnNPyWeWqh03owA_3MSg",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_no_nbf_field": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.tfkguqAWbMvNLrwT6tDVmxG10cpzx4X2Mtjg1DmZyHvx2Qh8rb2BxmyTHsXLYARbPsTrlQxrvAIEtxj1KuwB_9l35B0v4oPIEvHfGLcZ0RT83AtBJD48pQXAGVGN0Or8EViRTERXn8QWwZriZDSCRMEjpl1QwnTVcmCKqqlyJkdcWZ5sCF7rgmX1nyHyZEDoXeL7-VflC0GuAegqSw4zvVrjOTIjKQA-E4uj-dFPaB98xypOG6BzPpoyi-blH3UUjABGMGPc7KZQb4yenprZMnLwjb8Xt3tkpjzlZ8DtmIk1ujNaqs6Znhm2r82haAxwG4K7X30zBj9ROXNtlIOgVA",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
    // }
    // Body: {
    //     "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
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
    "Token_OIDCAuth_user1_wrong_audience": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLjEwZ2VuLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.RRBBvQGHdAos2EOAOnrgcf0SlDnvFivHiQN4OIOtiIKHSfyAiKnDbmqbJWFmuhUl7ykHUMO9lBYDwSoP4wwoJjBLhmmicvAJSh1Kvi-CGnAmbQPOHqty-th_w_zHKdZDD4m6rGZX_g23jPDeIMuPSbjyQX6mClw96zz8373VjIDjynPWN-5IaKTVDlHbDHyxzdtyvVI1mwC26PwnlMrw35gvP3S9YunD4VWulzUcFF70YK6SMdPWYobwtn9mOzNQyvaNBQ6RlT-ulC8DjeYuOuvNSGoImGVYyqCgFmZtcKpP6kq1YnRCpw7tVGJ03VBlobwoC95mht7N0ssFQFKdGQ",

    // Header: {
    //     "alg": "RS256",
    //     "kid": "custom-key-1",
    //     "typ": "JWT"
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
    "Token_OIDCAuth_user1_wrong_issuer": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLjEwZ2VuLmNvbS9vaWRjL2lzc3VlclgiLCJzdWIiOiJ1c2VyMUBtb25nb2RiLmNvbSIsIm5iZiI6MTY2MTM3NDA3NywiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.QtAhP-bMZsz59YLd71Ik47mCKssmPT3tuWZq1IYLl159UkpnYo5B2ODf4W1oFky62ArZLZJ6VZHXzn5_yFun96R4qz6XWtolXRYqv6q4CjBTpBl-wWAG3Pm4SLps8tAYAXuZR5pVkDPcPocFG0cw4i-HnRd70wn5Xqc3KO6MGwunVm88FtGwwKQKGXRT2y8pfbfY34VGbOm6zt_Vnoq9RpFzkbkpCTOfcYf_QALlsmaGOlTkJj87FfZBfmrUOCq1PFF7jCYIEY57QTqXjssIrae4G7NmGx3zfYUitDw-3wOQxOu0Hbf06eWz0HQ91zmKNKls3kZhX6sVgDtYRfv2HA",

};

///////////////////////////////////////////////////////////
// BSON Payloads
///////////////////////////////////////////////////////////

const kOIDCPayloads = {
    // Payload: {}
    "emptyObject": "BQAAAAA=",

    // Payload: {
    //     "n": "user1@mongodb.com"
    // }
    "Advertize_OIDCAuth_user1": "HgAAAAJuABIAAAB1c2VyMUBtb25nb2RiLmNvbQAA",

    // Payload: {
    //     "n": "user2@mongodb.com"
    // }
    "Advertize_OIDCAuth_user2": "HgAAAAJuABIAAAB1c2VyMkBtb25nb2RiLmNvbQAA",

    // Payload: {
    //     "n": "user1@10gen.com"
    // }
    "Advertize_OIDCAuth_user1@10gen": "HAAAAAJuABAAAAB1c2VyMUAxMGdlbi5jb20AAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.JHc9hOplK7bmkKv8EP2LRvpXFEWrkDNUXAAL-SJOrt2gHCLrvo8vY6IzKlqDcmiuQr3PBENziVV6Ys6Zbr6Pg3nc5Y2-7VYPTRYQPK3BEyJjxvg-96ocd7AiTRVn2vKLNruaIDivZ82asExO21LsUwaLc2bASl8L0nIzq9SbkNjrImd2hkXYsivrH342FddKwbzCINkNfBjc5DMSljamHdMvlJr5o-oei634pKfHHUKu4RP1PMSaM461ZBtz5Zse0vwMqjkEcc3SIhZYRrw8rLJAJd1YdtBlu4-rOAPWlIIAc_lbYopgeEJDgSarywNAkFy2LrLXjysa29CFHXvO2Q"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1']
    "Authenticate_OIDCAuth_user1": "uQIAAAJqd3QAqwIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVW05c1pTSmRmUS5KSGM5aE9wbEs3Ym1rS3Y4RVAyTFJ2cFhGRVdya0ROVVhBQUwtU0pPcnQyZ0hDTHJ2bzh2WTZJektscURjbWl1UXIzUEJFTnppVlY2WXM2WmJyNlBnM25jNVkyLTdWWVBUUllRUEszQkV5Smp4dmctOTZvY2Q3QWlUUlZuMnZLTE5ydWFJRGl2WjgyYXNFeE8yMUxzVXdhTGMyYkFTbDhMMG5JenE5U2JrTmpySW1kMmhrWFlzaXZySDM0MkZkZEt3YnpDSU5rTmZCamM1RE1TbGphbUhkTXZsSnI1by1vZWk2MzRwS2ZISFVLdTRSUDFQTVNhTTQ2MVpCdHo1WnNlMHZ3TXFqa0VjYzNTSWhaWVJydzhyTEpBSmQxWWR0Qmx1NC1yT0FQV2xJSUFjX2xiWW9wZ2VFSkRnU2FyeXdOQWtGeTJMckxYanlzYTI5Q0ZIWHZPMlEAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzM4NCIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.bqPFUS064dpczLH3Y9q4uT6D7S1VOJ9vsF6U4Itwbn1P2XPJqSyBw8zdbEeVjFHnicT-T1O_mOJrf3b9lOAt3yhtgK7A0JqDDS0znjxx_iyZ5MnrbDOOkDVf5ZYxdtYhomzDaJxSJgrkHjQzWZGptBp5-Abq24gnNSOcuoCNet-Es7rXlzs17oubFdufQOGReoQ6v2AzzZx1AIFy4fYAWAbgfx3rNZRD2UTl71uozrRdaRUc3QC2Si1JVq3jzZixE6c6pjVR5yax9GbxhfKpJSkz-g1O55AM-CJWHjn3RMAoR3q50jkQ7--Ukwm3rDgAJeTj7VRnhZ81ikvnBsQuhw"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_RS384']
    "Authenticate_OIDCAuth_user1_RS384": "uQIAAAJqd3QAqwIAAGV5SmhiR2NpT2lKU1V6TTROQ0lzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVW05c1pTSmRmUS5icVBGVVMwNjRkcGN6TEgzWTlxNHVUNkQ3UzFWT0o5dnNGNlU0SXR3Ym4xUDJYUEpxU3lCdzh6ZGJFZVZqRkhuaWNULVQxT19tT0pyZjNiOWxPQXQzeWh0Z0s3QTBKcUREUzB6bmp4eF9peVo1TW5yYkRPT2tEVmY1Wll4ZHRZaG9tekRhSnhTSmdya0hqUXpXWkdwdEJwNS1BYnEyNGduTlNPY3VvQ05ldC1FczdyWGx6czE3b3ViRmR1ZlFPR1Jlb1E2djJBenpaeDFBSUZ5NGZZQVdBYmdmeDNyTlpSRDJVVGw3MXVvenJSZGFSVWMzUUMyU2kxSlZxM2p6Wml4RTZjNnBqVlI1eWF4OUdieGhmS3BKU2t6LWcxTzU1QU0tQ0pXSGpuM1JNQW9SM3E1MGprUTctLVVrd20zckRnQUplVGo3VlJuaFo4MWlrdm5Cc1F1aHcAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzUxMiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.Xo_hMRuKmSiJMx9yRR7Xam-kc_8MXViEcsQe_86wg2wpsCXnWI3bzomz-fdYL88d3GI7S-nOaK9g1dyR4BESkLp4O1FdBtYBPNevPVXAVm3fPme2kQUeicnZFdBJiHEDXOG_iCFMFyZyTQsY2SpgF6uY1Y4NLvZ8kItFJDUrrpfLwzDrpU6JJNUG-LtbRbej4Kj6zaDu70FpOAevDY4utB174ss5gvsIFUxm9tw73zPkVJhWAVRxCrfKijHi4BnKTzlRzoalAy2XWG_1E7CcAgpKoZWath9VnG_85jc3DM4KWsjlbf7qGm1aabOVf2w_DgfLWF7FsT9VhvmYGfWZgg"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_RS512']
    "Authenticate_OIDCAuth_user1_RS512": "uQIAAAJqd3QAqwIAAGV5SmhiR2NpT2lKU1V6VXhNaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVW05c1pTSmRmUS5Yb19oTVJ1S21TaUpNeDl5UlI3WGFtLWtjXzhNWFZpRWNzUWVfODZ3ZzJ3cHNDWG5XSTNiem9tei1mZFlMODhkM0dJN1Mtbk9hSzlnMWR5UjRCRVNrTHA0TzFGZEJ0WUJQTmV2UFZYQVZtM2ZQbWUya1FVZWljblpGZEJKaUhFRFhPR19pQ0ZNRnlaeVRRc1kyU3BnRjZ1WTFZNE5Mdlo4a0l0RkpEVXJycGZMd3pEcnBVNkpKTlVHLUx0YlJiZWo0S2o2emFEdTcwRnBPQWV2RFk0dXRCMTc0c3M1Z3ZzSUZVeG05dHc3M3pQa1ZKaFdBVlJ4Q3JmS2lqSGk0Qm5LVHpsUnpvYWxBeTJYV0dfMUU3Q2NBZ3BLb1pXYXRoOVZuR184NWpjM0RNNEtXc2psYmY3cUdtMWFhYk9WZjJ3X0RnZkxXRjdGc1Q5Vmh2bVlHZldaZ2cAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIyQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkV3JpdGVSb2xlIl19.Ga06JUo1FC5eN0Z3CvnyAA47QDGNuKxt10KOaSN5I3Sq0QWveopSUgUJCVvdC2tLQnX4J1KfJ8yhY3RzW32bD0WFGpcUaX6yr-38-_9abJdPC-3Ws1xgE6nYnCA0yoxrfRCsVv3uu_SPpLw7ZGZNOdcVqU1a3hCv0KkRc59edsZMW5qTs5UkcWes6egRE_c73iZT5UfcFL9tiNqGMreatj5U6s5aF9nCvBmM3GyMwKjY0qDriQIp3P-VZHEzfzP6FFIpeITzwHes3tqhvJ7Env1YqNIbpMysT76k0rCaTWR_0u9t1-ciF6NASOjrP5UDEMVts90ujNZjs_JcYKB8qQ"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user2']
    "Authenticate_OIDCAuth_user2": "vwIAAAJqd3QAsQIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeVFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVjNKcGRHVlNiMnhsSWwxOS5HYTA2SlVvMUZDNWVOMFozQ3ZueUFBNDdRREdOdUt4dDEwS09hU041STNTcTBRV3Zlb3BTVWdVSkNWdmRDMnRMUW5YNEoxS2ZKOHloWTNSelczMmJEMFdGR3BjVWFYNnlyLTM4LV85YWJKZFBDLTNXczF4Z0U2blluQ0EweW94cmZSQ3NWdjN1dV9TUHBMdzdaR1pOT2RjVnFVMWEzaEN2MEtrUmM1OWVkc1pNVzVxVHM1VWtjV2VzNmVnUkVfYzczaVpUNVVmY0ZMOXRpTnFHTXJlYXRqNVU2czVhRjluQ3ZCbU0zR3lNd0tqWTBxRHJpUUlwM1AtVlpIRXpmelA2RkZJcGVJVHp3SGVzM3RxaHZKN0VudjFZcU5JYnBNeXNUNzZrMHJDYVRXUl8wdTl0MS1jaUY2TkFTT2pyUDVVREVNVnRzOTB1ak5aanNfSmNZS0I4cVEAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMiIsInN1YiI6InVzZXIxQDEwZ2VuLmNvbSIsIm5iZiI6MTY2MTM3NDA3NywiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.TnxpFikGfEkDPuql58lLK_JRbiic5NYmMcczGiwdmuYxSoZS986dXJDkeplirYuLFvQXibNsYsIar9oNEWF7Ilmvy9i8BUfc1PRGRoP70L1ZKRZlo9u0Oa7wn3ckgyQdR8IE37uy4Zt_j1pBoL2StvRjlpMjpG4ZzBmqjoyac-o3-QahngH_cpSMaqus7h8xGszAVsTkK2Y_rZnFH0jcEQUfZYTTolgkORhIS--DbhkDsK5P851caAHHbQeGPsf_DQDxGTlUZkgHGhhevjlHK7IsNomGYJVDI_HBBA2VZ9iTnFYJA7gRPOsSuMaIdmuVfq-Aw2WcR3OXESeaMmuCow"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1@10gen']
    "Authenticate_OIDCAuth_user1@10gen": "tgIAAAJqd3QAqAIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TWlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNaUlzSW5OMVlpSTZJblZ6WlhJeFFERXdaMlZ1TG1OdmJTSXNJbTVpWmlJNk1UWTJNVE0zTkRBM055d2laWGh3SWpveU1UUTNORGd6TmpRM0xDSmhkV1FpT2xzaWFuZDBRR3RsY201bGJDNXRiMjVuYjJSaUxtTnZiU0pkTENKdWIyNWpaU0k2SW1ka1ptaHFhak15TkdWb2FqSXphelFpTENKdGIyNW5iMlJpTFhKdmJHVnpJanBiSW0xNVVtVmhaRkp2YkdVaVhYMC5UbnhwRmlrR2ZFa0RQdXFsNThsTEtfSlJiaWljNU5ZbU1jY3pHaXdkbXVZeFNvWlM5ODZkWEpEa2VwbGlyWXVMRnZRWGliTnNZc0lhcjlvTkVXRjdJbG12eTlpOEJVZmMxUFJHUm9QNzBMMVpLUlpsbzl1ME9hN3duM2NrZ3lRZFI4SUUzN3V5NFp0X2oxcEJvTDJTdHZSamxwTWpwRzRaekJtcWpveWFjLW8zLVFhaG5nSF9jcFNNYXF1czdoOHhHc3pBVnNUa0syWV9yWm5GSDBqY0VRVWZaWVRUb2xna09SaElTLS1EYmhrRHNLNVA4NTFjYUFISGJRZUdQc2ZfRFFEeEdUbFVaa2dIR2hoZXZqbEhLN0lzTm9tR1lKVkRJX0hCQkEyVlo5aVRuRllKQTdnUlBPc1N1TWFJZG11VmZxLUF3MldjUjNPWEVTZWFNbXVDb3cAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjE2NzE2MzcyMTAsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.GZPXCQY3Xz38ptI-nk8NP9AR7esWFVNngfkFYfia1kekDSB0g1S_APwwtUvzQes1Ul6hRnuhxa9kNCqXWKA6hKVEmKR1qNJ0fvj0FuW0hxdHl0bBR2rcU3xJEueJEw_7almuaZcquAw0dsL3IgtiydwiQYq48niW0N6ht-hcqWFCUTlBpSedHIw6pVbQvRgVPEEAT3vlwUFB2FgjzGO-3qmFqlOcbxfzKdu2eW3LAmXKRatOIQ6X-0zLy1Y0Ci_v2hab0B7vcq-SpJ7jq_XD3_Q-LPW44_faxsEhsAFhFPA97IsTrEd79MZeBeCyanwNM4zRTvMm_9jISJxLd1ZnNQ"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_expired']
    "Authenticate_OIDCAuth_user1_expired": "uQIAAAJqd3QAqwIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qRTJOekUyTXpjeU1UQXNJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVW05c1pTSmRmUS5HWlBYQ1FZM1h6MzhwdEktbms4TlA5QVI3ZXNXRlZObmdma0ZZZmlhMWtla0RTQjBnMVNfQVB3d3RVdnpRZXMxVWw2aFJudWh4YTlrTkNxWFdLQTZoS1ZFbUtSMXFOSjBmdmowRnVXMGh4ZEhsMGJCUjJyY1UzeEpFdWVKRXdfN2FsbXVhWmNxdUF3MGRzTDNJZ3RpeWR3aVFZcTQ4bmlXME42aHQtaGNxV0ZDVVRsQnBTZWRISXc2cFZiUXZSZ1ZQRUVBVDN2bHdVRkIyRmdqekdPLTNxbUZxbE9jYnhmektkdTJlVzNMQW1YS1JhdE9JUTZYLTB6THkxWTBDaV92MmhhYjBCN3ZjcS1TcEo3anFfWEQzX1EtTFBXNDRfZmF4c0Voc0FGaEZQQTk3SXNUckVkNzlNWmVCZUN5YW53Tk00elJUdk1tXzlqSVNKeExkMVpuTlEAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoyMTQ3MDAwMDAwLCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLm1vbmdvZGIuY29tIl0sIm5vbmNlIjoiZ2RmaGpqMzI0ZWhqMjNrNCIsIm1vbmdvZGItcm9sZXMiOlsibXlSZWFkUm9sZSJdfQ.jOZgwgJ5SkGMkxfmn9jIgU2Wpq6S9X4k8gis0E1e7OdwHnJ5oNvlWdErvM8dFUPWMssyJNcNA7dbI9U19GUKSo0MSnh7HExFl9-1lnrPe7mBOlrQnHUaH3mo_ZRHgR-ZpBwkoCbRaQdemsZOwh6VlNcNjheK3FygG7cTlDHg3eTOI4KcolVc6zYwL0fitSM7QhtyrYJD1PsGHqAPWojSDn7qT4kIAyE2CdhnFbSF0Kq_jtST3Keq_6st9O85750tUjSYiVtZ6JAr9_03xAy139eaU4-5SdCxovDowU745n86GOw-4yOMCcY5EwncEWIPMypnNPyWeWqh03owA_3MSg"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_not_yet_valid']
    "Authenticate_OIDCAuth_user1_not_yet_valid": "uQIAAAJqd3QAqwIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3lNVFEzTURBd01EQXdMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMbTF2Ym1kdlpHSXVZMjl0SWwwc0ltNXZibU5sSWpvaVoyUm1hR3BxTXpJMFpXaHFNak5yTkNJc0ltMXZibWR2WkdJdGNtOXNaWE1pT2xzaWJYbFNaV0ZrVW05c1pTSmRmUS5qT1pnd2dKNVNrR01reGZtbjlqSWdVMldwcTZTOVg0azhnaXMwRTFlN09kd0huSjVvTnZsV2RFcnZNOGRGVVBXTXNzeUpOY05BN2RiSTlVMTlHVUtTbzBNU25oN0hFeEZsOS0xbG5yUGU3bUJPbHJRbkhVYUgzbW9fWlJIZ1ItWnBCd2tvQ2JSYVFkZW1zWk93aDZWbE5jTmpoZUszRnlnRzdjVGxESGczZVRPSTRLY29sVmM2ell3TDBmaXRTTTdRaHR5cllKRDFQc0dIcUFQV29qU0RuN3FUNGtJQXlFMkNkaG5GYlNGMEtxX2p0U1QzS2VxXzZzdDlPODU3NTB0VWpTWWlWdFo2SkFyOV8wM3hBeTEzOWVhVTQtNVNkQ3hvdkRvd1U3NDVuODZHT3ctNHlPTUNjWTVFd25jRVdJUE15cG5OUHlXZVdxaDAzb3dBXzNNU2cAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.tfkguqAWbMvNLrwT6tDVmxG10cpzx4X2Mtjg1DmZyHvx2Qh8rb2BxmyTHsXLYARbPsTrlQxrvAIEtxj1KuwB_9l35B0v4oPIEvHfGLcZ0RT83AtBJD48pQXAGVGN0Or8EViRTERXn8QWwZriZDSCRMEjpl1QwnTVcmCKqqlyJkdcWZ5sCF7rgmX1nyHyZEDoXeL7-VflC0GuAegqSw4zvVrjOTIjKQA-E4uj-dFPaB98xypOG6BzPpoyi-blH3UUjABGMGPc7KZQb4yenprZMnLwjb8Xt3tkpjzlZ8DtmIk1ujNaqs6Znhm2r82haAxwG4K7X30zBj9ROXNtlIOgVA"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_no_nbf_field']
    "Authenticate_OIDCAuth_user1_no_nbf_field": "ogIAAAJqd3QAlAIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpWlhod0lqb3lNVFEzTkRnek5qUTNMQ0poZFdRaU9sc2lhbmQwUUd0bGNtNWxiQzV0YjI1bmIyUmlMbU52YlNKZExDSnViMjVqWlNJNkltZGtabWhxYWpNeU5HVm9hakl6YXpRaUxDSnRiMjVuYjJSaUxYSnZiR1Z6SWpwYkltMTVVbVZoWkZKdmJHVWlYWDAudGZrZ3VxQVdiTXZOTHJ3VDZ0RFZteEcxMGNweng0WDJNdGpnMURtWnlIdngyUWg4cmIyQnhteVRIc1hMWUFSYlBzVHJsUXhydkFJRXR4ajFLdXdCXzlsMzVCMHY0b1BJRXZIZkdMY1owUlQ4M0F0QkpENDhwUVhBR1ZHTjBPcjhFVmlSVEVSWG44UVd3WnJpWkRTQ1JNRWpwbDFRd25UVmNtQ0txcWx5SmtkY1daNXNDRjdyZ21YMW55SHlaRURvWGVMNy1WZmxDMEd1QWVncVN3NHp2VnJqT1RJaktRQS1FNHVqLWRGUGFCOTh4eXBPRzZCelBwb3lpLWJsSDNVVWpBQkdNR1BjN0taUWI0eWVucHJaTW5Md2piOFh0M3RrcGp6bFo4RHRtSWsxdWpOYXFzNlpuaG0ycjgyaGFBeHdHNEs3WDMwekJqOVJPWE50bElPZ1ZBAAA=",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLm1vbmdvZGIuY29tL29pZGMvaXNzdWVyMSIsInN1YiI6InVzZXIxQG1vbmdvZGIuY29tIiwibmJmIjoxNjYxMzc0MDc3LCJleHAiOjIxNDc0ODM2NDcsImF1ZCI6WyJqd3RAa2VybmVsLjEwZ2VuLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.RRBBvQGHdAos2EOAOnrgcf0SlDnvFivHiQN4OIOtiIKHSfyAiKnDbmqbJWFmuhUl7ykHUMO9lBYDwSoP4wwoJjBLhmmicvAJSh1Kvi-CGnAmbQPOHqty-th_w_zHKdZDD4m6rGZX_g23jPDeIMuPSbjyQX6mClw96zz8373VjIDjynPWN-5IaKTVDlHbDHyxzdtyvVI1mwC26PwnlMrw35gvP3S9YunD4VWulzUcFF70YK6SMdPWYobwtn9mOzNQyvaNBQ6RlT-ulC8DjeYuOuvNSGoImGVYyqCgFmZtcKpP6kq1YnRCpw7tVGJ03VBlobwoC95mht7N0ssFQFKdGQ"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_wrong_audience']
    "Authenticate_OIDCAuth_user1_wrong_audience": "tgIAAAJqd3QAqAIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTG0xdmJtZHZaR0l1WTI5dEwyOXBaR012YVhOemRXVnlNU0lzSW5OMVlpSTZJblZ6WlhJeFFHMXZibWR2WkdJdVkyOXRJaXdpYm1KbUlqb3hOall4TXpjME1EYzNMQ0psZUhBaU9qSXhORGMwT0RNMk5EY3NJbUYxWkNJNld5SnFkM1JBYTJWeWJtVnNMakV3WjJWdUxtTnZiU0pkTENKdWIyNWpaU0k2SW1ka1ptaHFhak15TkdWb2FqSXphelFpTENKdGIyNW5iMlJpTFhKdmJHVnpJanBiSW0xNVVtVmhaRkp2YkdVaVhYMC5SUkJCdlFHSGRBb3MyRU9BT25yZ2NmMFNsRG52Rml2SGlRTjRPSU90aUlLSFNmeUFpS25EYm1xYkpXRm11aFVsN3lrSFVNTzlsQllEd1NvUDR3d29KakJMaG1taWN2QUpTaDFLdmktQ0duQW1iUVBPSHF0eS10aF93X3pIS2RaREQ0bTZyR1pYX2cyM2pQRGVJTXVQU2JqeVFYNm1DbHc5Nnp6ODM3M1ZqSURqeW5QV04tNUlhS1RWRGxIYkRIeXh6ZHR5dlZJMW13QzI2UHdubE1ydzM1Z3ZQM1M5WXVuRDRWV3VselVjRkY3MFlLNlNNZFBXWW9id3RuOW1Pek5ReXZhTkJRNlJsVC11bEM4RGplWXVPdXZOU0dvSW1HVll5cUNnRm1adGNLcFA2a3ExWW5SQ3B3N3RWR0owM1ZCbG9id29DOTVtaHQ3TjBzc0ZRRktkR1EAAA==",

    // Payload: {
    //     "jwt": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImN1c3RvbS1rZXktMSIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3Rlc3Qua2VybmVsLjEwZ2VuLmNvbS9vaWRjL2lzc3VlclgiLCJzdWIiOiJ1c2VyMUBtb25nb2RiLmNvbSIsIm5iZiI6MTY2MTM3NDA3NywiZXhwIjoyMTQ3NDgzNjQ3LCJhdWQiOlsiand0QGtlcm5lbC5tb25nb2RiLmNvbSJdLCJub25jZSI6ImdkZmhqajMyNGVoajIzazQiLCJtb25nb2RiLXJvbGVzIjpbIm15UmVhZFJvbGUiXX0.QtAhP-bMZsz59YLd71Ik47mCKssmPT3tuWZq1IYLl159UkpnYo5B2ODf4W1oFky62ArZLZJ6VZHXzn5_yFun96R4qz6XWtolXRYqv6q4CjBTpBl-wWAG3Pm4SLps8tAYAXuZR5pVkDPcPocFG0cw4i-HnRd70wn5Xqc3KO6MGwunVm88FtGwwKQKGXRT2y8pfbfY34VGbOm6zt_Vnoq9RpFzkbkpCTOfcYf_QALlsmaGOlTkJj87FfZBfmrUOCq1PFF7jCYIEY57QTqXjssIrae4G7NmGx3zfYUitDw-3wOQxOu0Hbf06eWz0HQ91zmKNKls3kZhX6sVgDtYRfv2HA"
    // }
    // Referenced tokens: ['Token_OIDCAuth_user1_wrong_issuer']
    "Authenticate_OIDCAuth_user1_wrong_issuer": "tgIAAAJqd3QAqAIAAGV5SmhiR2NpT2lKU1V6STFOaUlzSW10cFpDSTZJbU4xYzNSdmJTMXJaWGt0TVNJc0luUjVjQ0k2SWtwWFZDSjkuZXlKcGMzTWlPaUpvZEhSd2N6b3ZMM1JsYzNRdWEyVnlibVZzTGpFd1oyVnVMbU52YlM5dmFXUmpMMmx6YzNWbGNsZ2lMQ0p6ZFdJaU9pSjFjMlZ5TVVCdGIyNW5iMlJpTG1OdmJTSXNJbTVpWmlJNk1UWTJNVE0zTkRBM055d2laWGh3SWpveU1UUTNORGd6TmpRM0xDSmhkV1FpT2xzaWFuZDBRR3RsY201bGJDNXRiMjVuYjJSaUxtTnZiU0pkTENKdWIyNWpaU0k2SW1ka1ptaHFhak15TkdWb2FqSXphelFpTENKdGIyNW5iMlJpTFhKdmJHVnpJanBiSW0xNVVtVmhaRkp2YkdVaVhYMC5RdEFoUC1iTVpzejU5WUxkNzFJazQ3bUNLc3NtUFQzdHVXWnExSVlMbDE1OVVrcG5ZbzVCMk9EZjRXMW9Ga3k2MkFyWkxaSjZWWkhYem41X3lGdW45NlI0cXo2WFd0b2xYUllxdjZxNENqQlRwQmwtd1dBRzNQbTRTTHBzOHRBWUFYdVpSNXBWa0RQY1BvY0ZHMGN3NGktSG5SZDcwd241WHFjM0tPNk1Hd3VuVm04OEZ0R3d3S1FLR1hSVDJ5OHBmYmZZMzRWR2JPbTZ6dF9Wbm9xOVJwRnprYmtwQ1RPZmNZZl9RQUxsc21hR09sVGtKajg3RmZaQmZtclVPQ3ExUEZGN2pDWUlFWTU3UVRxWGpzc0lyYWU0RzdObUd4M3pmWVVpdER3LTN3T1F4T3UwSGJmMDZlV3owSFE5MXptS05LbHMza1poWDZzVmdEdFlSZnYySEEAAA==",

};
