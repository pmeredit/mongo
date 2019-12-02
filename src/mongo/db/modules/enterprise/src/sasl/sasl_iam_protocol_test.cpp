/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/client/sasl_iam_client_protocol.h"
#include "mongo/client/sasl_iam_protocol_common.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/base64.h"
#include "mongo/util/kms_message_support.h"

#include "sasl/sasl_iam_server_protocol.h"

namespace mongo {
namespace iam {
namespace {

static const AWSCredentials defaultCredentials("FAKEFAKEFAKEFAKEFAKE",
                                               "FAKEFAKEFAKEFAKEFAKEfakefakefakefakefake");

// Positive: Test a simple succesful conversation
TEST(SaslIamProtocol, Basic_Success) {
    // Boot KMS message init so that the Windows Crypto is setup
    kms_message_init();
    saslIAMGlobalParams.awsSTSHost = "dummy";

    std::vector<char> clientNonce;
    auto clientFirst = iam::generateClientFirst(&clientNonce);
    std::vector<char> serverNonce;
    char cbFlag;
    std::string principalName;

    auto serverFirst = iam::generateServerFirst(clientFirst, &serverNonce, &cbFlag);

    auto clientSecond = iam::generateClientSecond(serverFirst, clientNonce, defaultCredentials);

    auto httpTuple = iam::parseClientSecond(clientSecond, serverNonce, cbFlag, &principalName);
}

// Positive: Test the ARN is extracted correctly from XML
TEST(SaslIamProtocol, Xml_Good) {
    auto str1 = R"(<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
   <GetCallerIdentityResult>
     <Arn>arn:aws:iam::NUMBER:user/USER_NAME</Arn>
     <UserId>HEX STRING</UserId>
     <Account>NUMBER</Account>
   </GetCallerIdentityResult>
   <ResponseMetadata>
     <RequestId>GUID</RequestId>
   </ResponseMetadata>
 </GetCallerIdentityResponse>)";

    ASSERT_EQUALS("arn:aws:iam::NUMBER:user/USER_NAME", iam::getUserId(str1));
}

// Negative: Fail properly on incorrect xml
TEST(SaslIamProtocol, Xml_Bad) {
    auto str1 = R"(Foo)";

    ASSERT_THROWS(iam::getUserId(str1), std::exception);
}


// Negative: Fail properly on xml missing the information
TEST(SaslIamProtocol, Xml_Bad_Partial) {
    auto str1 = R"(<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
   <GetCallerIdentityResult>
     <UserId>HEX STRING</UserId>
     <Account>NUMBER</Account>
   </GetCallerIdentityResult>
   <ResponseMetadata>
     <RequestId>GUID</RequestId>
   </ResponseMetadata>
 </GetCallerIdentityResponse>)";

    ASSERT_THROWS_CODE(iam::getUserId(str1), AssertionException, 51283);
}


// Negative: Server rejects when the ClientFirst message nonce is the wrong length
TEST(SaslIamProtocol, ClientFirst_ShortNonce) {
    IamClientFirst clientFirst;

    clientFirst.setNonce(std::vector<char>{0x1, 0x2});
    clientFirst.setGs2_cb_flag(static_cast<int>('n'));

    std::vector<char> serverNonce;
    char cbFlag;

    ASSERT_THROWS_CODE(
        iam::generateServerFirst(iam::convertToByteString(clientFirst), &serverNonce, &cbFlag),
        AssertionException,
        51285);
}

// Negative: Server rejects when the ClientFirst has the wrong channel prefix flag
TEST(SaslIamProtocol, ClientFirst_ChannelPrefix) {
    IamClientFirst clientFirst;

    clientFirst.setNonce(std::vector<char>(32, 0));
    clientFirst.setGs2_cb_flag(static_cast<int>('p'));

    std::vector<char> serverNonce;
    char cbFlag;

    ASSERT_THROWS_CODE(
        iam::generateServerFirst(iam::convertToByteString(clientFirst), &serverNonce, &cbFlag),
        AssertionException,
        51284);
}

// Negative: Client rejects when the ServerFirst has a short server nonce
TEST(SaslIamProtocol, ServerFirst_ShortNonce) {
    std::vector<char> clientNonce;
    auto clientFirst = iam::generateClientFirst(&clientNonce);

    IamServerFirst serverFirst;
    serverFirst.setServerNonce(std::vector<char>{0x1, 0x2});
    serverFirst.setStsHost("dummy");

    ASSERT_THROWS_CODE(iam::generateClientSecond(
                           iam::convertToByteString(serverFirst), clientNonce, defaultCredentials),
                       AssertionException,
                       51298);
}


// Negative: Client rejects when the ServerFirst has the wrong client nonce
TEST(SaslIamProtocol, ServerFirst_WrongNonce) {
    std::vector<char> clientNonce;
    auto clientFirst = iam::generateClientFirst(&clientNonce);

    IamServerFirst serverFirst;

    auto serverNoncePiece = iam::generateServerNonce();

    std::vector<char> serverNonce;
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));

    serverFirst.setServerNonce(serverNonce);
    serverFirst.setStsHost("dummy");

    ASSERT_THROWS_CODE(iam::generateClientSecond(
                           iam::convertToByteString(serverFirst), clientNonce, defaultCredentials),
                       AssertionException,
                       51297);
}

void parseServerFirstWithHost(StringData host) {
    std::vector<char> clientNonce;
    auto clientFirst = iam::generateClientFirst(&clientNonce);

    IamServerFirst serverFirst;

    auto serverNoncePiece = iam::generateServerNonce();

    std::vector<char> serverNonce;
    std::copy(clientNonce.begin(), clientNonce.end(), std::back_inserter(serverNonce));
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));

    serverFirst.setServerNonce(serverNonce);
    serverFirst.setStsHost(host);

    iam::generateClientSecond(
        iam::convertToByteString(serverFirst), clientNonce, defaultCredentials);
}


// Negative: Client rejects when the ServerFirst has empty host name
TEST(SaslIamProtocol, ServerFirst_BadHost_Empty) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost(""), AssertionException, 51296);
}


// Negative: Client rejects when the ServerFirst has long host name
TEST(SaslIamProtocol, ServerFirst_BadHost_LongName) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost(std::string(256, 'a')), AssertionException, 51296);
}


// Negative: Client rejects when the ServerFirst has empty dns part
TEST(SaslIamProtocol, ServerFirst_BadHost_EmptyDnsComponent) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost("empty..dns.component"), AssertionException, 51295);
}

void parseWithCustomAuthHeader(StringData authHeader) {
    std::vector<char> clientNonce;
    auto clientFirst = iam::generateClientFirst(&clientNonce);
    std::vector<char> serverNonce;
    char cbFlag;
    std::string principalName;

    auto serverFirst = iam::generateServerFirst(clientFirst, &serverNonce, &cbFlag);

    IamClientSecond second;

    second.setAuthHeader(authHeader);

    second.setXAmzDate("FAKE");

    auto httpTuple = iam::parseClientSecond(
        iam::convertToByteString(second), serverNonce, cbFlag, &principalName);
}

// Negative: Missing basic, required HTTP headers
TEST(SaslIamProtocol, ClientSecond_BadAuth_MissingSignedHeaders) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51293);
}

// Negative: Missing a trailing comma after SignedHeaders
TEST(SaslIamProtocol, ClientSecond_BadAuth_MissingTrailingComma) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-gs2-cb-flag "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51292);
}

// Negative: Missing either the x-mongodb-gs2-cb-flag or x-mongodb-server-nonce flags
TEST(SaslIamProtocol, ClientSecond_BadAuth_MissingRequiredField) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51289);

    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-gs2-cb-flag, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51288);
}

// Negative: SignedHeaders has an extra header
TEST(SaslIamProtocol, ClientSecond_BadAuth_ExtraHeader) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-fake-field;x-mongodb-gs2-"
            "cb-flag;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51290);
}


// Negative: SignedHeaders has an extra header
TEST(SaslIamProtocol, ClientSecond_BadAuth_ExtraHeaderAtEnd) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-gs2-"
            "cb-flag;x-mongodb-server-nonce;x-trailing-fake-field, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51290);
}

// Negative: SignedHeaders has client binding headers which are wrong since we do not support
// channel bindings
TEST(SaslIamProtocol, ClientSecond_BadAuth_WrongBindings) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-channel-type-"
            "prefix;x-mongodb-gs2-cb-flag;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51290);

    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-channel-binding-"
            "data;x-mongodb-gs2-cb-flag;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51290);
}


// Negative: Credential is missing
TEST(SaslIamProtocol, ClientSecond_BadCredential_Missing) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 "
            "SignedHeaders=content-length;content-type;host;x-amz-date;"
            "x-mongodb-gs2-cb-flag;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51742);
}

// Negative: Credential lacks a /
TEST(SaslIamProtocol, ClientSecond_BadCredential_MissingSlash) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE,"
            "SignedHeaders=content-length;content-type;host;x-amz-date;"
            "x-mongodb-gs2-cb-flag;x-mongodb-server-nonce, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51743);
}


// Positive: EC2 instance metadata returns a valid string
TEST(SaslIAMClientProtocolUtil, ParseRole_Basic) {
    ASSERT_EQUALS("foo", iam::parseRoleFromEC2IamSecurityCredentials("foo\n"));
}

// Negative: EC2 instance metadata does not return a valid string
TEST(SaslIAMClientProtocolUtil, ParseRole_Bad) {
    ASSERT_THROWS_CODE(
        iam::parseRoleFromEC2IamSecurityCredentials("foo"), AssertionException, 51294);
}

// Positive: EC2 instance role metadata returns a valid json document
TEST(SaslIAMClientProtocolUtil, EC2ParseTemporaryCreds_Basic) {
    auto credsJson = R"({
    "Code" : "Success",
    "LastUpdated" : "DATE",
    "Type" : "AWS-HMAC",
    "AccessKeyId" : "ACCESS_KEY_ID",
    "SecretAccessKey" : "SECRET_ACCESS_KEY",
    "Token" : "SECURITY_TOKEN_STRING",
    "Expiration" : "EXPIRATION_DATE"
})";

    auto creds = iam::parseCredentialsFromEC2IamSecurityCredentials(credsJson);
    ASSERT_EQUALS(creds.accessKeyId, "ACCESS_KEY_ID");
    ASSERT_EQUALS(creds.secretAccessKey, "SECRET_ACCESS_KEY");
    ASSERT_EQUALS(creds.sessionToken.get(), "SECURITY_TOKEN_STRING");
}

// Positive: ECS Task metadata returns a valid json document
TEST(SaslIAMClientProtocolUtil, ParseECSTemporaryCreds_Basic) {
    auto credsJson = R"({
    "AccessKeyId": "ACCESS_KEY_ID",
    "Expiration": "EXPIRATION_DATE",
    "RoleArn": "TASK_ROLE_ARN",
    "SecretAccessKey": "SECRET_ACCESS_KEY",
    "Token": "SECURITY_TOKEN_STRING"
})";

    auto creds = iam::parseCredentialsFromECSTaskIamCredentials(credsJson);
    ASSERT_EQUALS(creds.accessKeyId, "ACCESS_KEY_ID");
    ASSERT_EQUALS(creds.secretAccessKey, "SECRET_ACCESS_KEY");
    ASSERT_EQUALS(creds.sessionToken.get(), "SECURITY_TOKEN_STRING");
}


// Positive: Test Region extraction
TEST(SaslIAMClientProtocolUtil, TestRegions) {
    ASSERT_EQUALS("us-east-1", iam::getRegionFromHost("sts.amazonaws.com"));
    ASSERT_EQUALS("us-east-1", iam::getRegionFromHost("first"));
    ASSERT_EQUALS("second", iam::getRegionFromHost("first.second"));
    ASSERT_EQUALS("second", iam::getRegionFromHost("first.second.third"));
    ASSERT_EQUALS("us-east-2", iam::getRegionFromHost("sts.us-east-2.amazonaws.com"));
}

// Positive: Test ARN is converted properly
TEST(SaslIAMServerProtocolUtil, ARN_Good) {
    ASSERT_EQUALS("arn:aws:iam::123456789:user/a.user.name",
                  iam::getSimplifiedARN("arn:aws:iam::123456789:user/a.user.name"));
    ASSERT_EQUALS("arn:aws:sts::123456789:assumed-role/ROLE/*",
                  iam::getSimplifiedARN("arn:aws:sts::123456789:assumed-role/ROLE/i-a0912374abc"));
    ASSERT_EQUALS("arn:aws:sts::123456789:assumed-role/ROLE/*",
                  iam::getSimplifiedARN("arn:aws:sts::123456789:assumed-role/ROLE/a.session"));
}

// Negative: Bad ARN fail
TEST(SaslIAMServerProtocolUtil, ARN_Bad) {
    // Wrong service
    ASSERT_THROWS_CODE(iam::getSimplifiedARN("arn:aws:fake::123456789:role/a.user.name"),
                       AssertionException,
                       51282);
    // Runt
    ASSERT_THROWS_CODE(iam::getSimplifiedARN("arn:aws:iam::123456789"), AssertionException, 51281);
    // Wrong suffix for IAM
    ASSERT_THROWS_CODE(iam::getSimplifiedARN("arn:aws:iam::123456789:role/a.user.name"),
                       AssertionException,
                       51280);

    // Missing / in suffix
    ASSERT_THROWS_CODE(
        iam::getSimplifiedARN("arn:aws:sts::123456789:role"), AssertionException, 51279);

    // Missing two /
    ASSERT_THROWS_CODE(iam::getSimplifiedARN("arn:aws:sts::123456789:assumed-role/foo"),
                       AssertionException,
                       51278);

    // Extra /
    ASSERT_THROWS_CODE(iam::getSimplifiedARN("arn:aws:sts::123456789:assumed-role/foo/bar/stuff"),
                       AssertionException,
                       51277);
}

}  // namespace
}  // namespace iam
}  // namespace mongo
