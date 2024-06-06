/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/client/sasl_aws_client_protocol.h"
#include "mongo/client/sasl_aws_protocol_common.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/base64.h"
#include "mongo/util/kms_message_support.h"

#include "sasl/sasl_aws_server_protocol.h"

namespace mongo {
namespace awsIam {
namespace {

static const AWSCredentials defaultCredentials("FAKEFAKEFAKEFAKEFAKE",
                                               "FAKEFAKEFAKEFAKEFAKEfakefakefakefakefake");

// Positive: Test a simple succesful conversation
TEST(SaslAwsProtocol, Basic_Success) {
    // Boot KMS message init so that the Windows Crypto is setup
    kms_message_init();
    saslAWSGlobalParams.awsSTSHost = "dummy";

    std::vector<char> clientNonce;
    auto clientFirst = awsIam::generateClientFirst(&clientNonce);
    std::vector<char> serverNonce;
    char cbFlag;
    std::string principalName;

    auto serverFirst = awsIam::generateServerFirst(clientFirst, &serverNonce, &cbFlag);

    auto clientSecond = awsIam::generateClientSecond(serverFirst, clientNonce, defaultCredentials);

    auto httpTuple = awsIam::parseClientSecond(clientSecond, serverNonce, cbFlag, &principalName);
}

// Positive: Test the ARN is extracted correctly from XML
TEST(SaslAwsProtocol, Xml_Good) {
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

    ASSERT_EQUALS("arn:aws:iam::NUMBER:user/USER_NAME", awsIam::getArn(str1));
}

// Negative: Fail properly on incorrect xml
TEST(SaslAwsProtocol, Xml_Bad) {
    auto str1 = R"(Foo)";

    ASSERT_THROWS(awsIam::getArn(str1), std::exception);
}


// Negative: Fail properly on xml missing the information
TEST(SaslAwsProtocol, Xml_Bad_Partial) {
    auto str1 = R"(<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
   <GetCallerIdentityResult>
     <UserId>HEX STRING</UserId>
     <Account>NUMBER</Account>
   </GetCallerIdentityResult>
   <ResponseMetadata>
     <RequestId>GUID</RequestId>
   </ResponseMetadata>
 </GetCallerIdentityResponse>)";

    ASSERT_THROWS_CODE(awsIam::getArn(str1), AssertionException, 51283);
}


// Negative: Server rejects when the ClientFirst message nonce is the wrong length
TEST(SaslAwsProtocol, ClientFirst_ShortNonce) {
    AwsClientFirst clientFirst;

    clientFirst.setNonce(std::vector<char>{0x1, 0x2});
    clientFirst.setGs2_cb_flag(static_cast<int>('n'));

    std::vector<char> serverNonce;
    char cbFlag;

    ASSERT_THROWS_CODE(awsIam::generateServerFirst(
                           awsIam::convertToByteString(clientFirst), &serverNonce, &cbFlag),
                       AssertionException,
                       51285);
}

// Negative: Server rejects when the ClientFirst has the wrong channel prefix flag
TEST(SaslAwsProtocol, ClientFirst_ChannelPrefix) {
    AwsClientFirst clientFirst;

    clientFirst.setNonce(std::vector<char>(32, 0));
    clientFirst.setGs2_cb_flag(static_cast<int>('p'));

    std::vector<char> serverNonce;
    char cbFlag;

    ASSERT_THROWS_CODE(awsIam::generateServerFirst(
                           awsIam::convertToByteString(clientFirst), &serverNonce, &cbFlag),
                       AssertionException,
                       51284);
}

// Negative: Client rejects when the ServerFirst has a short server nonce
TEST(SaslAwsProtocol, ServerFirst_ShortNonce) {
    std::vector<char> clientNonce;
    auto clientFirst = awsIam::generateClientFirst(&clientNonce);

    AwsServerFirst serverFirst;
    serverFirst.setServerNonce(std::vector<char>{0x1, 0x2});
    serverFirst.setStsHost("dummy");

    ASSERT_THROWS_CODE(awsIam::generateClientSecond(awsIam::convertToByteString(serverFirst),
                                                    clientNonce,
                                                    defaultCredentials),
                       AssertionException,
                       51298);
}


// Negative: Client rejects when the ServerFirst has the wrong client nonce
TEST(SaslAwsProtocol, ServerFirst_WrongNonce) {
    std::vector<char> clientNonce;
    auto clientFirst = awsIam::generateClientFirst(&clientNonce);

    AwsServerFirst serverFirst;

    auto serverNoncePiece = awsIam::generateServerNonce();

    std::vector<char> serverNonce;
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));

    serverFirst.setServerNonce(serverNonce);
    serverFirst.setStsHost("dummy");

    ASSERT_THROWS_CODE(awsIam::generateClientSecond(awsIam::convertToByteString(serverFirst),
                                                    clientNonce,
                                                    defaultCredentials),
                       AssertionException,
                       51297);
}

void parseServerFirstWithHost(StringData host) {
    std::vector<char> clientNonce;
    auto clientFirst = awsIam::generateClientFirst(&clientNonce);

    AwsServerFirst serverFirst;

    auto serverNoncePiece = awsIam::generateServerNonce();

    std::vector<char> serverNonce;
    std::copy(clientNonce.begin(), clientNonce.end(), std::back_inserter(serverNonce));
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(serverNonce));

    serverFirst.setServerNonce(serverNonce);
    serverFirst.setStsHost(host);

    awsIam::generateClientSecond(
        awsIam::convertToByteString(serverFirst), clientNonce, defaultCredentials);
}


// Negative: Client rejects when the ServerFirst has empty host name
TEST(SaslAwsProtocol, ServerFirst_BadHost_Empty) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost(""), AssertionException, 51296);
}


// Negative: Client rejects when the ServerFirst has long host name
TEST(SaslAwsProtocol, ServerFirst_BadHost_LongName) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost(std::string(256, 'a')), AssertionException, 51296);
}


// Negative: Client rejects when the ServerFirst has empty dns part
TEST(SaslAwsProtocol, ServerFirst_BadHost_EmptyDnsComponent) {
    ASSERT_THROWS_CODE(parseServerFirstWithHost("empty..dns.component"), AssertionException, 51295);
}

void parseWithCustomAuthHeader(StringData authHeader) {
    std::vector<char> clientNonce;
    auto clientFirst = awsIam::generateClientFirst(&clientNonce);
    std::vector<char> serverNonce;
    char cbFlag;
    std::string principalName;

    auto serverFirst = awsIam::generateServerFirst(clientFirst, &serverNonce, &cbFlag);

    AwsClientSecond second;

    second.setAuthHeader(authHeader);

    second.setXAmzDate("FAKE");

    auto httpTuple = awsIam::parseClientSecond(
        awsIam::convertToByteString(second), serverNonce, cbFlag, &principalName);
}

// Negative: Missing basic, required HTTP headers
TEST(SaslAwsProtocol, ClientSecond_BadAuth_MissingSignedHeaders) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51293);
}

// Negative: Missing a trailing comma after SignedHeaders
TEST(SaslAwsProtocol, ClientSecond_BadAuth_MissingTrailingComma) {
    ASSERT_THROWS_CODE(
        parseWithCustomAuthHeader(
            "Actual: AWS4-HMAC-SHA256 Credential=FAKEFAKEFAKE/20191107/us-east-1/sts/aws4_request, "
            "SignedHeaders=content-length;content-type;host;x-amz-date;x-mongodb-gs2-cb-flag "
            "Signature=ab62ce1c75f19c4c8b918b2ed63b46512765ed9b8bb5d79b374ae83eeac11f55"),
        AssertionException,
        51292);
}

// Negative: Missing either the x-mongodb-gs2-cb-flag or x-mongodb-server-nonce flags
TEST(SaslAwsProtocol, ClientSecond_BadAuth_MissingRequiredField) {
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
TEST(SaslAwsProtocol, ClientSecond_BadAuth_ExtraHeader) {
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
TEST(SaslAwsProtocol, ClientSecond_BadAuth_ExtraHeaderAtEnd) {
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
TEST(SaslAwsProtocol, ClientSecond_BadAuth_WrongBindings) {
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
TEST(SaslAwsProtocol, ClientSecond_BadCredential_Missing) {
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
TEST(SaslAwsProtocol, ClientSecond_BadCredential_MissingSlash) {
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
TEST(SaslAWSClientProtocolUtil, ParseRole_Basic) {
    ASSERT_EQUALS("foo", awsIam::parseRoleFromEC2IamSecurityCredentials("foo\n"));
}

// Positive: EC2 instance metadata does not end with newline
TEST(SaslAWSClientProtocolUtil, ParseRole_Good) {
    ASSERT_EQUALS("foo", awsIam::parseRoleFromEC2IamSecurityCredentials("foo"));
}

// Positive: EC2 instance role metadata returns a valid json document
TEST(SaslAWSClientProtocolUtil, EC2ParseTemporaryCreds_Basic) {
    auto credsJson = R"({
    "Code" : "Success",
    "LastUpdated" : "DATE",
    "Type" : "AWS-HMAC",
    "AccessKeyId" : "ACCESS_KEY_ID",
    "SecretAccessKey" : "SECRET_ACCESS_KEY",
    "Token" : "SECURITY_TOKEN_STRING",
    "Expiration" : "EXPIRATION_DATE"
})";

    auto creds = awsIam::parseCredentialsFromEC2IamSecurityCredentials(credsJson);
    ASSERT_EQUALS(creds.accessKeyId, "ACCESS_KEY_ID");
    ASSERT_EQUALS(creds.secretAccessKey, "SECRET_ACCESS_KEY");
    ASSERT_EQUALS(creds.sessionToken.value(), "SECURITY_TOKEN_STRING");
}

// Positive: ECS Task metadata returns a valid json document
TEST(SaslAWSClientProtocolUtil, ParseECSTemporaryCreds_Basic) {
    auto credsJson = R"({
    "AccessKeyId": "ACCESS_KEY_ID",
    "Expiration": "EXPIRATION_DATE",
    "RoleArn": "TASK_ROLE_ARN",
    "SecretAccessKey": "SECRET_ACCESS_KEY",
    "Token": "SECURITY_TOKEN_STRING"
})";

    auto creds = awsIam::parseCredentialsFromECSTaskIamCredentials(credsJson);
    ASSERT_EQUALS(creds.accessKeyId, "ACCESS_KEY_ID");
    ASSERT_EQUALS(creds.secretAccessKey, "SECRET_ACCESS_KEY");
    ASSERT_EQUALS(creds.sessionToken.value(), "SECURITY_TOKEN_STRING");
}


// Positive: Test Region extraction
TEST(SaslAWSClientProtocolUtil, TestRegions) {
    ASSERT_EQUALS("us-east-1", awsIam::getRegionFromHost("sts.amazonaws.com"));
    ASSERT_EQUALS("us-east-1", awsIam::getRegionFromHost("first"));
    ASSERT_EQUALS("second", awsIam::getRegionFromHost("first.second"));
    ASSERT_EQUALS("second", awsIam::getRegionFromHost("first.second.third"));
    ASSERT_EQUALS("us-east-2", awsIam::getRegionFromHost("sts.us-east-2.amazonaws.com"));
}

// Positive: Test ARN is converted properly
TEST(SaslAWSServerProtocolUtil, ARN_Good) {
    ASSERT_EQUALS("arn:aws:iam::123456789:user/a.user.name",
                  awsIam::makeSimplifiedArn("arn:aws:iam::123456789:user/a.user.name"));
    ASSERT_EQUALS(
        "arn:aws:sts::123456789:assumed-role/ROLE/*",
        awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role/ROLE/i-a0912374abc"));
    ASSERT_EQUALS("arn:aws:sts::123456789:assumed-role/ROLE/*",
                  awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role/ROLE/a.session"));

    // resource-id as segment.
    ASSERT_EQUALS(
        "arn:aws:sts::123456789:assumed-role/ROLE/*",
        awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role:ROLE/i-a0912374abc"));

    // Alternative partitions.
    ASSERT_EQUALS("arn:aws-us-gov:iam::123456789:user/a.user.name",
                  awsIam::makeSimplifiedArn("arn:aws-us-gov:iam::123456789:user/a.user.name"));
    ASSERT_EQUALS("arn:aws-cn:iam::123456789:user/a.user.name",
                  awsIam::makeSimplifiedArn("arn:aws-cn:iam::123456789:user/a.user.name"));
}


// Positive: Test ARN is converted properly with path /
TEST(SaslAWSServerProtocolUtil, ARN_Good_With_Path) {
    ASSERT_EQUALS("arn:aws:iam::123456789012:user/division_abc/subdivision_xyz/JaneDoe",
                  awsIam::makeSimplifiedArn(
                      "arn:aws:iam::123456789012:user/division_abc/subdivision_xyz/JaneDoe"));
    ASSERT_EQUALS("arn:aws:sts::123456789:assumed-role/ROLE/WITH/PATH/*",
                  awsIam::makeSimplifiedArn(
                      "arn:aws:sts::123456789:assumed-role/ROLE/WITH/PATH/i-a0912374abc"));
    ASSERT_EQUALS(
        "arn:aws:sts::123456789:assumed-role/ROLE/WITH/PATH/*",
        awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role/ROLE/WITH/PATH/a.session"));
}


// Negative: Bad ARN fail
TEST(SaslAWSServerProtocolUtil, ARN_Bad) {
    // Not an arn
    ASSERT_THROWS_CODE(awsIam::makeSimplifiedArn("ran:aws:fake::123456789:role/a.user.name"),
                       AssertionException,
                       5479901);
    // Wrong service
    ASSERT_THROWS_CODE(awsIam::makeSimplifiedArn("arn:aws:fake::123456789:role/a.user.name"),
                       AssertionException,
                       5479902);
    // Runt
    ASSERT_THROWS_CODE(
        awsIam::makeSimplifiedArn("arn:aws:iam::123456789"), AssertionException, 5479900);
    // Wrong suffix for IAM
    ASSERT_THROWS_CODE(awsIam::makeSimplifiedArn("arn:aws:iam::123456789:role/a.user.name"),
                       AssertionException,
                       51280);
    // Wrong suffix for STS
    ASSERT_THROWS_CODE(
        awsIam::makeSimplifiedArn("arn:aws:sts::123456789:role"), AssertionException, 51279);

    // Missing / in suffix
    ASSERT_THROWS_CODE(awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role"),
                       AssertionException,
                       5479903);

    // Missing two /
    ASSERT_THROWS_CODE(awsIam::makeSimplifiedArn("arn:aws:sts::123456789:assumed-role/foo"),
                       AssertionException,
                       51278);
}

}  // namespace
}  // namespace awsIam
}  // namespace mongo
