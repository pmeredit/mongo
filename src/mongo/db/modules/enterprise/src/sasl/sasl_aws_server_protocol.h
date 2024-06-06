/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>
#include <tuple>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/client/sasl_aws_protocol_common.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {
namespace awsIam {

/**
 * Server-side global parameters to control IAM Auth
 */
struct SaslAWSGlobalParams {
    /**
     * URL of Amazon STS Endpoint. Used to connect to STS.
     * Must start with https://
     */
    std::string awsSTSUrl;

    /**
     *  Derived from STS URL, just the host name. Used to set the Host: header in HTTP requests.
     */
    std::string awsSTSHost;

    /**
     * Number of times to retry requests to AWS STS.
     * Defaulted in sasl_aws_server_options.idl
     */
    int awsSTSRetryCount{0};

    /**
     * Boolean flag to control whether STS calls use the HTTP connection pool.
     */
    AtomicWord<bool> awsSTSUseConnectionPool;
};

extern SaslAWSGlobalParams saslAWSGlobalParams;

/**
 * Generate server nonce. Used by unit tests.
 */
std::array<char, awsIam::kServerFirstNoncePieceLength> generateServerNonce();

/**
 * Parse the IAM Auth client first message and then generate the server first message.
 *
 * Returns the generated nonce for clients to store.
 */
std::string generateServerFirst(StringData clientFirst,
                                std::vector<char>* serverNonce,
                                char* cbFlag);

/**
 * Parse the IAM Auth lient Second message and return a list of http headers and body to use
 * to contact STS.
 */
std::tuple<std::vector<std::string>, std::string> parseClientSecond(
    StringData clientSecond,
    const std::vector<char>& serverNonce,
    char cbFlag,
    std::string* awsAccountId);

/**
 * Example of a typical response
 * <GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
 *   <GetCallerIdentityResult>
 *     <Arn>arn:aws:iam::NUMBER:user/USER_NAME</Arn>
 *     <UserId>HEX STRING</UserId>
 *     <Account>NUMBER</Account>
 *   </GetCallerIdentityResult>
 *   <ResponseMetadata>
 *     <RequestId>GUID</RequestId>
 *   </ResponseMetadata>
 * </GetCallerIdentityResponse>
 */
std::string getArn(StringData request);

/**
 * ARNS for IAM resources come in the following forms:
 *
 * User:
 *   arn:aws:iam::123456789:user/a.user.name
 *
 * EC2 Role:
 *   arn:aws:sts::123456789:assumed-role/<A_ROLE_NAME>/<i-ec2_instance>
 *
 * Assumed Role:
 *   arn:aws:sts::123456789:assumed-role/<A_ROLE_NAME>/<SESSION_NAME>
 *
 * Return
 * - Users - same as input
 * - Assume Rolee, EC2 Role - last component is changed to *
 *   - arn:aws:sts::123456789:assumed-role/<A_ROLE_NAME>/<star>
 *  Note: "<star>" is used instead of "*" to avoid a clang/gcc warning.
 */
std::string makeSimplifiedArn(StringData arn);

}  // namespace awsIam
}  // namespace mongo
