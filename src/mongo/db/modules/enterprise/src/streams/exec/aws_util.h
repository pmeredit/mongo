#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/lambda/LambdaClient.h>

#include "mongo/base/string_data.h"
#include "streams/exec/context.h"

namespace streams {

// Streams implementation to support credential refresh for AWS Service Clients.
class AWSCredentialsProvider : public Aws::Auth::AWSCredentialsProvider {
public:
    AWSCredentialsProvider(Context* context, std::string connectionName);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

protected:
    void Reload() override;

private:
    void RefreshIfExpired();

    Aws::Auth::AWSCredentials _credentials;
    Context* _context{nullptr};
    std::string _connectionName;
};

// Base class to support mocking the AWS's lambda client.
class LambdaClient {
public:
    LambdaClient(){};
    virtual ~LambdaClient() = default;

    virtual Aws::Lambda::Model::InvokeOutcome Invoke(
        const Aws::Lambda::Model::InvokeRequest& request) const = 0;
};

// Wrapper class for AWS's lambda client.
class AWSLambdaClient : public LambdaClient {
public:
    AWSLambdaClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
                    const Aws::Client::ClientConfiguration& clientConfiguration);

    Aws::Lambda::Model::InvokeOutcome Invoke(
        const Aws::Lambda::Model::InvokeRequest& request) const override;

private:
    Aws::Lambda::LambdaClient _client;
};

bool isAWSRegion(mongo::StringData region);

};  // namespace streams
