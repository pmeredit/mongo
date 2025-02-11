/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/cidr.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/time_support.h"
#include "streams/exec/feedable_pipeline.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/planner.h"
#include "streams/exec/rate_limiter.h"

namespace streams {

/**
 * HttpsOperator is an operator that allows users to make requests to a configured
 * destination in response to input data.
 *
 * This operator requires a HTTPS connection and will make a request to that connection and set
 * the response to the user-configured 'as' field before sending the document  to the next
 * operator.
 */
class HttpsOperator : public Operator {
public:
    static constexpr StringData kName = "HttpsOperator";
    static constexpr StringData kHttpsScheme = "https";
    static constexpr StringData kApplicationJson = "application/json";
    static constexpr StringData kTextPlain = "text/plain";

    struct Options {
        std::unique_ptr<mongo::HttpClient> httpClient{};

        // Http method used to make request.
        mongo::HttpClient::HttpMethod method{mongo::HttpClient::HttpMethod::kGET};
        // URL used to make an HTTP request with.
        std::string url;
        // Optional url path that is evaluated per document and appended to the url defined in
        // the connection.
        boost::intrusive_ptr<mongo::Expression> pathExpr{nullptr};

        // Defined in the connection.
        std::vector<std::string> connectionHeaders;
        // Query parameters used when making a HTTP request. Evaluated at runtime on each
        // document.
        std::vector<std::pair<std::string, StringOrExpression>> queryParams;
        // Defined in the operator within the pipeline. Evaluated at runtime on each document.
        std::vector<std::pair<std::string, StringOrExpression>> operatorHeaders;
        // Represents the key that this operator will associate with the response value.
        std::string as;
        // The payloadPipeline is to run against the dataMsg docs passed into the operator.
        boost::optional<FeedablePipeline> payloadPipeline;
        // Represents the timeout for establishing a connection to the
        // HTTPS destination.
        mongo::Seconds connectionTimeoutSecs{30};
        // Represents the timeout for making a request and receiving a response.
        mongo::Seconds requestTimeoutSecs{60};

        // throttleFn is a callback for the behavior that occurs when throttled
        std::function<void(Microseconds)> throttleFn{sleepFor<Microseconds>};

        // timer is the timer to be used for rate limiting
        Timer timer{};

        // Specifies how error responses are handled
        mongo::OnErrorEnum onError{mongo::OnErrorEnum::DLQ};
    };
    struct UrlComponents {
        std::string scheme;
        std::string user;
        std::string password;
        std::string host;
        std::string port;
        std::string path;
        std::vector<std::string> queryParams;
        std::string fragment;
    };

    HttpsOperator(Context* context, Options options);

    const Options& getOptions();

    void registerMetrics(MetricManager* metricManager) override;

    // joinPaths is a helper that handles joining two strings representing url paths.
    static std::string joinPaths(const std::string& basePath, const std::string& path);

protected:
    std::string doGetName() const override {
        return kName.toString();
    }

    void doStart() override;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;


private:
    friend class HttpsOperatorTest;

    static constexpr double kTryLogRate{1.0 / 60};

    struct ProcessResult {
        bool addDocToOutputMsg{false};
        int64_t dataMsgBytes{0};
        int64_t numDlqDocs{0};
        int64_t numDlqBytes{0};
        int64_t numInputBytes{0};
        int64_t numOutputBytes{0};
    };

    ProcessResult processStreamDoc(StreamDocument* streamDoc);

    // evaluateFullUrl accepts an input document and applies a mongo expression using that
    // document
    // to create the path to be appended to the connection uri. It will also call
    // evaluateQueryParams and tack on the query parameters to the end of the URL.
    std::string evaluateFullUrl(const mongo::Document& doc);

    // evaluateHeaders accepts an input document and applies a mongo expression using that
    // document to create a set of headers to be used in the http client.
    std::vector<std::string> evaluateHeaders(const mongo::Document& doc);

    // evaluatePath accepts an input document and applies a mongo expression using that
    // document to create a url path.
    std::string evaluatePath(const mongo::Document& doc);

    // evaluateQueryParams accepts an input document and applies a mongo expression using that
    // document to create a query string.
    std::vector<std::string> evaluateQueryParams(const mongo::Document& doc);

    // parseContentTypeFromHeaders will extract the content-type value if present
    static boost::optional<std::string> parseContentTypeFromHeaders(StringData rawHeaders);

    // makeDocumentWithAPIResponse sets the api response as a value in the input document using
    // a user-configured key.
    mongo::Document makeDocumentWithAPIResponse(const mongo::Document& inputDoc,
                                                mongo::Value apiResponse);

    // parseBaseUrl parses the base url provided via the options member variable sets a member
    // representing the parsed components of the provided base url.
    void parseBaseUrl();

    // makeUrlString accepts additional path segments and query params to form a url string.
    std::string makeUrlString(std::string path, std::vector<std::string> queryParams);

    // parseCidrDenyList will retrieve the feature flag value from context and return a new
    // cidrDenyList
    std::vector<mongo::CIDR> parseCidrDenyList();

    // initializeHTTPClient creates an http client, sets http-request-related settings as
    // configured by the user
    void initializeHTTPClient();

    // writeToStreamMeta writes to a StreamDocuments' stream meta
    void writeToStreamMeta(StreamDocument* streamDoc,
                           const std::string& requestUrl,
                           mongo::HttpClient::HttpReply httpResponse,
                           double responseTimeMs);

    // writeToDLQ writes StreamDocument to the DLQ with a specified error message and operator stats
    // for the given document
    void writeToDLQ(StreamDocument* streamDoc,
                    const mongo::Document& payloadDoc,
                    const std::string& errorMsg,
                    ProcessResult& result);

    // tryLog will only log if the given logID hasn't been used within the last minute
    void tryLog(int id, std::function<void(int logID)> logFn);

    // validateOptions is called when the operator is initialized and checks whether the options
    // passed into the operator are valid. Throws a user error if not valid.
    void validateOptions();

    // useOnErrorBehaviorForStatusCode is called to determine if we should use the onError behavior
    // vs failing the processor.
    bool useOnErrorBehaviorForStatusCode(std::uint16_t code) {
        return code == 400 || code == 404 || code == 410 || code == 413 || code == 414 ||
            code == 431;
    };

    HttpsOperator::Options _options;

    int64_t _rateLimitPerSec;
    RateLimiter _rateLimiter;
    // staticEncodedUrl is defined only if the operator has no per-document evaluations to perform
    // that modifies the url.
    std::string _staticEncodedUrl;
    std::vector<mongo::CIDR> _cidrDenyList;
    UrlComponents _baseUrlComponents;

    std::shared_ptr<Counter> _throttleDurationCounter;

    stdx::unordered_map<int, std::unique_ptr<RateLimiter>> _logIDToRateLimiter;
};

}  // namespace streams
