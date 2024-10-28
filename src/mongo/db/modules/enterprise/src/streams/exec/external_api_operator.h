/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/cidr.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/time_support.h"
#include "streams/exec/operator.h"
#include "streams/exec/rate_limiter.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stream_processor_feature_flags.h"

namespace streams {

using StringOrExpression = std::variant<std::string, boost::intrusive_ptr<mongo::Expression>>;

/**
 * ExternalApiOperator is an operator that allows users to make requests to a configured
 * external api in response to input data.
 *
 * This operator requires a WebAPI connection and will make a request to that connection and set
 * the response to the user-configured 'as' field before sending the document  to the next
 * operator.
 */
class ExternalApiOperator : public Operator {
public:
    struct Options {
        std::unique_ptr<mongo::HttpClient> httpClient{};

        // Http method used to make request.
        mongo::HttpClient::HttpMethod requestType{mongo::HttpClient::HttpMethod::kGET};
        // URL used to make an HTTP request with.
        std::string url;
        // Optional url path that is evaluated per document and appended to the url defined in
        // the connection.
        boost::intrusive_ptr<mongo::Expression> urlPathExpr{nullptr};

        // Defined in the connection. Evaluated once on startup.
        std::vector<std::string> connectionHeaders;
        // Query parameters used when making a HTTP request. Evaluated at runtime on each
        // document.
        // TODO(SERVER-95938): make query parameters accept array, object, and numeric literal
        // values.
        std::vector<std::pair<std::string, StringOrExpression>> queryParams;
        // Defined in the operator within the pipeline. Evaluated at runtime on each document.
        std::vector<std::pair<std::string, StringOrExpression>> operatorHeaders;
        // Represents the key that this operator will associate with the response value.
        std::string as;
        // Represents the timeout for establishing a connection to the
        // external api.
        mongo::Seconds connectionTimeoutSecs{30};
        // Represents the timeout for making a request and receiving a response.
        mongo::Seconds requestTimeoutSecs{60};

        // throttleFn is a callback for the behavior that occurs when throttled
        std::function<void(Microseconds)> throttleFn{sleepFor<Microseconds>};

        // timer is the timer to be used for rate limiting
        Timer timer{};
    };

    ExternalApiOperator(Context* context, Options options);

    const Options& getOptions();

protected:
    std::string doGetName() const override {
        return "ExternalApiOperator";
    }

    void doStart() override;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;


private:
    // parseCidrDenyList will retrieve the feature flag value from context and return a new
    // cidrDenyList
    std::vector<mongo::CIDR> parseCidrDenyList();

    // initializeHTTPClient creates an http client, sets http-request-related settings as
    // configured by the user
    void initializeHTTPClient();

    // doRequest creates a request struct, performs the request to the configured URL and
    // returns the response.
    boost::optional<mongo::HttpClient::HttpReply> doRequest(const StreamDocument& doc);

    // makeDocumentWithAPIResponse sets the api response as a value in the input document using
    // a user-configured key.
    mongo::Document makeDocumentWithAPIResponse(mongo::Document inputDoc, mongo::Value apiResponse);

    // evaluateFullUrl accepts an input document and applies a mongo expression using that
    // document to create the urlPath to be appended to the connection uri.
    std::string evaluateFullUrl(const mongo::Document& doc);

    // evaluateHeaders accepts an input document and applies a mongo expression using that
    // document to create a set of headers to be used in the http client.
    std::vector<std::string> evaluateHeaders(const mongo::Document& doc);

    ExternalApiOperator::Options _options;

    int64_t _rateLimitPerSec;
    RateLimiter _rateLimiter;

    std::vector<mongo::CIDR> _cidrDenyList;
};

int64_t getRateLimitPerSec(boost::optional<StreamProcessorFeatureFlags> featureFlags);

}  // namespace streams
