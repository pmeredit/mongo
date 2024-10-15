/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/http_client.h"
#include "streams/exec/operator.h"
#include "streams/exec/stages_gen.h"

namespace streams {

/**
 * ExternalApiOperator is an operator that allows users to make requests to a configured external
 * api in response to input data.
 *
 * This operator requires a WebAPI connection and will make a request to that connection and set the
 * response to the user-configured 'as' field before sending the document  to the next operator.
 */
class ExternalApiOperator : public Operator {
public:
    struct Options {
        // URI used to make an HTTP request with.
        std::string uri;
        // Query parameters used when making a HTTP request.
        mongo::BSONObj params;
        mongo::HttpClient::HttpMethod requestType{mongo::HttpClient::HttpMethod::kGET};
        std::unique_ptr<mongo::HttpClient> httpClient{};

        // Defined in the connection. Evaluated once on startup.
        std::vector<std::string> connectionHeaders;
        // Defined in the operator within the pipeline. Evaluated at runtime on each document.
        boost::intrusive_ptr<mongo::Expression> operatorHeadersExpr;
        // as represents the key that this operator will associate with the response value.
        std::string as;
        // connectionTimeoutSecs represents the timeout for establishing a connection to the
        // external api.
        mongo::Seconds connectionTimeoutSecs{30};
        // requestTimeoutSecs represents the timeout for making a request and receiving a response.
        mongo::Seconds requestTimeoutSecs{60};
    };

    ExternalApiOperator(Context* context, Options options);

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
    // initializeHTTPClient creates an http client, sets http-request-related settings as configured
    // by the user
    void initializeHTTPClient();

    // doRequest creates a request struct, performs the request to the configured URL and returns
    // the response.
    boost::optional<mongo::HttpClient::HttpReply> doRequest(const StreamDocument& doc);

    // makeDocumentWithAPIResponse sets the api response as a value in the input document using a
    // user-configured key.
    mongo::Document makeDocumentWithAPIResponse(mongo::Document inputDoc, mongo::Value apiResponse);

    // evaluateHeaders accepts an input document and applies a mongo expression using that document
    // to create a set of headers to be used in the http client.
    std::vector<std::string> evaluateHeaders(const mongo::Document& doc);

    ExternalApiOperator::Options _options;
};

}  // namespace streams
