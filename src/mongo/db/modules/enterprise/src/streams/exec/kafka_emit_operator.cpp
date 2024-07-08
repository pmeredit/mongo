/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <exception>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/kafka_emit_operator.h"

#include "mongo/base/status.h"
#include "mongo/bson/bsontypes_util.h"
#include "mongo/bson/json.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/kafka_event_callback.h"
#include "streams/exec/log_util.h"
#include "streams/exec/util.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using namespace fmt::literals;

KafkaEmitOperator::Connector::Connector(Options options) : _options(std::move(options)) {
    tassert(ErrorCodes::InternalError,
            "Expected kafkaEventCallback to be set",
            _options.kafkaEventCallback);
}

KafkaEmitOperator::Connector::~Connector() {
    stop();
}

void KafkaEmitOperator::Connector::setConnectionStatus(ConnectionStatus status) {
    stdx::unique_lock lock(_mutex);
    _connectionStatus = status;
}

void KafkaEmitOperator::Connector::start() {
    invariant(!_connectionThread.joinable());
    _connectionThread = stdx::thread{[this]() {
        try {
            testConnection();
            setConnectionStatus({ConnectionStatus::kConnected});
        } catch (const SPException& e) {
            setConnectionStatus(
                {ConnectionStatus::kError,
                 _options.kafkaEventCallback->appendRecentErrorsToStatus(e.toStatus())});
        } catch (const DBException& e) {
            setConnectionStatus(
                {ConnectionStatus::kError,
                 _options.kafkaEventCallback->appendRecentErrorsToStatus(e.toStatus())});
        } catch (const std::exception& e) {
            SPStatus status(
                mongo::Status{ErrorCodes::InternalError,
                              std::string("Unexpected error while connecting to kafka $emit")},
                e.what());
            setConnectionStatus({ConnectionStatus::kError, std::move(status)});
        }
    }};
}

void KafkaEmitOperator::Connector::stop() {
    if (_connectionThread.joinable()) {
        // Wait for the connection thread to exit.
        _connectionThread.join();
    }
}

ConnectionStatus KafkaEmitOperator::Connector::getConnectionStatus() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _connectionStatus;
}

void KafkaEmitOperator::Connector::testConnection() {
    // Validate that the connection can be established by querying metadata.
    RdKafka::Metadata* metadata{nullptr};
    if (_options.topicName) {
        std::string errstr;
        std::unique_ptr<RdKafka::Topic> topic{RdKafka::Topic::create(_options.producer,
                                                                     *_options.topicName,
                                                                     /*conf*/ nullptr,
                                                                     errstr)};
        if (!topic) {
            uasserted(8141700,
                      "$emit to Kafka failed to connect to topic with error: {}"_format(errstr));
        }

        RdKafka::ErrorCode kafkaErrorCode = _options.producer->metadata(
            false /* all_topics */, topic.get(), &metadata, _options.metadataQueryTimeout.count());
        std::unique_ptr<RdKafka::Metadata> deleter(metadata);
        uassert(
            8141701,
            "$emit to Kafka topic encountered error while connecting, kafka error code: {}"_format(
                kafkaErrorCode),
            kafkaErrorCode == RdKafka::ERR_NO_ERROR);
    } else {
        RdKafka::ErrorCode kafkaErrorCode = _options.producer->metadata(
            true /* all_topics */, nullptr, &metadata, _options.metadataQueryTimeout.count());
        std::unique_ptr<RdKafka::Metadata> deleter(metadata);
        uassert(8141702,
                "$emit to Kafka encountered error while connecting, kafka error code: {}"_format(
                    kafkaErrorCode),
                kafkaErrorCode == RdKafka::ERR_NO_ERROR);
    }
}


std::unique_ptr<RdKafka::Conf> KafkaEmitOperator::createKafkaConf() {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    _eventCbImpl = std::make_unique<KafkaEventCallback>(_context, getName());

    auto setConf = [confPtr = conf.get()](const std::string& confName, auto confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(8720702,
                      str::stream() << "Failed while setting configuration " << confName
                                    << " with error: " << errstr);
        }
    };
    setConf("bootstrap.servers", _options.bootstrapServers);
    // Do not log broker disconnection messages.
    setConf("log.connection.close", "false");
    // Do not refresh topic or broker metadata.
    setConf("topic.metadata.refresh.interval.ms", "-1");
    // Set the event callback.
    setConf("event_cb", _eventCbImpl.get());

    // Set the resolve callback.
    if (_options.gwproxyEndpoint) {
        _resolveCbImpl = std::make_unique<KafkaResolveCallback>(
            _context, getName() /* operator name */, *_options.gwproxyEndpoint /* target proxy */);
        setConf("resolve_cb", _resolveCbImpl.get());

        // Set the connect callback if authentication is required.
        if (_options.gwproxyKey) {
            _connectCbImpl = std::make_unique<KafkaConnectAuthCallback>(
                _context,
                getName() /* operator name */,
                *_options.gwproxyKey /* symmetric key */,
                10 /* connection timeout unit:seconds */);
            setConf("connect_cb", _connectCbImpl.get());
        }
    }

    // Set auth related configurations.
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    // Configure the underlying kafka producer queue with sensible defaults. In particular:
    // - We wish to allow up to one second for events to accumulate (this reduces the overhead for
    // sending messages to our broker).
    // - We want to have a relatively low memory footprint, so allow our queue to buffer up to 16MB
    // of data (or, 16384KB).
    // - Finally, we configure the maximum number of documents to the default, which is 100k. We
    // don't expect to hit this as this is relatively high compared to the maximum memory limit.
    setConf("queue.buffering.max.ms", "1000");
    setConf("queue.buffering.max.kbytes", "16384");
    setConf("queue.buffering.max.messages", "100000");
    // This is the maximum time librdkafka may use to deliver a message (including retries).
    // Set to 10 seconds.
    setConf("message.timeout.ms", "30000");
    return conf;
}

KafkaEmitOperator::KafkaEmitOperator(Context* context, Options options)
    : SinkOperator(context, /* numInputs */ 1),
      _options(std::move(options)),
      _expCtx(context->expCtx) {
    _conf = createKafkaConf();

    std::string errstr;
    _producer.reset(RdKafka::Producer::create(_conf.get(), errstr));
    uassert(
        8720703, str::stream() << "Failed to create producer with error: " << errstr, _producer);
    if (_options.testOnlyPartition) {
        _outputPartition = *_options.testOnlyPartition;
    }
}

void KafkaEmitOperator::doSinkOnDataMsg(int32_t inputIdx,
                                        StreamDataMsg dataMsg,
                                        boost::optional<StreamControlMsg> controlMsg) {
    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};
    int64_t numOutputDocs{0};
    int64_t numOutputBytes{0};
    bool samplersPresent = samplersExist();
    for (auto& streamDoc : dataMsg.docs) {
        try {
            processStreamDoc(streamDoc);
            numOutputDocs++;
            numOutputBytes += streamDoc.doc.memUsageForSorter();
            if (samplersPresent) {
                StreamDataMsg msg;
                msg.docs.push_back(streamDoc);
                sendOutputToSamplers(std::move(msg));
            }
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            numDlqBytes += _context->dlq->addMessage(
                toDeadLetterQueueMsg(_context->streamMetaFieldName, streamDoc, std::move(error)));
            ++numDlqDocs;
        }
    }
    incOperatorStats({.numOutputDocs = numOutputDocs,
                      .numOutputBytes = numOutputBytes,
                      .numDlqDocs = numDlqDocs,
                      .numDlqBytes = numDlqBytes});
}

namespace {
static constexpr size_t kMaxTopicNamesCacheSize = 100;
}

std::vector<char> serializeInt(int32_t valueInt) {
    // Big-endian serialization
    std::vector<char> intBytes(sizeof(valueInt), 0);
    DataView(intBytes.data()).write<BigEndian<int32_t>>(valueInt);
    return intBytes;
}

std::vector<char> serializeLong(int64_t valueLong) {
    // Big-endian serialization
    std::vector<char> longBytes(sizeof(valueLong), 0);
    DataView(longBytes.data()).write<BigEndian<int64_t>>(valueLong);
    return longBytes;
}

std::vector<char> serializeDouble(double valueDouble) {
    // Big-endian serialization
    std::vector<char> doubleBytes(sizeof(valueDouble), 0);
    DataView(doubleBytes.data()).write<BigEndian<double>>(valueDouble);
    return doubleBytes;
}

void KafkaEmitOperator::serializeToHeaders(RdKafka::Headers* headers,
                                           const std::string& topicName,
                                           const std::string& headerKey,
                                           Value headerValue) {
    RdKafka::ErrorCode err = RdKafka::ERR_NO_ERROR;
    auto pushBinaryHeader = [&](RdKafka::Headers* headers,
                                const std::string& key,
                                const void* valuePointer,
                                size_t valueLength) {
        err = headers->add(std::move(key), valuePointer, valueLength);
        uassert(9136400,
                "Failed to emit to topic {} due to error during adding to Kafka headers: {}"_format(
                    topicName, err),
                err == RdKafka::ERR_NO_ERROR);
    };

    auto pushStringHeader = [&](RdKafka::Headers* headers,
                                const std::string& key,
                                std::string value) {
        err = headers->add(std::move(key), value);
        uassert(9136401,
                "Failed to emit to topic {} due to error during adding to Kafka headers: {}"_format(
                    topicName, err),
                err == RdKafka::ERR_NO_ERROR);
    };

    auto valueType = headerValue.getType();
    if (headerValue.missing() || valueType == jstNULL) {
        std::vector<uint8_t> emptyBytes{};
        BSONBinData emptyBinData{};
        pushBinaryHeader(headers, headerKey, emptyBinData.data, emptyBinData.length);
        return;
    }

    switch (valueType) {
        case BinData: {
            auto binData = headerValue.getBinData();
            pushBinaryHeader(headers, headerKey, binData.data, binData.length);
        } break;
        case String: {
            pushStringHeader(headers, headerKey, headerValue.getString());
        } break;
        case Object: {
            auto obj = headerValue.getDocument().toBson();
            auto asJson = tojson(obj, _options.jsonStringFormat, false /* pretty */);
            pushStringHeader(headers, headerKey, asJson);
        } break;
        case NumberInt: {
            int32_t valueInt = headerValue.getInt();
            std::vector<char> intBytes = serializeInt(valueInt);
            pushBinaryHeader(headers, headerKey, intBytes.data(), intBytes.size());
        } break;
        case NumberLong: {
            int64_t valueLong = headerValue.getLong();
            std::vector<char> longBytes = serializeLong(valueLong);
            pushBinaryHeader(headers, headerKey, longBytes.data(), longBytes.size());
        } break;
        case NumberDouble: {
            double valueDouble = headerValue.getDouble();
            std::vector<char> doubleBytes = serializeDouble(valueDouble);
            pushBinaryHeader(headers, headerKey, doubleBytes.data(), doubleBytes.size());
        } break;
        default:
            uasserted(ErrorCodes::BadValue,
                      str::stream() << "Header value has invalid type: " << str::stream()
                                    << headerValue.getType());
    }
};

RdKafka::Headers* KafkaEmitOperator::createKafkaHeaders(const StreamDocument& streamDoc,
                                                        std::string topicName) {
    RdKafka::ErrorCode err = RdKafka::ERR_NO_ERROR;
    RdKafka::Headers* headers = nullptr;
    ON_BLOCK_EXIT([&] {
        if (err != RdKafka::ERR_NO_ERROR && headers != nullptr) {
            delete headers;
        }
    });
    if (_options.headers) {
        Value headersField =
            _options.headers->evaluate(streamDoc.doc, &_context->expCtx->variables);
        if (!headersField.missing()) {
            auto createHeaders = []() {
                auto headers = RdKafka::Headers::create();
                uassert(8797000,
                        "Failed to create Kafka message headers during $emit",
                        headers != nullptr);
                return headers;
            };

            if (headersField.getType() == Array) {
                auto& headersArray = headersField.getArray();
                headers = createHeaders();
                for (const auto& headerField : headersArray) {
                    uassert(ErrorCodes::BadValue,
                            "Each header must be of type Object",
                            headerField.getType() == Object);
                    auto headerObject = headerField.getDocument();
                    auto headerKeyField = headerObject.getField("k");
                    auto headerValueField = headerObject.getField("v");
                    uassert(ErrorCodes::BadValue,
                            "Each header key must be of type String",
                            headerKeyField.getType() == String);
                    auto headerKey = headerKeyField.getStringData();
                    serializeToHeaders(headers, topicName, headerKey.toString(), headerValueField);
                }
            } else if (headersField.getType() == Object) {
                auto headersObj = headersField.getDocument();
                headers = createHeaders();
                auto it = headersObj.fieldIterator();
                while (it.more()) {
                    auto field = it.next();
                    auto headerValue = field.second;
                    serializeToHeaders(headers, topicName, field.first.toString(), field.second);
                }
            } else {
                uasserted(ErrorCodes::BadValue,
                          "Kafka $emit header expression must evaluate to an Object or an Array");
            }
        }
    }
    return headers;
}

Value KafkaEmitOperator::createKafkaKey(const StreamDocument& streamDoc) {
    if (_options.key) {
        Value keyField = _options.key->evaluate(streamDoc.doc, &_context->expCtx->variables);
        if (!keyField.missing()) {
            auto unexpectedKeyTypeError = [](BSONType type) {
                return (str::stream()
                        << "Kafka $emit key expression must evaluate to '" << typeName(type)
                        << "' according to the "
                           "specified key format")
                    .ss.str();
            };

            switch (_options.keyFormat) {
                case mongo::KafkaKeyFormatEnum::BinData: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(BinData),
                            keyField.getType() == BinData);
                    return keyField;
                }
                case mongo::KafkaKeyFormatEnum::String: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(String),
                            keyField.getType() == String);
                    return keyField;
                }
                case mongo::KafkaKeyFormatEnum::Json: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(Object),
                            keyField.getType() == Object);
                    auto keyObject = keyField.getDocument().toBson();
                    auto keyJson = tojson(keyObject, _options.jsonStringFormat, false /* pretty */);
                    return Value(std::move(keyJson));
                }
                case mongo::KafkaKeyFormatEnum::Int: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(NumberInt),
                            keyField.getType() == NumberInt);
                    int32_t keyInt = keyField.getInt();
                    std::vector<char> keyIntBytes = serializeInt(keyInt);
                    Value keyHolder(BSONBinData{keyIntBytes.data(),
                                                static_cast<int>(keyIntBytes.size()),
                                                mongo::BinDataGeneral});
                    return keyHolder;
                }
                case mongo::KafkaKeyFormatEnum::Long: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(NumberLong),
                            keyField.getType() == NumberLong);
                    int64_t keyLong = keyField.getLong();
                    std::vector<char> keyLongBytes = serializeLong(keyLong);
                    Value keyHolder(BSONBinData{keyLongBytes.data(),
                                                static_cast<int>(keyLongBytes.size()),
                                                mongo::BinDataGeneral});
                    return keyHolder;
                }
                case mongo::KafkaKeyFormatEnum::Double: {
                    uassert(ErrorCodes::BadValue,
                            unexpectedKeyTypeError(NumberDouble),
                            keyField.getType() == NumberDouble);
                    double keyDouble = keyField.getDouble();
                    std::vector<char> keyDoubleBytes = serializeDouble(keyDouble);
                    Value keyHolder(BSONBinData{keyDoubleBytes.data(),
                                                static_cast<int>(keyDoubleBytes.size()),
                                                mongo::BinDataGeneral});
                    return keyHolder;
                }
                default: {
                    MONGO_UNREACHABLE;
                }
            }
        }
    }
    return {};
}

void KafkaEmitOperator::processStreamDoc(const StreamDocument& streamDoc) {
    auto docAsStr = tojson(streamDoc.doc.toBson(), _options.jsonStringFormat);
    auto docSize = docAsStr.size();

    constexpr int flags = RdKafka::Producer::RK_MSG_BLOCK /* block if queue is full */ |
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */;

    auto topicName = _options.topicName.isLiteral()
        ? _options.topicName.getLiteral()
        : _options.topicName.evaluate(_expCtx.get(), streamDoc.doc);
    auto topicIt = _topicCache.find(topicName);
    if (topicIt == _topicCache.cend()) {
        uassert(ErrorCodes::StreamProcessorTooManyOutputTargets,
                "Too many unique topic names: {}"_format(_topicCache.size()),
                _topicCache.size() < kMaxTopicNamesCacheSize);

        std::string errstr;
        std::unique_ptr<RdKafka::Topic> topic{RdKafka::Topic::create(_producer.get(),
                                                                     topicName,
                                                                     /*conf*/ nullptr,
                                                                     errstr)};
        uassert(8117200, "Failed to create topic with error: {}"_format(errstr), topic);
        bool inserted = false;
        std::tie(topicIt, inserted) = _topicCache.emplace(topicName, std::move(topic));
        uassert(8117201, "Failed to insert a new topic {}"_format(topicName), inserted);
    }

    const void* keyPointer = nullptr;
    size_t keyLength = 0;
    // The 'keyHolder' is the serialized Kafka key held in mongo::Value. It may either be a binData
    // or a string. It may be a stack value for short string, so it's important to assign
    // 'keyPointer' at the current function. Otherwise it may point to returned stack address.
    auto keyHolder = createKafkaKey(streamDoc);
    if (keyHolder.getType() == BinData) {
        keyPointer = keyHolder.getBinData().data;
        keyLength = keyHolder.getBinData().length;
    } else if (keyHolder.getType() == String) {
        keyPointer = keyHolder.getStringData().data();
        keyLength = keyHolder.getStringData().length();
    } else {
        invariant(keyHolder.getType() == EOO);
    }

    RdKafka::Headers* headers = createKafkaHeaders(streamDoc, topicName);

    // TODO(SERVER-80742): Validate the connection is still established.
    // This call to produce will succeed even if the actual connection to Kafka is down.
    RdKafka::ErrorCode err =
        _producer->produce(topicName,
                           _outputPartition,
                           flags,
                           const_cast<char*>(docAsStr.c_str()),
                           docSize,
                           keyPointer,
                           keyLength,
                           0 /* timestamp */,
                           headers,
                           nullptr /* Per-message opaque value passed to delivery report */);

    // If there is no error, we will need to clean up the header ourselves. Otherwise, the API above
    // has already freed up the headers for us.
    if (err != RdKafka::ERR_NO_ERROR) {
        if (headers != nullptr) {
            delete headers;
        }
        uasserted(8720704, "Failed to emit to topic {} due to error: {}"_format(topicName, err));
    }
}

void KafkaEmitOperator::doStart() {
    invariant(!_connector);
    // Create a Connector instace.
    Connector::Options options;
    if (_options.topicName.isLiteral()) {
        options.topicName = _options.topicName.getLiteral();
    }
    options.producer = _producer.get();
    options.metadataQueryTimeout = _options.metadataQueryTimeout;
    options.kafkaEventCallback = _eventCbImpl.get();
    _connector = std::make_unique<Connector>(std::move(options));
    _connector->start();
}

void KafkaEmitOperator::doStop() {
    if (_connector) {
        _connector->stop();
        _connector.reset();
    }

    doFlush();
}

ConnectionStatus KafkaEmitOperator::doGetConnectionStatus() {
    if (_connectionStatus.isConnecting()) {
        tassert(ErrorCodes::InternalError, "Expected _connector to be set.", _connector);
        _connectionStatus = _connector->getConnectionStatus();
        if (!_connectionStatus.isConnecting()) {
            // If the connector has reached kConnected or kError, get rid of it.
            _connector->stop();
            _connector.reset();
        }
    }

    if (_eventCbImpl->hasError()) {
        _connectionStatus =
            ConnectionStatus{ConnectionStatus::kError,
                             _eventCbImpl->appendRecentErrorsToStatus(
                                 {{ErrorCodes::Error{8214909}, "Kafka $emit encountered error."}})};
    }

    return _connectionStatus;
}

void KafkaEmitOperator::doFlush() {
    if (!_producer) {
        return;
    }

    LOGV2_DEBUG(74685, 0, "KafkaEmitOperator flush starting", "context"_attr = _context);
    auto err = _producer->flush(_options.flushTimeout.count());
    if (err != RdKafka::ERR_NO_ERROR) {
        spasserted(_eventCbImpl->appendRecentErrorsToStatus(
            Status{ErrorCodes::Error{74686},
                   fmt::format("$emit to Kafka encountered error while flushing, kafka error code: "
                               "{}, message: {}",
                               err,
                               RdKafka::err2str(err))}));
    }
    LOGV2_DEBUG(74687, 0, "KafkaEmitOperator flush complete", "context"_attr = _context);
}

};  // namespace streams
