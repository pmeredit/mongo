/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/sample_data_source_operator.h"
#include "mongo/db/basic_types.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/util.h"

using namespace mongo;

namespace streams {

int SampleDataSourceOperator::randomInt(int min, int max) {
    dassert(max > min);
    // nextInt32 returns a half open [0, max) interval so we add 1 to max.
    auto randomValue = _random.nextInt32(max + 1 - min);
    return randomValue + min;
}

Document SampleDataSourceOperator::generateSolarDataDoc(Date_t timestamp) {
    const int maxWatts = 250;
    std::string deviceId(fmt::format("device_{}", randomInt(0, 10)));
    int groupId = randomInt(0, 10);
    int modifier = kDefaultTimeZone.dateParts(timestamp).hour < 12 ? 2 : 1;
    // The maximum values of watts and temp are halved from 00:00-12:00 UTC time.
    int watts = randomInt(0, maxWatts / modifier);
    int temp = randomInt(5, 25 / modifier);
    // Error has .5% change, i.e. 1 out of 200 change.
    bool error = randomInt(1, 200) == 1;

    SampleDataSourceSolarSpec sampleDataSpec{
        deviceId, groupId, timestamp.toString(), maxWatts, error ? 1 : 0};
    if (error) {
        sampleDataSpec.setEvent_details(StringData{"Network error"});
    } else {
        sampleDataSpec.setObs(SampleDataSourceSolarSpecObs{watts, temp});
    }

    return Document(std::move(sampleDataSpec.toBSON()));
}

int32_t SampleDataSourceOperator::doRunOnce() {
    StreamDataMsg dataMsg;
    for (int docNum = 0; docNum < _options.docsPerRun; ++docNum) {
        Date_t ts = Date_t::now();
        auto doc = generateSolarDataDoc(ts);
        int64_t numInputBytes = doc.getCurrentApproximateSize();

        if (_options.timestampExtractor) {
            try {
                ts = _options.timestampExtractor->extractTimestamp(doc);
            } catch (const DBException& e) {
                _options.dlq->addMessage(toDeadLetterQueueMsg(std::move(doc), e.toString()));
            }
        }

        if (_options.watermarkGenerator) {
            if (_options.watermarkGenerator->isLate(ts.toMillisSinceEpoch())) {
                _options.dlq->addMessage(toDeadLetterQueueMsg(
                    std::move(doc), std::string{"Input document arrived late."}));
                continue;
            }

            _options.watermarkGenerator->onEvent(ts.toMillisSinceEpoch());
        }

        incOperatorStats(OperatorStats{.numInputDocs = int64_t(1), .numInputBytes = numInputBytes});

        MutableDocument mutableDoc{std::move(doc)};
        mutableDoc.addField(_options.timestampOutputFieldName, Value(ts));

        StreamDocument streamDoc{mutableDoc.freeze()};
        streamDoc.minEventTimestampMs = ts.toMillisSinceEpoch();
        streamDoc.minProcessingTimeMs = Date_t::now().toMillisSinceEpoch();
        streamDoc.streamMeta.setTimestamp(ts);
        streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::SampleData);

        dataMsg.docs.push_back(std::move(streamDoc));
    }

    boost::optional<StreamControlMsg> controlMsg = boost::none;
    if (_options.watermarkGenerator) {
        controlMsg = StreamControlMsg{_options.watermarkGenerator->getWatermarkMsg()};
    }

    int32_t docsSent = dataMsg.docs.size();
    sendDataMsg(0, std::move(dataMsg), std::move(controlMsg));

    return docsSent;
}

}  // namespace streams
