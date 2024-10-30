/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/sample_data_source_operator.h"
#include "mongo/db/basic_types.h"
#include "streams/exec/context.h"

using namespace mongo;

namespace streams {

int SampleDataSourceOperator::randomInt(int min, int max) {
    dassert(max > min);
    // nextInt32 returns a half open [0, max) interval so we add 1 to max.
    auto randomValue = _random.nextInt32(max + 1 - min);
    return randomValue + min;
}

Document SampleDataSourceOperator::generateSolarDataDoc(Date_t timestamp) {
    const int maxWatts = 450;
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
        sampleDataSpec.setEvent_details("Network error"_sd);
    } else {
        sampleDataSpec.setObs(SampleDataSourceSolarSpecObs{watts, temp});
    }

    return Document(std::move(sampleDataSpec.toBSON()));
}

std::vector<StreamMsgUnion> SampleDataSourceOperator::getMessages(WithLock) {
    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(_options.docsPerRun);

    for (int docNum = 0; docNum < _options.docsPerRun; ++docNum) {
        Date_t ts = Date_t::now();
        auto doc = generateSolarDataDoc(ts);
        dataMsg.docs.emplace_back(doc);
    }

    return {StreamMsgUnion{.dataMsg = std::move(dataMsg)}};
}

}  // namespace streams
