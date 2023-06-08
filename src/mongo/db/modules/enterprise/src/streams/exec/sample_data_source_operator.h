#pragma once

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/platform/random.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

class DocumentTimestampExtractor;
class DeadLetterQueue;

/**
 * This $source allows customers to try out the query syntax
 * without having an Atlas DB or Kafka topic to connect to.
 * It generates a stream of documents in a pre-determined schema.
 */
class SampleDataSourceOperator : public SourceOperator {
public:
    struct Options : public SourceOperator::Options {
        std::unique_ptr<DelayedWatermarkGenerator> watermarkGenerator;

        // The random seed used to generate data. Note that processing wallclock time is also
        // used for the data generation.
        int seed{0};
        // The number of documents to send in each call to doRunOnce().
        int docsPerRun{2};
    };

    SampleDataSourceOperator(Options options)
        : SourceOperator(0, 1), _options(std::move(options)), _random(_options.seed) {}

private:
    int randomInt(int min, int max);
    mongo::Document generateSolarDataDoc(mongo::Date_t timestamp);
    int32_t doRunOnce() override;

    std::string doGetName() const override {
        return "SampleDataSourceOperator";
    }

    const Options _options;
    mongo::PseudoRandom _random;
    const mongo::TimeZone kDefaultTimeZone{mongo::TimeZoneDatabase::utcZone()};
};

}  // namespace streams
