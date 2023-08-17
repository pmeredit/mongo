#pragma once

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/platform/random.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

class DocumentTimestampExtractor;
struct Context;

/**
 * This $source allows customers to try out the query syntax
 * without having an Atlas DB or Kafka topic to connect to.
 * It generates a stream of documents in a pre-determined schema.
 */
class SampleDataSourceOperator : public SourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        std::unique_ptr<DelayedWatermarkGenerator> watermarkGenerator;

        // The random seed used to generate data. Note that processing wallclock time is also
        // used for the data generation.
        int seed{0};
        // The number of documents to send in each call to doRunOnce().
        int docsPerRun{2};
    };

    SampleDataSourceOperator(Context* context, Options options)
        : SourceOperator(context, 1), _options(std::move(options)), _random(_options.seed) {}

private:
    std::string doGetName() const override {
        return "SampleDataSourceOperator";
    }

    int64_t doRunOnce() override;

    bool doIsConnected() override {
        return true;
    }

    int randomInt(int min, int max);
    mongo::Document generateSolarDataDoc(mongo::Date_t timestamp);

    const Options _options;
    mongo::PseudoRandom _random;
    const mongo::TimeZone kDefaultTimeZone{mongo::TimeZoneDatabase::utcZone()};
};

}  // namespace streams
