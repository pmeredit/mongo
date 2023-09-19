#pragma once

namespace streams {

constexpr const char kDefaultTsFieldName[] = "_ts";
constexpr const char kDefaultTimestampOutputFieldName[] = "_ts";
constexpr const char kFromFieldName[] = "from";
constexpr const char kStreamsMetaField[] = "_stream_meta";
constexpr const char kTenantIdLabelKey[] = "tenant_id";
constexpr const char kProcessorIdLabelKey[] = "processor_id";

constexpr const char kSourceStageName[] = "$source";
constexpr const char kEmitStageName[] = "$emit";
constexpr const char kMergeStageName[] = "$merge";
constexpr char kTumblingWindowStageName[] = "$tumblingWindow";
constexpr char kHoppingWindowStageName[] = "$hoppingWindow";
constexpr const char kLookUpStageName[] = "$lookup";

};  // namespace streams
