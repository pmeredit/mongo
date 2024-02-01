#pragma once

namespace streams {

constexpr const char kIdFieldName[] = "_id";
constexpr const char kDefaultTsFieldName[] = "_ts";
constexpr const char kDefaultTimestampOutputFieldName[] = "_ts";
constexpr const char kFromFieldName[] = "from";
constexpr const char kTenantIdLabelKey[] = "tenant_id";
constexpr const char kProcessorIdLabelKey[] = "processor_id";
constexpr const char kStatusLabelKey[] = "status";

constexpr const char kSourceStageName[] = "$source";
constexpr const char kEmitStageName[] = "$emit";
constexpr const char kMergeStageName[] = "$merge";
constexpr char kTumblingWindowStageName[] = "$tumblingWindow";
constexpr char kHoppingWindowStageName[] = "$hoppingWindow";
constexpr const char kLookUpStageName[] = "$lookup";
constexpr const char kGroupStageName[] = "$group";
constexpr const char kSortStageName[] = "$sort";
constexpr const char kLimitStageName[] = "$limit";

constexpr char kNoDbDbName[] = "$nodb$";
constexpr char kNoCollCollectionName[] = "$nocoll$";
constexpr char kNoDbCollNamespaceString[] = "$nodb$.$nocoll$";

};  // namespace streams
