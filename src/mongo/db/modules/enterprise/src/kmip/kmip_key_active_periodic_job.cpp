/*
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "kmip_key_active_periodic_job.h"

#include "kmip_consts.h"
#include "kmip_response.h"
#include "mongo/base/status.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage


namespace mongo {

namespace kmip {

constexpr auto kStateAttribute = "State"_sd;
constexpr Seconds kDefaultPeriodSecs{900};

namespace {
const auto getKMIPIsActivePollingJob =
    ServiceContext::declareDecoration<std::unique_ptr<KMIPIsActivePollingJob>>();
}  // namespace

KMIPIsActivePollingJob* KMIPIsActivePollingJob::get(ServiceContext* service) {
    if (!getKMIPIsActivePollingJob(service)) {
        getKMIPIsActivePollingJob(service) = std::make_unique<KMIPIsActivePollingJob>();
    }

    return getKMIPIsActivePollingJob(service).get();
}

namespace {
const std::map<uint32_t, std::string> keyStateToString{{0x00, "stateless"},
                                                       {0x01, "preActive"},
                                                       {0x02, "active"},
                                                       {0x03, "deactivated"},
                                                       {0x04, "compromised"},
                                                       {0x05, "destroyed"},
                                                       {0x06, "destroyedCompromised"}};

std::string kmipKeyStateToString(const StateName& name) {
    const uint32_t num = static_cast<uint32_t>(name);
    return keyStateToString.find(num)->second;
}

void run(std::string keyId) try {
    LOGV2_DEBUG(4250505, 1, "Periodic Job for checking if KMIP Key is Active is starting");

    auto swKmipService = mongo::kmip::KMIPService::createKMIPService();
    if (!swKmipService.isOK()) {
        LOGV2_WARNING(4250502,
                      "Failed to create KMIP Service to connect to KMIP Server",
                      "key_uid"_attr = keyId,
                      "error"_attr = swKmipService.getStatus());
        return;
    }

    StatusWith<KMIPResponse::Attribute> swStateAttribute =
        swKmipService.getValue().getAttributes(keyId, kStateAttribute.toString());

    if (!swStateAttribute.isOK()) {
        LOGV2_WARNING(4250503,
                      "Failed to get attribute for key from KMIP Server",
                      "key_uid"_attr = keyId,
                      "error"_attr = swStateAttribute.getStatus());
        return;
    }

    const auto& attribute = swStateAttribute.getValue();

    if (attribute.value.stateEnum != StateName::active) {
        LOGV2_FATAL_NOTRACE(4250501,
                            "KMIP Key used for ESE is not in active state. Shutting down server.",
                            "key_uid"_attr = keyId,
                            "key_state"_attr = attribute.name);
    }

    LOGV2_DEBUG(4250504, 1, "Periodic Job for checking if KMIP Key is Active has finished");
} catch (const DBException& e) {
    LOGV2_WARNING(4250500,
                  "Failed to get attribute for key from KMIP Server",
                  "key_uid"_attr = keyId,
                  "error"_attr = e);
}
}  // namespace

Status KMIPIsActivePollingJob::createJob(KMIPService& kmipService,
                                         std::string keyId,
                                         boost::optional<Seconds> periodSeconds) {

    StatusWith<KMIPResponse::Attribute> swStateAttribute =
        kmipService.getAttributes(keyId, kStateAttribute.toString());

    if (!swStateAttribute.isOK()) {
        return swStateAttribute.getStatus();
    }

    const auto& attribute = swStateAttribute.getValue();

    if (attribute.value.stateEnum != StateName::active) {
        return Status(ErrorCodes::BadValue,
                      str::stream()
                          << "State of KMIP Key for ESE is not active on startup. UID: (" << keyId
                          << "). State: " << kmipKeyStateToString(attribute.value.stateEnum));
    }

    auto periodicRunner = getGlobalServiceContext()->getPeriodicRunner();

    // This job is killable. If interrupted, we will warn, and retry after the configured interval.
    PeriodicRunner::PeriodicJob job(
        "KMIPKeyIsActiveCheck",
        [keyId](Client* client) { run(keyId); },
        Milliseconds(periodSeconds ? periodSeconds.value() : kDefaultPeriodSecs),
        true /*isKillableByStepdown*/);

    if (_anchor.isValid()) {
        _anchor.stop();
        _anchor.detach();
    }
    _anchor = periodicRunner->makeJob(std::move(job));

    return Status::OK();
}

}  // namespace kmip

}  // namespace mongo
