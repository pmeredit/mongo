/*
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>

#include "kmip_service.h"
#include "mongo/db/service_context.h"
#include "mongo/util/periodic_runner.h"

namespace mongo {

namespace kmip {

/**
 * This class creates a periodic job that checks the state of the ESE Key Encryption
 * Key in the KMIP Server. It shuts down the server if the key is not in the active
 * state and logs a warning for any other error.
 */
class KMIPIsActivePollingJob {
public:
    Status createJob(KMIPService& kmipService,
                     std::string keyId,
                     boost::optional<Seconds> periodSeconds);

    void startJob() {
        _anchor.start();
    }

    static KMIPIsActivePollingJob* get(ServiceContext* service);

private:
    PeriodicJobAnchor _anchor;
};

}  // namespace kmip
}  // namespace mongo
