/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <boost/exception/diagnostic_information.hpp>
#include <boost/exception/exception.hpp>

#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
#include "streams/util/exception.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

SPStatus exceptionToSPStatus() noexcept {
    try {
        throw;
    } catch (const SPException& ex) {
        return ex.toStatus();
    } catch (const DBException& ex) {
        return ex.toStatus();
    } catch (const std::exception& ex) {
        return Status(ErrorCodes::InternalError,
                      str::stream() << "Caught std::exception of type " << demangleName(typeid(ex))
                                    << ": " << ex.what());
    } catch (const boost::exception& ex) {
        return Status(ErrorCodes::InternalError,
                      str::stream()
                          << "Caught boost::exception of type " << demangleName(typeid(ex)) << ": "
                          << boost::diagnostic_information(ex));

    } catch (...) {
        LOGV2_FATAL_CONTINUE(75385, "Caught unknown exception in exceptionToStatus()");
        std::terminate();
    }
}

}  // namespace streams
