#include "mongo/base/status.h"
namespace streams {

// Error codes returned by mongostreams.
class ErrorCodes {
public:
    // These error codes should be in the range 90000-91000.
    enum Error {
        // Stream processor does not exist in mongostreams state.
        // This can happen when a stop was requested for a processor that has already been stopped.
        // In this case, the agent can most likely ignore this error and treat the stop as a
        // success.
        StreamProcessorDoesNotExist = 1'000'000,
        // Stream processor already exists in mongostreams state.
        // This can happen when a start was requested for a processor that is already running.
        // In this case, the agent can most likely ignore this error and treat the start as a
        // success.
        StreamProcessorAlreadyExists,
        // Mongostreams is shutting down.
        // This can happen when k8s has killed the pod.
        ShuttingDown,

        MaxValue = 1'001'000,
    };
};

// Returns whether or not the input `status` is a retryable error.
bool isRetryableStatus(const mongo::Status& status) {
    switch (status.code()) {
        case mongo::ErrorCodes::Error::ExceededMemoryLimit:
            return false;
        case mongo::ErrorCodes::BSONObjectTooLarge:
            // Note that operators like $merge etc handle this by DLQ-ing the offending document.
            // If a BSONObjectTooLarge exceptions escapes to the executor and fails the stream
            // processor, it should be treated as a permanent failure.
            return false;
        default:
            return true;
    }
}
}  // namespace streams
