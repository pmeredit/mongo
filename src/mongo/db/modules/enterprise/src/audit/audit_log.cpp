/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_log.h"

#include <boost/filesystem.hpp>

#include "audit_manager_global.h"
#include "audit_options.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/server_options.h"
#include "mongo/logger/console_appender.h"
#include "mongo/logger/encoder.h"
#include "mongo/logger/log_domain-impl.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/logger/rotatable_file_appender.h"
#include "mongo/logger/rotatable_file_manager.h"
#include "mongo/logger/rotatable_file_writer.h"
#include "mongo/logger/syslog_appender.h"
#include "mongo/logv2/log.h"

namespace mongo::audit {

namespace {

auto& getLogDomain() {
    static auto& p = *new logger::LogDomain<AuditEvent>();
    return p;
}

class AuditEventSyslogEncoder : public logger::Encoder<AuditEvent> {
public:
    ~AuditEventSyslogEncoder() final {}

private:
    virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
        BSONObj eventAsBson(event.toBSON());
        std::string toWrite = eventAsBson.jsonString(JsonStringFormat::LegacyStrict);
        return os.write(toWrite.c_str(), toWrite.length());
    }
};

class AuditEventTextEncoder : public logger::Encoder<AuditEvent> {
public:
    ~AuditEventTextEncoder() final {}

private:
    virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
        BSONObj eventAsBson(event.toBSON());
        std::string toWrite = eventAsBson.jsonString(JsonStringFormat::LegacyStrict) + '\n';
        return os.write(toWrite.c_str(), toWrite.length());
    }
};


class AuditEventJsonEncoder {
public:
    static std::string encode(const AuditEvent& event) {
        BSONObj eventAsBson(event.toBSON());
        return eventAsBson.jsonString(JsonStringFormat::LegacyStrict) + '\n';
    }
};

class AuditEventBsonEncoder {
public:
    static std::string encode(const AuditEvent& event) {
        BSONObj toWrite(event.toBSON());
        return std::string(toWrite.objdata(), toWrite.objsize());
    }
};

/**
 * Appender for writing to instances of RotatableFileWriter for audit events.
 */
template <typename Encoder>
class RotatableAuditFileAppender : public logger::Appender<AuditEvent> {
    RotatableAuditFileAppender(const RotatableAuditFileAppender&) = delete;
    RotatableAuditFileAppender& operator=(const RotatableAuditFileAppender&) = delete;

public:
    RotatableAuditFileAppender(logger::RotatableFileWriter* writer) : _writer(writer) {}

    ~RotatableAuditFileAppender() final {}

    Status append(const Event& event) final {
        Status status = Status::OK();
        try {
            std::string toWrite = Encoder::encode(event);
            logger::RotatableFileWriter::Use useWriter(_writer.get());

            status = useWriter.status();
            if (!status.isOK()) {
                LOGV2_WARNING(
                    24243, "Failure acquiring audit logger: {status}", "status"_attr = status);
                return status;
            }

            useWriter.stream().write(toWrite.c_str(), toWrite.length()).flush();

            status = useWriter.status();
        } catch (...) {
            status = exceptionToStatus();
        }

        if (!status.isOK()) {
            try {
                LOGV2_WARNING(
                    24244, "Failure writing to audit log: {status}", "status"_attr = status);
            } catch (...) {
                // If neither audit subsystem can write,
                // then just eat the standard logging exception,
                // and return audit's bad status.
            }
        }

        return status;
    }

private:
    std::unique_ptr<logger::RotatableFileWriter> _writer;
};


MONGO_INITIALIZER_WITH_PREREQUISITES(AuditDomain, ("InitializeGlobalAuditManager"))
(InitializerContext*) {
    if (!auditGlobalParams.enabled) {
        return Status::OK();
    }

    auto& domain = getLogDomain();

    // On any audit log failure, abort.
    domain.setAbortOnFailure(true);

    switch (getGlobalAuditManager()->auditFormat) {
        case AuditFormatConsole: {
            domain.attachAppender(
                std::make_unique<logger::ConsoleAppender<AuditEvent>>(
                    std::make_unique<AuditEventTextEncoder>()));
            break;
        }
#ifndef _WIN32
        case AuditFormatSyslog: {
            domain.attachAppender(
                std::make_unique<logger::SyslogAppender<AuditEvent>>(
                    std::make_unique<AuditEventSyslogEncoder>()));
            break;
        }
#endif  // ndef _WIN32
        case AuditFormatJsonFile:
        case AuditFormatBsonFile: {
            std::string auditLogPath =
                boost::filesystem::absolute(getGlobalAuditManager()->auditLogPath,
                                            serverGlobalParams.cwd)
                    .string();
            logger::StatusWithRotatableFileWriter statusOrWriter =
                logger::globalRotatableFileManager()->openFile(auditLogPath, true);
            logger::RotatableFileWriter* writer;
            if (statusOrWriter.isOK()) {
                writer = statusOrWriter.getValue();
            } else if (statusOrWriter.getStatus() == ErrorCodes::FileAlreadyOpen) {
                writer = logger::globalRotatableFileManager()->getFile(auditLogPath);
                if (!writer) {
                    return Status(ErrorCodes::InternalError,
                                  auditLogPath +
                                      " opened successfully, but "
                                      "globalRotatableFileManager()->getFile() returned NULL.");
                }
            } else {
                return statusOrWriter.getStatus();
            }

            if (getGlobalAuditManager()->auditFormat == AuditFormatJsonFile) {
                domain.attachAppender(
                    std::make_unique<RotatableAuditFileAppender<AuditEventJsonEncoder>>(writer));
            } else if (getGlobalAuditManager()->auditFormat == AuditFormatBsonFile) {
                domain.attachAppender(
                    std::make_unique<RotatableAuditFileAppender<AuditEventBsonEncoder>>(writer));
            } else {
                return Status(ErrorCodes::InternalError, "invalid format");
            }

            break;
        }
        default:
            return Status(ErrorCodes::InternalError, "Audit format misconfigured");
    }
    return Status::OK();
}

}  // namespace

void logEvent(const AuditEvent& event) {
    uassertStatusOK(getLogDomain().append(event));
}

}  // namespace mongo::audit
