/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_log.h"

#include <boost/filesystem.hpp>

#include "audit_manager_global.h"
#include "audit_options.h"
#include "logger/console_appender.h"
#include "logger/encoder.h"
#include "logger/rotatable_file_writer.h"
#include "logger/syslog_appender.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_util.h"

namespace mongo::audit {

namespace {

std::string getAuditLogPath() {
    return boost::filesystem::absolute(getGlobalAuditManager()->auditLogPath,
                                       serverGlobalParams.cwd)
        .string();
}

template <typename Encoder>
class RotatableAuditFileAppender : public logger::Appender<AuditEvent> {
public:
    RotatableAuditFileAppender(std::unique_ptr<logger::RotatableFileWriter> writer)
        : _writer(std::move(writer)) {}

    Status rotate(bool renameFiles, StringData suffix) final {
        auto target = getAuditLogPath() + suffix.toString();
        return logger::RotatableFileWriter::Use(_writer.get()).rotate(renameFiles, target);
    }

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

protected:
    std::unique_ptr<logger::RotatableFileWriter> _writer;
};

class AuditEventJsonEncoder {
public:
    static std::string encode(const AuditEvent& event) {
        BSONObj eventAsBson(event.toBSON());
        return eventAsBson.jsonString(JsonStringFormat::LegacyStrict) + '\n';
    }
};
using JSONAppender = RotatableAuditFileAppender<AuditEventJsonEncoder>;

class AuditEventBsonEncoder {
public:
    static std::string encode(const AuditEvent& event) {
        BSONObj toWrite(event.toBSON());
        return std::string(toWrite.objdata(), toWrite.objsize());
    }
};
using BSONAppender = RotatableAuditFileAppender<AuditEventBsonEncoder>;

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

std::unique_ptr<logger::Appender<AuditEvent>> auditLogAppender;

MONGO_INITIALIZER_WITH_PREREQUISITES(AuditDomain, ("InitializeGlobalAuditManager"))
(InitializerContext*) {
    if (!auditGlobalParams.enabled) {
        return;
    }

    const auto format = getGlobalAuditManager()->auditFormat;
    switch (format) {
        case AuditFormatConsole: {
            auditLogAppender.reset(
                new logger::ConsoleAppender<AuditEvent>(std::make_unique<AuditEventTextEncoder>()));
            break;
        }
#ifndef _WIN32
        case AuditFormatSyslog: {
            auditLogAppender.reset(new logger::SyslogAppender<AuditEvent>(
                std::make_unique<AuditEventSyslogEncoder>()));
            break;
        }
#endif  // ndef _WIN32
        case AuditFormatJsonFile:
        case AuditFormatBsonFile: {
            auto auditLogPath = getAuditLogPath();
            auto writer = std::make_unique<logger::RotatableFileWriter>();
            auto status = logger::RotatableFileWriter::Use(writer.get())
                              .setFileName(auditLogPath, true /* append */);
            uassertStatusOK(status);

            if (format == AuditFormatJsonFile) {
                auditLogAppender.reset(new JSONAppender(std::move(writer)));
            } else {
                invariant(format == AuditFormatBsonFile);
                auditLogAppender.reset(new BSONAppender(std::move(writer)));
            }

            logv2::addLogRotator(logv2::kAuditLogTag, [](bool renameFiles, StringData suffix) {
                return auditLogAppender->rotate(renameFiles, suffix);
            });

            break;
        }
        default:
            uasserted(ErrorCodes::InternalError, "Audit format misconfigured");
    }
}

}  // namespace

void logEvent(const AuditEvent& event) {
    auto status = auditLogAppender->append(event);
    if (!status.isOK()) {
        // TODO: Write to console?
        ::abort();
    }
}

}  // namespace mongo::audit
