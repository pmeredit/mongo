/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_log.h"

#include <boost/filesystem.hpp>

#include "audit/audit_feature_flag_gen.h"
#include "audit_manager.h"
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
#include "mongo/util/time_support.h"

namespace mongo::audit {

namespace {
namespace fs = boost::filesystem;

template <typename Encoder>
class RotatableAuditFileAppender : public logger::Appender<AuditEvent> {
public:
    RotatableAuditFileAppender(std::unique_ptr<logger::RotatableFileWriter> writer)
        : _writer(std::move(writer)) {}

    Status rotate(bool renameFiles,
                  StringData suffix,
                  std::function<void(Status)> onMinorError) final {
        auto target = getGlobalAuditManager()->getPath() + suffix.toString();
        std::vector<Status> errors;
        Status result = logger::RotatableFileWriter::Use(_writer.get())
                            .rotate(renameFiles, target, true /* append */, [&](Status s) {
                                errors.push_back(s);
                                if (onMinorError)
                                    onMinorError(s);
                            });
        if (!errors.empty())
            LOGV2_WARNING(
                4719803, "Errors occurred during audit log rotate", "errors"_attr = errors);
        return result;
    }

    Status append(const Event& event) final {
        auto* am = getGlobalAuditManager();
        invariant(am->isEnabled());

        Status status = Status::OK();
        try {
            std::string toWrite = Encoder::encode(event);

            if (am->getCompressionEnabled() &&
                feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
                auto ac = am->getAuditEncryptionCompressionManager();
                invariant(ac);

                auto inBuff = ConstDataRange(toWrite.data(), toWrite.length());
                toWrite = ac->compressAndEncrypt(inBuff);
            }

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

class AuditEventSyslogEncoder final : public logger::Encoder<AuditEvent> {
public:
    ~AuditEventSyslogEncoder() final {}

private:
    virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
        BSONObj eventAsBson(event.toBSON());
        std::string toWrite = eventAsBson.jsonString(JsonStringFormat::LegacyStrict);
        return os.write(toWrite.c_str(), toWrite.length());
    }
};

class AuditEventTextEncoder final : public logger::Encoder<AuditEvent> {
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

}  // namespace

void AuditManager::_initializeAuditLog() {
    if (!isEnabled()) {
        return;
    }

    const auto format = getFormat();
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
            auto auditLogPath = getPath();

            try {
                const auto auditDirectoryPath = fs::path(auditLogPath).parent_path();
                if (!fs::exists(auditDirectoryPath)) {
                    fs::create_directory(auditDirectoryPath);
                }
            } catch (const std::exception& e) {
                auto status = Status(ErrorCodes::BadValue, "Unable to initialize audit path")
                                  .withContext(e.what());
                uassertStatusOK(std::move(status));
            }

            if (getCompressionEnabled() &&
                feature_flags::gFeatureFlagAtRestEncryption.isEnabledAndIgnoreFCV()) {
                _ac = std::make_unique<AuditEncryptionCompressionManager>();
            }

            auto writer = std::make_unique<logger::RotatableFileWriter>();
            uassertStatusOK(logger::RotatableFileWriter::Use(writer.get())
                                .setFileName(auditLogPath, true /* append */));

            uassertStatusOK(logger::RotatableFileWriter::Use(writer.get())
                                .rotate(serverGlobalParams.logRenameOnRotate,
                                        auditLogPath + terseCurrentTimeForFilename(),
                                        true /* append */,
                                        nullptr));

            if (format == AuditFormatJsonFile) {
                auditLogAppender.reset(new JSONAppender(std::move(writer)));
            } else {
                invariant(format == AuditFormatBsonFile);
                auditLogAppender.reset(new BSONAppender(std::move(writer)));
            }

            logv2::addLogRotator(
                logv2::kAuditLogTag,
                [](bool renameFiles, StringData suffix, std::function<void(Status)> onMinorError) {
                    return auditLogAppender->rotate(renameFiles, suffix, onMinorError);
                });

            break;
        }
        default:
            uasserted(ErrorCodes::InternalError, "Audit format misconfigured");
    }
}

void logEvent(const AuditEvent& event) {
    auto status = auditLogAppender->append(event);
    if (!status.isOK()) {
        // TODO: Write to console?
        ::abort();
    }
}

bool tryLogEvent(Client* client,
                 AuditEventType type,
                 AuditEvent::Serializer serializer,
                 ErrorCodes::Error code) {
    const auto auditManager = getGlobalAuditManager();
    if (!auditManager->isEnabled()) {
        return false;
    }

    const auto event = AuditEvent(client, type, serializer, code);
    if (!auditManager->shouldAudit(&event)) {
        return false;
    }

    logEvent(event);

    return true;
}

}  // namespace mongo::audit
