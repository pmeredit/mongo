/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/base/string_data.h"

#include "audit_log.h"

#include <boost/filesystem.hpp>
#include <string>

#include "audit_frame.h"
#include "audit_key_manager_local.h"
#include "audit_manager.h"
#include "audit_options.h"
#include "logger/console_appender.h"
#include "logger/encoder.h"
#include "logger/rotatable_file_writer.h"
#include "logger/syslog_appender.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_util.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo::audit {
namespace {
namespace fs = boost::filesystem;
namespace moe = mongo::optionenvironment;

PlainAuditFrame encodeForEncrypt(const AuditEvent& event) {
    auto payload = event.toBSON();
    // "ts" field should have been excluded by AuditEvent ctor.
    invariant(!payload["ts"_sd]);
    return {event.getTimestamp(), payload};
}

template <typename Encoder>
class RotatableAuditFileAppender : public logger::Appender<AuditEvent> {
public:
    RotatableAuditFileAppender(std::unique_ptr<logger::RotatableFileWriter> writer,
                               std::unique_ptr<logger::RotatableFileWriter> headerWriter)
        : _writer(std::move(writer)), _headerWriter(std::move(headerWriter)) {}

    Status rotate(bool renameFiles,
                  StringData suffix,
                  std::function<void(Status)> onMinorError) final {

        bool auditLogExists = fs::exists(getGlobalAuditManager()->getPath());

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


        auto* am = getGlobalAuditManager();
        invariant(am->isEnabled());

        if (am->getEncryptionEnabled()) {
            auto ac = am->getAuditEncryptionCompressionManager();
            invariant(ac);

            // Make sure we lock before encodeFileHeader
            logger::RotatableFileWriter::Use useWriter(_writer.get());
            BSONObj encodedHeader = ac->encodeFileHeader();
            std::string toWrite;
            bool isJson = am->getFormat() == AuditFormat::AuditFormatJsonFile;
            if (isJson) {
                toWrite = encodedHeader.jsonString();
            } else {
                toWrite = std::string(encodedHeader.objdata(), encodedHeader.objsize());
            }

            if (_headerWriter != nullptr) {
                // Duplicate the header line
                logger::RotatableFileWriter::Use useHeaderWriter(_headerWriter.get());
                result = writeStream(useHeaderWriter, toWrite, isJson /* appendNewline */);

                if (!result.isOK()) {
                    try {
                        LOGV2_WARNING(5991700,
                                      "Failure writing header to audit header log",
                                      "error"_attr = result);
                    } catch (...) {
                        // If neither audit subsystem can write,
                        // then just eat the standard logging exception,
                        // and return audit's bad status.
                        return result;
                    }
                }
            }

            result = writeStream(useWriter, toWrite, isJson /* appendNewline */);

            if (!result.isOK()) {
                try {
                    LOGV2_WARNING(
                        242451, "Failure writing header to audit log", "error"_attr = result);
                } catch (...) {
                    // If neither audit subsystem can write,
                    // then just eat the standard logging exception,
                    // and return audit's bad status.
                    return result;
                }
            }
        }

        if (auditLogExists) {
            logRotateLog(Client::getCurrent(), result, errors, suffix.toString());
        }
        return result;
    }

    /**
     * Writes to file stream, returns Status::OK() on success.
     */
    Status writeStream(logger::RotatableFileWriter::Use& useWriter,
                       StringData toWrite,
                       const bool appendNewline = false) try {
        auto status = useWriter.status();
        if (!status.isOK()) {
            LOGV2_WARNING(24243, "Failure acquiring audit logger", "error"_attr = status);
            return status;
        }

        constexpr auto kNewline = "\n"_sd;
        useWriter.stream()
            .write(toWrite.begin(), toWrite.size())
            .write(kNewline.rawData(), appendNewline ? 1 : 0)
            .flush();
        return useWriter.status();
    } catch (...) {
        return exceptionToStatus();
    }

    Status append(const Event& event) final {
        auto* am = getGlobalAuditManager();
        invariant(am->isEnabled());

        Status status = Status::OK();
        try {
            if (am->getEncryptionEnabled()) {
                PlainAuditFrame toWrite = encodeForEncrypt(event);

                auto ac = am->getAuditEncryptionCompressionManager();
                invariant(ac);

                std::vector<uint8_t> compressed;
                ConstDataRange toEncrypt(toWrite.payload.objdata(), toWrite.payload.objsize());
                if (am->getCompressionEnabled()) {
                    compressed = ac->compress(toEncrypt);
                    toEncrypt = ConstDataRange(compressed);
                }

                logger::RotatableFileWriter::Use useWriter(_writer.get());
                BSONObj obj = ac->encryptAndEncode(toEncrypt, toWrite.ts);

                if (am->getFormat() == AuditFormat::AuditFormatJsonFile) {
                    status = writeStream(useWriter, obj.jsonString(), true);
                } else {
                    status = writeStream(useWriter, StringData(obj.objdata(), obj.objsize()));
                }
            } else {
                std::string toWrite = Encoder::encode(event);
                logger::RotatableFileWriter::Use useWriter(_writer.get());
                status = writeStream(useWriter, toWrite);
            }
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
    std::unique_ptr<logger::RotatableFileWriter> _writer, _headerWriter;
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

void AuditManager::_initializeAuditLog(const moe::Environment& params) {
    if (!isEnabled()) {
        return;
    }

    const auto format = getFormat();
    switch (format) {
        case AuditFormat::AuditFormatConsole: {
            auditLogAppender.reset(
                new logger::ConsoleAppender<AuditEvent>(std::make_unique<AuditEventTextEncoder>()));
            break;
        }
#ifndef _WIN32
        case AuditFormat::AuditFormatSyslog: {
            auditLogAppender.reset(new logger::SyslogAppender<AuditEvent>(
                std::make_unique<AuditEventSyslogEncoder>()));
            break;
        }
#endif  // ndef _WIN32
        case AuditFormat::AuditFormatJsonFile:
        case AuditFormat::AuditFormatBsonFile: {
            const auto& auditLogPath = getPath();
            bool auditLogExists = false;

            try {
                auditLogExists = fs::exists(auditLogPath);

                const auto auditDirectoryPath = fs::path(auditLogPath).parent_path();
                if (!fs::exists(auditDirectoryPath)) {
                    fs::create_directory(auditDirectoryPath);
                }
            } catch (const std::exception& e) {
                auto status = Status(ErrorCodes::BadValue, "Unable to initialize audit path")
                                  .withContext(e.what());
                uassertStatusOK(std::move(status));
            }

            if (getEncryptionEnabled()) {
                // Store info on how to create the manager
                if (params.count("auditLog.localAuditKeyFile")) {
                    _managerType = ManagerType::kLocal;
                    _localAuditKeyFile = params["auditLog.localAuditKeyFile"].as<std::string>();
                } else if (params.count("auditLog.auditEncryptionKeyIdentifier")) {
                    if (gAuditEncryptKeyWithKMIPGet) {
                        _managerType = ManagerType::kKMIPGet;
                    } else {
                        _managerType = ManagerType::kKMIPEncrypt;
                    }
                    _auditEncryptionKeyUID =
                        params["auditLog.auditEncryptionKeyIdentifier"].as<std::string>();
                } else {
                    uasserted(ErrorCodes::BadValue,
                              "auditLog.localAuditKeyFile or auditLog.auditEncryptionKeyIdentifier "
                              "must be specified if "
                              "audit log encryption is enabled");
                }

                // fail if GCM is not supported
                std::string mode = crypto::getStringFromCipherMode(crypto::aesMode::gcm);
                uassert(ErrorCodes::InvalidOptions,
                        str::stream() << "Audit log encryption cannot be enabled as the server is "
                                         "not compiled with "
                                      << mode << " support",
                        crypto::getSupportedSymmetricAlgorithms().count(mode));
            }

            auto writer = std::make_unique<logger::RotatableFileWriter>();

            // Set up the log writer. If the file does not yet exist, delay opening
            // the stream (hence delaying file creation) until the first log rotate
            // occurs. This is to avoid having to create an empty file simply to be
            // rotated later, leaving one more unnecessary file.
            uassertStatusOK(logger::RotatableFileWriter::Use(writer.get())
                                .setFileName(auditLogPath, true /* append */, auditLogExists));

            std::unique_ptr<logger::RotatableFileWriter> headerWriter = nullptr;
            const auto& headerPath = getHeaderMetadataPath();
            if (!headerPath.empty()) {
                headerWriter = std::make_unique<logger::RotatableFileWriter>();
                uassertStatusOK(logger::RotatableFileWriter::Use(headerWriter.get())
                                    .setFileName(headerPath, true /* append */));
            }

            if (format == AuditFormat::AuditFormatJsonFile) {
                auditLogAppender.reset(
                    new JSONAppender(std::move(writer), std::move(headerWriter)));
            } else {
                invariant(format == AuditFormat::AuditFormatBsonFile);
                auditLogAppender.reset(
                    new BSONAppender(std::move(writer), std::move(headerWriter)));
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
