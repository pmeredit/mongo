/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_log.h"

#include <boost/filesystem.hpp>
#include <string>

#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit_frame.h"
#include "audit_key_manager_local.h"
#include "audit_options.h"
#include "logger/console_appender.h"
#include "logger/encoder.h"
#include "logger/mock_appender.h"
#include "logger/rotatable_file_writer.h"
#include "logger/syslog_appender.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/multitenancy.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_util.h"
#include "mongo/rpc/object_check.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo::audit {

ImpersonatedClientAttrs::ImpersonatedClientAttrs(Client* client) {
    if (auto as = AuthorizationSession::get(client)) {
        auto userName = as->getImpersonatedUserName();
        auto roleNamesIt = as->getImpersonatedRoleNames();
        if (!userName) {
            userName = as->getAuthenticatedUserName();
            roleNamesIt = as->getAuthenticatedRoleNames();
        }
        if (userName) {
            this->userName = std::move(userName.value());
        }
        for (; roleNamesIt.more(); roleNamesIt.next()) {
            this->roleNames.emplace_back(roleNamesIt.get());
        }
    }
};

namespace {
namespace fs = boost::filesystem;
namespace moe = mongo::optionenvironment;

PlainAuditFrame encodeForEncrypt(const AuditInterface::AuditEvent& event) {
    auto payload = event.toBSON();
    // Timestamp field should have been excluded by AuditEvent ctor.
    auto fieldName = event.getTimestampFieldName();
    invariant(!payload[fieldName]);
    return {event.getTimestamp(), payload};
}

template <typename Encoder>
class RotatableAuditFileAppender : public logger::Appender<AuditInterface::AuditEvent> {
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
        auto& stream = useWriter.stream();
        stream.write(toWrite.data(), toWrite.size());
        if (appendNewline)
            stream.write("\n", 1);
        stream.flush();
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
    static std::string encode(const AuditInterface::AuditEvent& event) {
        BSONObj eventAsBson(event.toBSON());
        return eventAsBson.jsonString(JsonStringFormat::LegacyStrict) + '\n';
    }
};
using JSONAppender = RotatableAuditFileAppender<AuditEventJsonEncoder>;

class AuditEventBsonEncoder {
public:
    static std::string encode(const AuditInterface::AuditEvent& event) {
        BSONObj toWrite(event.toBSON());
        return std::string(toWrite.objdata(), toWrite.objsize());
    }
};
using BSONAppender = RotatableAuditFileAppender<AuditEventBsonEncoder>;

class AuditEventSyslogEncoder final : public logger::Encoder<AuditInterface::AuditEvent> {
public:
    ~AuditEventSyslogEncoder() final {}

private:
    std::ostream& encode(const AuditInterface::AuditEvent& event, std::ostream& os) override {
        BSONObj eventAsBson(event.toBSON());
        std::string toWrite = eventAsBson.jsonString(JsonStringFormat::LegacyStrict);
        return os.write(toWrite.c_str(), toWrite.length());
    }
};

class AuditEventTextEncoder final : public logger::Encoder<AuditInterface::AuditEvent> {
public:
    ~AuditEventTextEncoder() final {}

private:
    std::ostream& encode(const AuditInterface::AuditEvent& event, std::ostream& os) override {
        BSONObj eventAsBson(event.toBSON());
        std::string toWrite = eventAsBson.jsonString(JsonStringFormat::LegacyStrict) + '\n';
        return os.write(toWrite.c_str(), toWrite.length());
    }
};

std::unique_ptr<logger::Appender<AuditInterface::AuditEvent>> auditLogAppender;

}  // namespace

void AuditManager::_initializeAuditLog(const moe::Environment& params) {
    if (!isEnabled()) {
        return;
    }

    const auto format = getFormat();
    switch (format) {
        case AuditFormat::AuditFormatConsole: {
            auditLogAppender.reset(new logger::ConsoleAppender<AuditInterface::AuditEvent>(
                std::make_unique<AuditEventTextEncoder>()));
            break;
        }
#ifndef _WIN32
        case AuditFormat::AuditFormatSyslog: {
            auditLogAppender.reset(new logger::SyslogAppender<AuditInterface::AuditEvent>(
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
                        uassert(ErrorCodes::BadValue,
                                "By default, audit log encryption uses KMIP protocol version 1.2+, "
                                "but security.kmip.useLegacyProtocol is set to true, forcing the "
                                "use of the KMIP 1.0 protocol. To use the KMIP 1.0 protocol with "
                                "audit log encryption, the auditEncryptKeyWithKMIPGet setParameter "
                                "must be enabled.",
                                !params["security.kmip.useLegacyProtocol"].as<bool>());
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
        case AuditFormat::AuditFormatMock: {
            auditLogAppender.reset(new logger::MockAppender<AuditEventBsonEncoder>(
                std::make_unique<AuditEventBsonEncoder>()));
            break;
        }
        default:
            uasserted(ErrorCodes::InternalError, "Audit format misconfigured");
    }
}

BSONObj getLastLine_forTest() {
    auto sd = checked_cast<logger::MockAppender<AuditEventBsonEncoder>*>(auditLogAppender.get())
                  ->getLast();
    if (sd.empty()) {
        return {};
    }

    ConstDataRange cdr(sd.rawData(), sd.size());
    return cdr.read<Validated<BSONObj>>().val;
}

void logEvent(const AuditInterface::AuditEvent& event) {
    auto status = auditLogAppender->append(event);
    if (!status.isOK()) {
        // TODO: Write to console?
        ::abort();
    }
}

template <typename EventType>
bool tryLogEvent(typename EventType::TypeArgT tryLogParams) {
    const auto auditManager = getGlobalAuditManager();
    if (!auditManager->isEnabled()) {
        return false;
    }

    const auto& client = tryLogParams.client;

    if (!tryLogParams.tenantId && client) {
        if (auto opCtx = client->getOperationContext()) {
            tryLogParams.tenantId = getActiveTenant(opCtx);
        }
    }

    EventType event(tryLogParams);
    if (!auditManager->shouldAudit(&event)) {
        return false;
    }

    logEvent(event);

    return true;
}

template bool tryLogEvent<AuditMongo::AuditEventMongo>(TryLogEventParamsMongo params);
template bool tryLogEvent<AuditOCSF::AuditEventOCSF>(TryLogEventParamsOCSF params);

}  // namespace mongo::audit

namespace mongo {

void audit::sanitizeCredentialsAuditDoc(BSONObjBuilder* builder, const BSONObj& doc) {
    constexpr StringData kCredentials = "credentials"_sd;

    for (const auto& it : doc) {
        if (kCredentials == it.fieldName()) {
            BSONArrayBuilder cb(builder->subarrayStart(kCredentials));
            for (const auto& it2 : it.Obj()) {
                cb.append(it2.fieldName());
            }
        } else {
            builder->append(it);
        }
    }
}

bool audit::isStandaloneOrPrimary(OperationContext* opCtx) {
    auto replCoord = repl::ReplicationCoordinator::get(opCtx);
    return !replCoord->getSettings().isReplSet() || replCoord->getMemberState().primary();
}

}  // namespace mongo
