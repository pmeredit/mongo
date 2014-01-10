/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_log_domain.h"

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

namespace mongo {

namespace logger {
    template class LogDomain<audit::AuditEvent>;
}  // namespace logger

namespace audit {

    static AuditLogDomain globalAuditLogDomain;

    AuditLogDomain* getGlobalAuditLogDomain() { return &globalAuditLogDomain; }

namespace {

    std::ostream& encodeTextBody(const AuditEvent& event, std::ostream& os) {
        // Use impersonated user list if it exists; else use the authenticated users list.
        UserNameIterator users = event.getImpersonatedUsers();
        if (!users.more()) {
            users = event.getAuthenticatedUsers();
        }
        if (users.more()) {
            os << users.next().getFullName();
            while (users.more()) {
                os << ',' << users.next().getFullName();
            }
            os << ' ';
        }

        os << event.getRemoteAddr().toString() << '/' << event.getLocalAddr().toString() << ' ';
        return event.putText(os) << '\n';
    }

    class AuditEventTextEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventTextEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            BSONObj eventAsBson(event.toBSON());
            std::string toWrite = eventAsBson.jsonString() + '\n';
            return os.write(toWrite.c_str(), toWrite.length());
        }
    };

    class AuditEventSyslogEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventSyslogEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            BSONObj eventAsBson(event.toBSON());
            std::string toWrite = eventAsBson.jsonString();
            return os.write(toWrite.c_str(), toWrite.length());
        }
    };

    class AuditEventBsonEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventBsonEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            BSONObj toWrite(event.toBSON());
            return os.write(toWrite.objdata(), toWrite.objsize());
        }
    };

    MONGO_INITIALIZER_WITH_PREREQUISITES(AuditDomain,
            ("InitializeGlobalAuditManager"))(InitializerContext*) {
        
        if (!auditGlobalParams.enabled) {
            return Status::OK();
        }

        // On any audit log failure, abort.
        getGlobalAuditLogDomain()->setAbortOnFailure(true);

        switch (getGlobalAuditManager()->auditFormat) {
        case AuditFormatConsole:
        {
            getGlobalAuditLogDomain()->attachAppender(
                AuditLogDomain::AppenderAutoPtr(
                    new logger::ConsoleAppender<AuditEvent>(new AuditEventTextEncoder)));
            break;
        }
#ifndef _WIN32
        case AuditFormatSyslog:
        {
            getGlobalAuditLogDomain()->attachAppender(
                AuditLogDomain::AppenderAutoPtr(
                    new logger::SyslogAppender<AuditEvent>(new AuditEventSyslogEncoder)));
            break;
        }
#endif // ndef _WIN32
        case AuditFormatJsonFile:
        case AuditFormatBsonFile:
        {
            std::string auditLogPath = boost::filesystem::absolute(
                    getGlobalAuditManager()->auditLogPath,
                    serverGlobalParams.cwd).string();
            logger::StatusWithRotatableFileWriter statusOrWriter =
                logger::globalRotatableFileManager()->openFile(auditLogPath, true);
            logger::RotatableFileWriter* writer;
            if (statusOrWriter.isOK()) {
                writer = statusOrWriter.getValue();
            }
            else if (statusOrWriter.getStatus() == ErrorCodes::FileAlreadyOpen) {
                writer = logger::globalRotatableFileManager()->getFile(auditLogPath);
                if (!writer) {
                    return Status(ErrorCodes::InternalError, auditLogPath +
                                  " opened successfully, but "
                                  "globalRotatableFileManager()->getFile() returned NULL.");

                }
            }
            else {
                return statusOrWriter.getStatus();
            }

            logger::Encoder<AuditEvent>* encoder;
            if (getGlobalAuditManager()->auditFormat == AuditFormatJsonFile) {
                encoder = new AuditEventTextEncoder;
            }
            else if (getGlobalAuditManager()->auditFormat == AuditFormatBsonFile) {
                encoder = new AuditEventBsonEncoder;
            }
            else {
                return Status(ErrorCodes::InternalError, "invalid format");
            }

            getGlobalAuditLogDomain()->attachAppender(
                AuditLogDomain::AppenderAutoPtr(
                    new logger::RotatableFileAppender<AuditEvent>(encoder, writer)));
            break;
        }
        default:
            return Status(ErrorCodes::InternalError, "Audit format misconfigured");
        }
        return Status::OK();
    }

}  // namespace

}  // namespace audit
}  // namespace mongo
