/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_log_domain.h"
#include "audit_manager_global.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/server_parameters.h"
#include "mongo/logger/console_appender.h"
#include "mongo/logger/encoder.h"
#include "mongo/logger/log_domain-impl.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/logger/rotatable_file_appender.h"
#include "mongo/logger/rotatable_file_manager.h"
#include "mongo/logger/rotatable_file_writer.h"

namespace mongo {

namespace logger {
    template class LogDomain<audit::AuditEvent>;
}  // namespace logger

namespace audit {

    static AuditLogDomain globalAuditLogDomain;

    AuditLogDomain* getGlobalAuditLogDomain() { return &globalAuditLogDomain; }

namespace {

    class AuditEventTextEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventTextEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            os << logger::MessageEventDetailsEncoder::getDateFormatter()(event.getTimestamp()) <<
                ' ';

            UserSet::NameIterator users = event.getAuthenticatedUsers();
            if (users.more()) {
                os << users.next().getFullName();
                while (users.more()) {
                    os << ',' << users.next().getFullName();
                }
                os << ' ';
            }
            os << event.getRemoteAddr().toString() << '/' << event.getLocalAddr().toString() << ' ';
            os <<
                event.getOperationId().getConnectionId() << '.' <<
                event.getOperationId().getOperationNumber() << ' ';
            return event.putText(os) << '\n';
        }
    };

    class AuditEventBsonEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventBsonEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            BSONObj toWrite(event.toBSON());
            return os.write(toWrite.objdata(), toWrite.objsize());
        }
    };

    MONGO_INITIALIZER_WITH_PREREQUISITES(AuditDomain, ("CreateAuditManager"))(InitializerContext*) {
        
        std::string auditLogPath(getGlobalAuditManager()->auditLogPath);
        if (auditLogPath == ":console") {
            getGlobalAuditLogDomain()->attachAppender(
                    AuditLogDomain::AppenderAutoPtr(
                            new logger::ConsoleAppender<AuditEvent>(new AuditEventTextEncoder)));
        }
        else if (!auditLogPath.empty()) {
            logger::StatusWithRotatableFileWriter statusOrWriter =
                logger::globalRotatableFileManager()->openFile(auditLogPath, true);
            logger::RotatableFileWriter* writer;
            if (statusOrWriter.isOK()) {
                writer = statusOrWriter.getValue();
            }
            else {
                writer = logger::globalRotatableFileManager()->getFile(auditLogPath);
                if (!writer) {
                    return Status(ErrorCodes::InternalError, auditLogPath +
                                  " opened successfully, but "
                                  "globalRotatableFileManager()->getFile() returned NULL.");

                }
            }

            logger::Encoder<AuditEvent>* encoder;
            if (getGlobalAuditManager()->auditFormat == AuditFormatText) {
                encoder = new AuditEventTextEncoder;
            }
            else if (getGlobalAuditManager()->auditFormat == AuditFormatBson) {
                encoder = new AuditEventBsonEncoder;
            }
            else {
                return Status(ErrorCodes::InternalError, "invalid format");
            }

            getGlobalAuditLogDomain()->attachAppender(
                    AuditLogDomain::AppenderAutoPtr(
                            new logger::RotatableFileAppender<AuditEvent>(encoder, writer)));
        }
        return Status::OK();
    }

}  // namespace

}  // namespace audit
}  // namespace mongo
