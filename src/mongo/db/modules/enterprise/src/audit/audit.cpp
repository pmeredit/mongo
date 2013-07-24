/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "audit_log_domain.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/server_parameters.h"
#include "mongo/logger/console_appender.h"
#include "mongo/logger/encoder.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/logger/rotatable_file_appender.h"
#include "mongo/logger/rotatable_file_manager.h"
#include "mongo/logger/rotatable_file_writer.h"

namespace mongo {
namespace audit {
namespace {

    class AuditEventEncoder : public logger::Encoder<AuditEvent> {
        virtual ~AuditEventEncoder() {}
        virtual std::ostream& encode(const AuditEvent& event, std::ostream& os) {
            os << logger::MessageEventDetailsEncoder::getDateFormatter()(event.getTimestamp()) <<
                ' ';

            PrincipalSet::NameIterator users = event.getAuthenticatedUsers();
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

    MONGO_EXPORT_STARTUP_SERVER_PARAMETER(auditLogPath, std::string, std::string());

    MONGO_INITIALIZER(AuditDomain)(InitializerContext*) {
        if (auditLogPath == ":console") {
            getGlobalAuditLogDomain()->attachAppender(
                    AuditLogDomain::AppenderAutoPtr(
                            new logger::ConsoleAppender<AuditEvent>(new AuditEventEncoder)));
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
            getGlobalAuditLogDomain()->attachAppender(
                    AuditLogDomain::AppenderAutoPtr(
                            new logger::RotatableFileAppender<AuditEvent>(new AuditEventEncoder,
                                                                          writer)));
        }
        return Status::OK();
    }

}  // namespace
}  // namespace audit
}  // namespace mongo
