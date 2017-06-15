/**
 *    Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_private.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo {
namespace audit {

class SetParameterEvent : public AuditEvent {
public:
    SetParameterEvent(const AuditEventEnvelope& envelope,
                      std::map<std::string, BSONElement> parametersToSet)
        : AuditEvent(envelope), _parametersToSet(parametersToSet) {}
    virtual ~SetParameterEvent() = default;

private:
    virtual std::ostream& putTextDescription(std::ostream& os) const;
    virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

    std::map<std::string, BSONElement> _parametersToSet;
};

std::ostream& SetParameterEvent::putTextDescription(std::ostream& os) const {
    os << "Set runtime parameters: ";
    for (const auto& param : _parametersToSet) {
        os << param.second << " ";
    }
    return os;
}

BSONObjBuilder& SetParameterEvent::putParamsBSON(BSONObjBuilder& builder) const {
    for (const auto& param : _parametersToSet) {
        builder.append(param.second);
    }
    return builder;
}

void logSetParameter(Client* client, const std::map<std::string, BSONElement>& parametersToSet) {
    if (!getGlobalAuditManager()->enabled)
        return;

    SetParameterEvent event(makeEnvelope(client, ActionType::setParameter, ErrorCodes::OK),
                            parametersToSet);
    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        getGlobalAuditLogDomain()->append(event).transitional_ignore();
    }
}
}  // namespace audit
}  // namespace mongo
