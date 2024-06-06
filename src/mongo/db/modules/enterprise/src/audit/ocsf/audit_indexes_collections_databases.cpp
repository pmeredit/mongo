/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/database_name.h"
#include "mongo/db/modules/enterprise/src/audit/ocsf/audit_ocsf.h"
#include "mongo/db/namespace_string.h"
#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"

namespace mongo::audit {

namespace {

constexpr auto kEntityManagementActivityCreate = 1;
constexpr auto kEntityManagementActivityUpdate = 3;
constexpr auto kEntityManagementActivityDelete = 4;
constexpr auto kEntityManagementSeverityInformational = 1;

constexpr auto kEntityField = "entity"_sd;
constexpr auto kEntityResultField = "entity_result"_sd;
constexpr auto kCommentField = "comment"_sd;
constexpr auto kPipelineField = "pipeline"_sd;

constexpr auto kDropCollectionId = "dropCollection"_sd;
constexpr auto kRenameCollectionId = "renameCollection"_sd;
constexpr auto kToId = "to"_sd;
constexpr auto kCommandId = "command"_sd;

constexpr auto kCollectionEntityTypeValue = "collection"_sd;
constexpr auto kLogCreateCollectionEntityTypeValue = "create_collection"_sd;
constexpr auto kLogRenameCollectionEntityTypeValue = "rename_collection_source"_sd;
constexpr auto kLogRenameCollectionEntityResultTypeValue = "rename_collection_destination"_sd;
constexpr auto kLogDropCollectionEntityTypeValue = "drop_collection"_sd;
constexpr auto kLogCreateDatabaseEntityTypeValue = "create_database"_sd;
constexpr auto kLogDropDatabaseEntityTypeValue = "drop_database"_sd;
constexpr auto kLogImportCollectionEntityTypeValue = "import_collection"_sd;
constexpr auto kLogCreateIndexEntityTypeValue = "create_index"_sd;
constexpr auto kLogDropIndexEntityTypeValue = "drop_index"_sd;
constexpr auto kLogCreateViewEntityResultTypeValue = "create_view"_sd;
constexpr auto kLogDropViewEntityResultTypeValue = "drop_view"_sd;

std::string serializeIndexName(StringData indexname, const NamespaceString& nsname) {
    return fmt::format(
        "{}@{}",
        indexname,
        NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
}

}  // namespace

void AuditOCSF::logCreateIndex(Client* client,
                               const BSONObj* indexSpec,
                               StringData indexname,
                               const NamespaceString& nsname,
                               StringData indexBuildState,
                               ErrorCodes::Error result) const {

    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityCreate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildEntity(builder,
                                                     kEntityField,
                                                     indexSpec,
                                                     serializeIndexName(indexname, nsname),
                                                     kLogCreateIndexEntityTypeValue);
             builder->append(kCommentField, "IndexBuildState: " + indexBuildState);
         },
         result});
}

void AuditOCSF::logCreateCollection(Client* client, const NamespaceString& nsname) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityCreate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()),
                 kLogCreateCollectionEntityTypeValue);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logCreateView(Client* client,
                              const NamespaceString& nsname,
                              const NamespaceString& viewOn,
                              BSONArray pipeline,
                              ErrorCodes::Error code) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityCreate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(viewOn, SerializationContext::stateDefault()),
                 kCollectionEntityTypeValue);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityResultField,
                 &pipeline,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()),
                 kLogCreateViewEntityResultTypeValue);
         },
         code});
}

void AuditOCSF::logImportCollection(Client* client, const NamespaceString& nsname) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityUpdate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()),
                 kLogImportCollectionEntityTypeValue);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logRenameCollection(Client* client,
                                    const NamespaceString& source,
                                    const NamespaceString& target) const {
    if (!isDDLAuditingAllowed(client, source)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityUpdate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(source, SerializationContext::stateDefault()),
                 kLogRenameCollectionEntityTypeValue);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityResultField,
                 nullptr,
                 NamespaceStringUtil::serialize(target, SerializationContext::stateDefault()),
                 kLogRenameCollectionEntityResultTypeValue);
         },
         ErrorCodes::OK});

    BSONObjBuilder builder;
    builder.append(kRenameCollectionId,
                   NamespaceStringUtil::serialize(source, SerializationContext::stateDefault()));
    builder.append(kToId,
                   NamespaceStringUtil::serialize(target, SerializationContext::stateDefault()));
    const auto cmdObj = builder.done();

    logDirectAuthOperation(client, source, cmdObj, DirectAuthOperation::kRename);
    logDirectAuthOperation(client, target, cmdObj, DirectAuthOperation::kRename);
}

void AuditOCSF::logCreateDatabase(Client* client, const DatabaseName& dbname) const {
    const auto& ns = NamespaceString(dbname);

    if (!isDDLAuditingAllowed(client, ns)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityCreate,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()),
                 kLogCreateDatabaseEntityTypeValue);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logDropIndex(Client* client,
                             StringData indexname,
                             const NamespaceString& nsname) const {

    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityDelete,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(builder,
                                                     kEntityField,
                                                     nullptr,
                                                     serializeIndexName(indexname, nsname),
                                                     kLogDropIndexEntityTypeValue);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logDropCollection(Client* client, const NamespaceString& nsname) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityDelete,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()),
                 kLogDropCollectionEntityTypeValue);
         },
         ErrorCodes::OK});

    BSONObjBuilder builder;
    builder.append(kDropCollectionId,
                   NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()));
    const auto cmdObj = builder.done();
    logDirectAuthOperation(client, nsname, cmdObj, DirectAuthOperation::kDrop);
}

void AuditOCSF::logDropDatabase(Client* client, const DatabaseName& dbname) const {
    const auto& ns = NamespaceString(dbname);

    if (!isDDLAuditingAllowed(client, ns)) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityDelete,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()),
                 kLogDropDatabaseEntityTypeValue);
         },
         ErrorCodes::OK});
}

void AuditOCSF::logDropView(Client* client,
                            const NamespaceString& nsname,
                            const NamespaceString& viewOn,
                            const std::vector<BSONObj>& pipeline,
                            ErrorCodes::Error code) const {
    if (!isDDLAuditingAllowed(client, nsname)) {
        return;
    }

    BSONObjBuilder builder;
    builder.append(kPipelineField, pipeline);
    BSONObj pipelineObj(builder.done());

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kIdentityAndAccess,
         ocsf::OCSFEventClass::kEntityManagement,
         kEntityManagementActivityDelete,
         kEntityManagementSeverityInformational,
         [&](BSONObjBuilder* builder) {
             AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityField,
                 nullptr,
                 NamespaceStringUtil::serialize(viewOn, SerializationContext::stateDefault()),
                 kCollectionEntityTypeValue);
             AuditOCSF::AuditEventOCSF::_buildEntity(
                 builder,
                 kEntityResultField,
                 nullptr,
                 NamespaceStringUtil::serialize(nsname, SerializationContext::stateDefault()),
                 kLogDropViewEntityResultTypeValue);

             builder->append(kCommentField, pipelineObj);
         },
         code});
}

}  // namespace mongo::audit
