/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/rpc/metadata/audit_metadata.h"

#include <tuple>
#include <utility>

#include "mongo/base/status.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/user_management_commands_parser.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace rpc {

namespace {

const char kAuditMetadataFieldName[] = "$audit";
const char kImpersonatedUsersFieldName[] = "$impersonatedUsers";
const char kImpersonatedRolesFieldName[] = "$impersonatedRoles";

StatusWith<std::vector<UserName>> readImpersonatedUsersFromAuditMetadata(
    const BSONObj& auditMetadata) {
    BSONElement impersonatedUsersEl;
    std::vector<UserName> impersonatedUsers;

    auto impersonatedUsersExtractStatus = bsonExtractTypedField(
        auditMetadata, kImpersonatedUsersFieldName, mongo::Array, &impersonatedUsersEl);

    if (!impersonatedUsersExtractStatus.isOK()) {
        return impersonatedUsersExtractStatus;
    }

    BSONArray impersonatedUsersArr(impersonatedUsersEl.embeddedObject());

    auto userNamesParseStatus = auth::parseUserNamesFromBSONArray(impersonatedUsersArr,
                                                                  "",  // dbname unused
                                                                  &impersonatedUsers);

    if (!userNamesParseStatus.isOK()) {
        return userNamesParseStatus;
    }

    return impersonatedUsers;
}

StatusWith<std::vector<RoleName>> readImpersonatedRolesFromAuditMetadata(
    const BSONObj& auditMetadata) {
    BSONElement impersonatedRolesEl;
    std::vector<RoleName> impersonatedRoles;

    auto impersonatedRolesExtractStatus = bsonExtractTypedField(
        auditMetadata, kImpersonatedRolesFieldName, mongo::Array, &impersonatedRolesEl);

    if (!impersonatedRolesExtractStatus.isOK()) {
        return impersonatedRolesExtractStatus;
    }

    BSONArray impersonatedRolesArr(impersonatedRolesEl.embeddedObject());

    auto rolesParseStatus = auth::parseRoleNamesFromBSONArray(impersonatedRolesArr,
                                                              "",  // dbname unused
                                                              &impersonatedRoles);

    if (!rolesParseStatus.isOK()) {
        return rolesParseStatus;
    }

    return impersonatedRoles;
}

void appendImpersonatedUsers(const std::vector<UserName>& users,
                             StringData usersFieldName,
                             BSONObjBuilder* auditMetadataBob) {
    BSONArrayBuilder usersArrayBab(auditMetadataBob->subarrayStart(usersFieldName));

    for (const auto& userName : users) {
        BSONObjBuilder userNameBob(usersArrayBab.subobjStart());
        userNameBob.append(AuthorizationManager::USER_NAME_FIELD_NAME, userName.getUser());
        userNameBob.append(AuthorizationManager::USER_DB_FIELD_NAME, userName.getDB());
    }
}

void appendImpersonatedRoles(const std::vector<RoleName>& roles,
                             StringData rolesFieldName,
                             BSONObjBuilder* auditMetadataBob) {
    BSONArrayBuilder rolesArrayBob(auditMetadataBob->subarrayStart(rolesFieldName));

    for (const auto& roleName : roles) {
        BSONObjBuilder roleNameBob(rolesArrayBob.subobjStart());
        roleNameBob.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, roleName.getRole());
        roleNameBob.append(AuthorizationManager::ROLE_DB_FIELD_NAME, roleName.getDB());
    }
}

}  // namespace

StatusWith<AuditMetadata> AuditMetadata::readFromMetadata(const BSONObj& metadataObj) {
    return readFromMetadata(metadataObj.getField(fieldName()));
}
StatusWith<AuditMetadata> AuditMetadata::readFromMetadata(const BSONElement& metadataEl) {
    // We expect the AuditMetadata field to have type 'Object' with a field name of '$audit'.
    // The layout of the '$audit' element should look like this:
    //     {$impersonatedUsers: [<user0>, <user1>, ... , <userN>],
    //     {$impersonatedRoles: [<role1>, <role2>, ... , <roleM>]}
    //
    // It is legal for the $audit element to not be present, but if it is present it is an error
    // for it to have any other layout.
    if (metadataEl.eoo()) {
        return AuditMetadata{};
    } else if (metadataEl.type() != mongo::Object) {
        return {ErrorCodes::TypeMismatch,
                str::stream() << "ServerSelectionMetadata element has incorrect type: expected"
                              << mongo::Object
                              << " but got "
                              << metadataEl.type()};
    }

    auto auditObj = metadataEl.embeddedObject();
    if (auditObj.nFields() != 2) {
        return Status(ErrorCodes::IncompatibleAuditMetadata,
                      str::stream() << "Expected auditMetadata to have only 2 fields but got: "
                                    << auditObj);
    }

    auto swImpersonatedUsers = readImpersonatedUsersFromAuditMetadata(auditObj);

    if (!swImpersonatedUsers.isOK()) {
        return swImpersonatedUsers.getStatus();
    }

    auto swImpersonatedRoles = readImpersonatedRolesFromAuditMetadata(auditObj);

    if (!swImpersonatedRoles.isOK()) {
        return swImpersonatedRoles.getStatus();
    }

    return AuditMetadata(std::make_tuple(std::move(swImpersonatedUsers.getValue()),
                                         std::move(swImpersonatedRoles.getValue())));
}

Status AuditMetadata::writeToMetadata(BSONObjBuilder* metadataBob) const {
    const auto& impersonatedUsersAndRoles = getImpersonatedUsersAndRoles();

    if (impersonatedUsersAndRoles == boost::none) {
        return Status::OK();
    }

    BSONObjBuilder auditMetadataBob(metadataBob->subobjStart(kAuditMetadataFieldName));

    appendImpersonatedUsers(
        std::get<0>(*impersonatedUsersAndRoles), kImpersonatedUsersFieldName, &auditMetadataBob);
    appendImpersonatedRoles(
        std::get<1>(*impersonatedUsersAndRoles), kImpersonatedRolesFieldName, &auditMetadataBob);

    return Status::OK();
}

}  // namespace rpc
}  // namespace mongo
