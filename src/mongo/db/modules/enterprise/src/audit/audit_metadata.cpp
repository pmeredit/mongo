/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/rpc/metadata/audit_metadata.h"

#include <utility>
#include <tuple>

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

    StatusWith<std::vector<UserName>>
    readImpersonatedUsersFromAuditMetadata(const BSONObj& auditMetadata) {
        BSONElement impersonatedUsersEl;
        std::vector<UserName> impersonatedUsers;

        auto impersonatedUsersExtractStatus = bsonExtractTypedField(
            auditMetadata,
            kImpersonatedUsersFieldName,
            mongo::Array,
            &impersonatedUsersEl
        );

        if (!impersonatedUsersExtractStatus.isOK()) {
            return impersonatedUsersExtractStatus;
        }

        BSONArray impersonatedUsersArr(impersonatedUsersEl.embeddedObject());

        auto userNamesParseStatus = auth::parseUserNamesFromBSONArray(
            impersonatedUsersArr,
            "", // dbname unused
            &impersonatedUsers
        );

        if (!userNamesParseStatus.isOK()) {
            return userNamesParseStatus;
        }

        return impersonatedUsers;
    }

    StatusWith<std::vector<RoleName>>
    readImpersonatedRolesFromAuditMetadata(const BSONObj& auditMetadata) {
        BSONElement impersonatedRolesEl;
        std::vector<RoleName> impersonatedRoles;

        auto impersonatedRolesExtractStatus = bsonExtractTypedField(
            auditMetadata,
            kImpersonatedRolesFieldName,
            mongo::Array,
            &impersonatedRolesEl
        );

        if (!impersonatedRolesExtractStatus.isOK()) {
            return impersonatedRolesExtractStatus;
        }

        BSONArray impersonatedRolesArr(impersonatedRolesEl.embeddedObject());

        auto rolesParseStatus = auth::parseRoleNamesFromBSONArray(
            impersonatedRolesArr,
            "", // dbname unused
            &impersonatedRoles
        );

        if (!rolesParseStatus.isOK()) {
            return rolesParseStatus;
        }

        return impersonatedRoles;
    }

    void appendImpersonatedUsers(const std::vector<UserName>& users,
                                 StringData usersFieldName,
                                 BSONObjBuilder* auditMetadataBob) {

        BSONArrayBuilder usersArrayBab(
            auditMetadataBob->subarrayStart(usersFieldName)
        );

        for (const auto& userName : users) {
            BSONObjBuilder userNameBob(usersArrayBab.subobjStart());
            userNameBob.append(AuthorizationManager::USER_NAME_FIELD_NAME, userName.getUser());
            userNameBob.append(AuthorizationManager::USER_DB_FIELD_NAME, userName.getDB());
        }
    }

    void appendImpersonatedRoles(const std::vector<RoleName>& roles,
                                 StringData rolesFieldName,
                                 BSONObjBuilder* auditMetadataBob) {

        BSONArrayBuilder rolesArrayBob(
            auditMetadataBob->subarrayStart(rolesFieldName)
        );

        for (const auto& roleName: roles) {
            BSONObjBuilder roleNameBob(rolesArrayBob.subobjStart());
            roleNameBob.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, roleName.getRole());
            roleNameBob.append(AuthorizationManager::ROLE_DB_FIELD_NAME, roleName.getDB());
        }

    }

}  // namespace

    StatusWith<AuditMetadata> AuditMetadata::readFromMetadata(const BSONObj& metadataObj) {
        // We expect the AuditMetadata field to have type 'Object' with a field name of '$audit'.
        // The layout of the '$audit' element should look like this:
        //     {$impersonatedUsers: [<user0>, <user1>, ... , <userN>],
        //     {$impersonatedRoles: [<role1>, <role2>, ... , <roleM>]}
        //
        // It is legal for the $audit element to not be present, but if it is present it is an error
        // for it to have any other layout.
        BSONElement auditMetadataEl;
        auto auditExtractStatus = bsonExtractTypedField(metadataObj,
                                                        kAuditMetadataFieldName,
                                                        mongo::Object,
                                                        &auditMetadataEl);

        if (auditExtractStatus == ErrorCodes::NoSuchKey) {
            return AuditMetadata{boost::none};
        }
        else if (!auditExtractStatus.isOK()) {
            return auditExtractStatus;
        }

        if (auditMetadataEl.embeddedObject().nFields() != 2) {
            return Status(ErrorCodes::IncompatibleAuditMetadata,
                          str::stream() << "Expected auditMetadata to have only 2 fields but got: "
                                        << auditMetadataEl.embeddedObject());

        }

        auto swImpersonatedUsers = readImpersonatedUsersFromAuditMetadata(
            auditMetadataEl.embeddedObject()
        );

        if (!swImpersonatedUsers.isOK()) {
            return swImpersonatedUsers.getStatus();
        }

        auto swImpersonatedRoles = readImpersonatedRolesFromAuditMetadata(
            auditMetadataEl.embeddedObject()
        );

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
            std::get<0>(*impersonatedUsersAndRoles),
            kImpersonatedUsersFieldName,
            &auditMetadataBob
        );
        appendImpersonatedRoles(
            std::get<1>(*impersonatedUsersAndRoles),
            kImpersonatedRolesFieldName,
            &auditMetadataBob
        );

        return Status::OK();
    }

    Status AuditMetadata::downconvert(const BSONObj& command,
                                      const BSONObj& metadata,
                                      BSONObjBuilder* legacyCommandBob,
                                      int* legacyQueryFlags) {

        // Write out all command elements unmodified.
        legacyCommandBob->appendElements(command);

        const auto swAuditMetadata = AuditMetadata::readFromMetadata(metadata);
        if (!swAuditMetadata.isOK()) {
            return swAuditMetadata.getStatus();
        }

        const auto& impersonatedUsersAndRoles = swAuditMetadata.getValue()
                                                               .getImpersonatedUsersAndRoles();

        if (impersonatedUsersAndRoles == boost::none) {
            return Status::OK();
        }

        // The users are intentionally appended first.
        appendImpersonatedUsers(
            std::get<0>(*impersonatedUsersAndRoles),
            kLegacyImpersonatedUsersFieldName,
            legacyCommandBob
        );
        appendImpersonatedRoles(
            std::get<1>(*impersonatedUsersAndRoles),
            kLegacyImpersonatedRolesFieldName,
            legacyCommandBob
        );

        return Status::OK();
    }

    Status AuditMetadata::upconvert(const BSONObj& legacyCommand,
                                    const int legacyQueryFlags,
                                    BSONObjBuilder* commandBob,
                                    BSONObjBuilder* metadataBob) {
        // The pre-OP_COMMAND utilities for parsing impersonated users/roles rely on mutating
        // the command object.
        BSONObj mutableCommand(legacyCommand.getOwned());

        bool usersFieldIsPresent = false;
        bool rolesFieldIsPresent = false;
        std::vector<UserName> impersonatedUsers;
        std::vector<RoleName> impersonatedRoles;

        try {
            audit::parseAndRemoveImpersonatedRolesField(mutableCommand,
                                                        &impersonatedRoles,
                                                        &rolesFieldIsPresent);

            audit::parseAndRemoveImpersonatedUsersField(mutableCommand,
                                                        &impersonatedUsers,
                                                        &usersFieldIsPresent);
        }
        catch (...) {
            return exceptionToStatus();
        }

        if (rolesFieldIsPresent != usersFieldIsPresent) {
            // If there is a version mismatch between the mongos and the mongod,
            // the mongos may fail to pass the role information, causing an error.
            return Status(ErrorCodes::IncompatibleAuditMetadata,
                          "Audit metadata does not include both user and role information");
        }

        // Append remaining elements of the mutable command.
        commandBob->appendElements(mutableCommand);

        const auto am = rolesFieldIsPresent ?
            AuditMetadata(std::make_tuple(std::move(impersonatedUsers),
                                          std::move(impersonatedRoles))) :
            AuditMetadata(boost::none);


        return am.writeToMetadata(metadataBob);
    }

}  // namespace rpc
}  // namespace mongo
