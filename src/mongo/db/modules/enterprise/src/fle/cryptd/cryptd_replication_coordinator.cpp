/**
 *    Copyright (C) 2019 MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "cryptd_replication_coordinator.h"

namespace mongo {
namespace repl {

CryptDReplicationCoordinatorNoOp::CryptDReplicationCoordinatorNoOp(ServiceContext* service)
    : _service(service) {}

void CryptDReplicationCoordinatorNoOp::startup(OperationContext* opCtx) {}

void CryptDReplicationCoordinatorNoOp::enterTerminalShutdown() {}

void CryptDReplicationCoordinatorNoOp::shutdown(OperationContext* opCtx) {}

ReplicationCoordinator::Mode CryptDReplicationCoordinatorNoOp::getReplicationMode() const {
    return modeReplSet;
}

bool CryptDReplicationCoordinatorNoOp::isReplEnabled() const {
    return getReplicationMode() == modeReplSet;
}

MemberState CryptDReplicationCoordinatorNoOp::getMemberState() const {
    return MemberState::RS_PRIMARY;
}

OpTime CryptDReplicationCoordinatorNoOp::getMyLastAppliedOpTime() const {
    return OpTime{};
}

const ReplSettings& CryptDReplicationCoordinatorNoOp::getSettings() const {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::isMasterForReportingPurposes() {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::canAcceptWritesForDatabase(OperationContext* opCtx,
                                                                  StringData dbName) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::canAcceptWritesForDatabase_UNSAFE(OperationContext* opCtx,
                                                                         StringData dbName) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::canAcceptWritesFor_UNSAFE(OperationContext* opCtx,
                                                                 const NamespaceString& ns) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::canAcceptWritesFor(OperationContext* opCtx,
                                                          const NamespaceString& ns) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::checkCanServeReadsFor_UNSAFE(OperationContext* opCtx,
                                                                      const NamespaceString& ns,
                                                                      bool slaveOk) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::checkCanServeReadsFor(OperationContext* opCtx,
                                                               const NamespaceString& ns,
                                                               bool slaveOk) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::isInPrimaryOrSecondaryState_UNSAFE() const {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::isInPrimaryOrSecondaryState(OperationContext* opCtx) const {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::shouldRelaxIndexConstraints(OperationContext* opCtx,
                                                                   const NamespaceString& ns) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::getMaintenanceMode() {
    MONGO_UNREACHABLE;
}

WriteConcernOptions CryptDReplicationCoordinatorNoOp::getGetLastErrorDefault() {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::buildsIndexes() {
    MONGO_UNREACHABLE;
}

OpTimeAndWallTime CryptDReplicationCoordinatorNoOp::getMyLastAppliedOpTimeAndWallTime() const {
    MONGO_UNREACHABLE;
}

WriteConcernOptions CryptDReplicationCoordinatorNoOp::populateUnsetWriteConcernOptionsSyncMode(
    WriteConcernOptions wc) {
    MONGO_UNREACHABLE;
}

OpTime CryptDReplicationCoordinatorNoOp::getCurrentCommittedSnapshotOpTime() const {
    MONGO_UNREACHABLE;
}

OpTimeAndWallTime CryptDReplicationCoordinatorNoOp::getCurrentCommittedSnapshotOpTimeAndWallTime()
    const {
    MONGO_UNREACHABLE;
}
void CryptDReplicationCoordinatorNoOp::appendDiagnosticBSON(mongo::BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::appendConnectionStats(
    executor::ConnectionPoolStats* stats) const {
    MONGO_UNREACHABLE;
}

std::vector<repl::MemberData> CryptDReplicationCoordinatorNoOp::getMemberData() const {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::canAcceptNonLocalWrites() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::waitForMemberState(MemberState, Milliseconds) {
    MONGO_UNREACHABLE;
}

Seconds CryptDReplicationCoordinatorNoOp::getSlaveDelaySecs() const {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::clearSyncSourceBlacklist() {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::setFollowerMode(const MemberState&) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::setFollowerModeStrict(OperationContext* opCtx,
                                                               const MemberState&) {
    MONGO_UNREACHABLE;
}

ReplicationCoordinator::ApplierState CryptDReplicationCoordinatorNoOp::getApplierState() {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::signalDrainComplete(OperationContext*, long long) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::waitForDrainFinish(Milliseconds) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::signalUpstreamUpdater() {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::setMyHeartbeatMessage(const std::string&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::setMyLastAppliedOpTimeAndWallTimeForward(
    const OpTimeAndWallTime&, DataConsistency) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::setMyLastDurableOpTimeAndWallTimeForward(
    const OpTimeAndWallTime&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::setMyLastAppliedOpTimeAndWallTime(const OpTimeAndWallTime&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::setMyLastDurableOpTimeAndWallTime(const OpTimeAndWallTime&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::resetMyLastOpTimes() {
    MONGO_UNREACHABLE;
}

OpTimeAndWallTime CryptDReplicationCoordinatorNoOp::getMyLastDurableOpTimeAndWallTime() const {
    MONGO_UNREACHABLE;
}

OpTime CryptDReplicationCoordinatorNoOp::getMyLastDurableOpTime() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::waitUntilOpTimeForRead(
    OperationContext*, const ReadConcernArgs& readConcern) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::waitUntilOpTimeForReadUntil(OperationContext*,
                                                                     const ReadConcernArgs&,
                                                                     boost::optional<Date_t>) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::awaitTimestampCommitted(OperationContext* opCtx,
                                                                 Timestamp ts) {
    MONGO_UNREACHABLE;
}

ReplicationCoordinator::StatusAndDuration CryptDReplicationCoordinatorNoOp::awaitReplication(
    OperationContext*, const OpTime&, const WriteConcernOptions&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::stepDown(OperationContext*,
                                                const bool,
                                                const Milliseconds&,
                                                const Milliseconds&) {
    MONGO_UNREACHABLE;
}

OID CryptDReplicationCoordinatorNoOp::getElectionId() {
    MONGO_UNREACHABLE;
}

int CryptDReplicationCoordinatorNoOp::getMyId() const {
    MONGO_UNREACHABLE;
}

HostAndPort CryptDReplicationCoordinatorNoOp::getMyHostAndPort() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::resyncData(OperationContext*, bool) {
    MONGO_UNREACHABLE;
}

StatusWith<BSONObj> CryptDReplicationCoordinatorNoOp::prepareReplSetUpdatePositionCommand() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetGetStatus(BSONObjBuilder*,
                                                                 ReplSetGetStatusResponseStyle) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::fillIsMasterForReplSet(IsMasterResponse*,
                                                              const SplitHorizon::Parameters&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::appendSlaveInfoData(BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

ReplSetConfig CryptDReplicationCoordinatorNoOp::getConfig() const {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::processReplSetGetConfig(BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::processReplSetMetadata(const rpc::ReplSetMetadata&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::cancelAndRescheduleElectionTimeout() {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::setMaintenanceMode(bool) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetSyncFrom(OperationContext*,
                                                                const HostAndPort&,
                                                                BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetFreeze(int, BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetReconfig(OperationContext*,
                                                                const ReplSetReconfigArgs&,
                                                                BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetInitiate(OperationContext*,
                                                                const BSONObj&,
                                                                BSONObjBuilder*) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::abortCatchupIfNeeded(
    PrimaryCatchUpConclusionReason reason) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::incrementNumCatchUpOpsIfCatchingUp(int numOps) {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetUpdatePosition(const UpdatePositionArgs&,
                                                                      long long*) {
    MONGO_UNREACHABLE;
}

std::vector<HostAndPort> CryptDReplicationCoordinatorNoOp::getHostsWrittenTo(const OpTime&, bool) {
    MONGO_UNREACHABLE;
}

std::vector<HostAndPort> CryptDReplicationCoordinatorNoOp::getOtherNodesInReplSet() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::checkIfWriteConcernCanBeSatisfied(
    const WriteConcernOptions&) const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::checkIfCommitQuorumCanBeSatisfied(
    const CommitQuorumOptions& commitQuorum) const {
    MONGO_UNREACHABLE;
}

StatusWith<bool> CryptDReplicationCoordinatorNoOp::checkIfCommitQuorumIsSatisfied(
    const CommitQuorumOptions& commitQuorum,
    const std::vector<HostAndPort>& commitReadyMembers) const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::checkReplEnabledForCommand(BSONObjBuilder*) {
    return Status(ErrorCodes::NoReplicationEnabled, "no replication on embedded");
}

HostAndPort CryptDReplicationCoordinatorNoOp::chooseNewSyncSource(const OpTime&) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::blacklistSyncSource(const HostAndPort&, Date_t) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::resetLastOpTimesFromOplog(OperationContext*,
                                                                 DataConsistency) {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::shouldChangeSyncSource(
    const HostAndPort&, const rpc::ReplSetMetadata&, boost::optional<rpc::OplogQueryMetadata>) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::advanceCommitPoint(const OpTimeAndWallTime&,
                                                          bool fromSyncSource) {
    MONGO_UNREACHABLE;
}

OpTime CryptDReplicationCoordinatorNoOp::getLastCommittedOpTime() const {
    MONGO_UNREACHABLE;
}

OpTimeAndWallTime CryptDReplicationCoordinatorNoOp::getLastCommittedOpTimeAndWallTime() const {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processReplSetRequestVotes(OperationContext*,
                                                                    const ReplSetRequestVotesArgs&,
                                                                    ReplSetRequestVotesResponse*) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::prepareReplMetadata(const BSONObj&,
                                                           const OpTime&,
                                                           BSONObjBuilder*) const {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::getWriteConcernMajorityShouldJournal() {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::processHeartbeatV1(const ReplSetHeartbeatArgsV1&,
                                                            ReplSetHeartbeatResponse*) {
    MONGO_UNREACHABLE;
}

long long CryptDReplicationCoordinatorNoOp::getTerm() {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::updateTerm(OperationContext*, long long) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::waitUntilSnapshotCommitted(OperationContext*,
                                                                  const Timestamp&) {
    MONGO_UNREACHABLE;
}

size_t CryptDReplicationCoordinatorNoOp::getNumUncommittedSnapshots() {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::createWMajorityWriteAvailabilityDateWaiter(OpTime opTime) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::dropAllSnapshots() {
    MONGO_UNREACHABLE;
}

Status CryptDReplicationCoordinatorNoOp::stepUpIfEligible(bool skipDryRun) {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::signalDropPendingCollectionsRemovedFromStorage() {
    MONGO_UNREACHABLE;
}

boost::optional<Timestamp> CryptDReplicationCoordinatorNoOp::getRecoveryTimestamp() {
    MONGO_UNREACHABLE;
}

bool CryptDReplicationCoordinatorNoOp::setContainsArbiter() const {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::attemptToAdvanceStableTimestamp() {
    MONGO_UNREACHABLE;
}

void CryptDReplicationCoordinatorNoOp::finishRecoveryIfEligible(OperationContext* opCtx) {
    MONGO_UNREACHABLE;
}

}  // namespace repl
}  // namespace mongo
