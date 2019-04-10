/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/storage/backup_cursor_state.h"

namespace mongo {

/**
 * Represents the `$backupCursorExtend` aggregation stage. The DocumentSource will return filenames
 * of the extra journal logs in the running `dbpath`.
 */
class DocumentSourceBackupCursorExtend final : public DocumentSource {
public:
    static const char* kStageName;

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec) {
            return stdx::make_unique<LiteParsed>();
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos) const final {
            return {Privilege(ResourcePattern::forClusterResource(), ActionSet{ActionType::fsync})};
        }

        bool isInitialSource() const final {
            return true;
        }

        bool allowedToForwardFromMongos() const final {
            return false;
        }

        void assertSupportsReadConcern(const repl::ReadConcernArgs& readConcern) const {
            uassert(ErrorCodes::InvalidOptions,
                    str::stream() << "Aggregation stage " << kStageName
                                  << " requires read concern local but found "
                                  << readConcern.toString(),
                    readConcern.getLevel() == repl::ReadConcernLevel::kLocalReadConcern);
        }
    };

    GetNextResult getNext() final;

    const char* getSourceName() const final {
        return kStageName;
    }

    boost::optional<MergingLogic> mergingLogic() {
        return boost::none;
    }

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     // This stage `uasserts` on a MongoS; the
                                     // `HostTypeRequirement` field has no effect.
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed);

        constraints.isIndependentOfAnyCollection = true;
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    void acceptVisitor(DocumentSourceVisitor* visitor) final {
        visitor->visit(this);
    }

private:
    DocumentSourceBackupCursorExtend(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                                     const UUID& backupId,
                                     const Timestamp& extendTo);

    const UUID _backupId;
    const Timestamp _extendTo;
    BackupCursorExtendState _backupCursorExtendState;
};

}  // namespace mongo
