#pragma once

#include <filesystem>

#include "streams/exec/checkpoint/manifest_builder.h"
#include "streams/exec/checkpoint/restorer.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/util/units.h"

namespace streams {

// This class provides a local disk implementation of the CheckpointStorage interface.
// This class works in the context of one StreamProcessor, each SP will create and
// own an object of this type
class LocalDiskCheckpointStorage : public CheckpointStorage {
public:
    struct Options {
        // The file system paths to use for writing-to and reading-from. On the saving side, a
        // stream processor named SP1 will generate a checkpoint with id chk1 under a dir like this:
        // writeRootDir/SP1/chk1/..., writeRootDir/SP1/chk2/...  .On the restore side, the
        // checkpoint files are expected to be under a similar relative path like:
        // restoreRootDir/SP/chk1/..., restoreRootDir/SP/chk2/...
        std::filesystem::path writeRootDir;
        std::filesystem::path restoreRootDir;

        // The operator state is written to one or more state files. This parameter is a soft limit
        // for the maximum size of one state file. After appending a Document via an appendState
        // call, if the file has exceeded this limit, then we start a new file
        const size_t maxStateFileSizeHint = 64_MiB;
    };

    struct ActiveCheckpointSave {
        CheckpointId checkpointId;
        int currStateFileIdx = 0;
        off_t currStateFileOffset = 0;
        std::unique_ptr<mongo::BufBuilder> stateFileBuf;
        ManifestBuilder manifest;
    };

    LocalDiskCheckpointStorage(Options cfg, const Context* ctxt);

    size_t maxStateFileSize() const {
        return _opts.maxStateFileSizeHint;
    }

    std::filesystem::path writeRootDir() const {
        return _opts.writeRootDir;
    }

private:
    using WriterHandle = CheckpointStorage::WriterHandle;
    using ReaderHandle = CheckpointStorage::ReaderHandle;
    using OpsRangeMap = Restorer::OpsRangeMap;
    using FileChecksums = Restorer::FileChecksums;

    // The next group of methods implement the CheckpointStorage interface
    CheckpointId doStartCheckpoint() override;
    void doCommitCheckpoint(CheckpointId id) override;

    // A stream processor SP expects that the files related to checkpoint chk1 are present in dir
    // writeRootDir/SP/chk1/... . In general rootDir need not (and likely will not) be the same as
    // the writeRootDir under which the SP is saving new checkpoint data
    void doStartCheckpointRestore(CheckpointId chkId) override;
    void doMarkCheckpointRestored(CheckpointId chkId) override;
    std::unique_ptr<WriterHandle> doCreateStateWriter(CheckpointId id, OperatorId opId) override;
    std::unique_ptr<ReaderHandle> doCreateStateReader(CheckpointId id, OperatorId opId) override;

    // We maintain one BufBuilder per current state file. As documents arrive, they are
    // serialized-appended at the end of this BufBuilder. The manifest is updated to track the
    // operator ranges.
    void doAppendRecord(WriterHandle* writer, mongo::Document doc) override;
    boost::optional<mongo::Document> doGetNextRecord(ReaderHandle* reader) override;
    void doCloseStateReader(ReaderHandle* reader) override;
    void doCloseStateWriter(WriterHandle* writer) override;

    // Multiple in-progress checkpoints are not supported. Once either doCommitCheckpoint or
    // doMarkCheckpointRestored is called, that checkpoint is then finalized and no more state can
    // be added/recovered
    bool isActiveCheckpoint(CheckpointId chkId) const;

    bool isCheckpointBeingRestored(CheckpointId chkId) const;

    // We expect there to be only one active writer(i.e. operator) or reader per checkpoint, and an
    // operator can be active just once i.e. once an operator has finished appending or restoring
    // its state(and declared this by closing the writer or reader for that operator), it cannot in
    // the future try to instantiate a new writer/reader and write/restore some more state.
    bool hasActiveWriter(CheckpointId chkId) const {
        return _activeWriter && _activeWriter->first == chkId;
    }

    bool isActiveWriter(CheckpointId chkId, OperatorId opId) const {
        return _activeWriter && _activeWriter->first == chkId && _activeWriter->second == opId;
    }

    bool isFinalizedWriter(CheckpointId chkId, OperatorId opId) const {
        return _finalizedWriters.find({chkId, opId}) != _finalizedWriters.end();
    }

    bool hasActiveReader(CheckpointId chkId) const {
        return _activeReader && _activeReader->first == chkId;
    }

    bool isActiveReader(CheckpointId chkId, OperatorId opId) const {
        return _activeReader && _activeReader->first == chkId && _activeReader->second == opId;
    }

    bool isFinalizedReader(CheckpointId chkId, OperatorId opId) const {
        return _finalizedReaders.find({chkId, opId}) != _finalizedReaders.end();
    }

    // This function gets called internally whenever we have accumulated enough data for 1 state
    // file (as configured via the maxStateFileSizeHint option). It performs the final leg of
    // processing on this data (compresion + checksumming) and then writes it out to disk
    void writeActiveStateFileToDisk();

    // This function is part of the restore path. Given a manifest file, it 1) validates the
    // embedded file checksum 2) Retrieves the operator range maps 3) Retrieves the stored checksums
    // of the state files.
    std::pair<OpsRangeMap, FileChecksums> getManifestInfo(
        const std::filesystem::path& manifestFile);

    // An internal helper for some basic validation of the read-in manifest file during the restore
    // flow. TODO(SERVER-83239): Add logical validation based on actual version. For now, just
    // assume v1
    bool validateManifest(const mongo::Manifest& manifest) const;

    Options _opts;
    std::string _tenantId;
    std::string _streamName;
    std::string _streamProcessorId;
    boost::optional<std::pair<CheckpointId, OperatorId>> _activeWriter;
    boost::optional<std::pair<CheckpointId, OperatorId>> _activeReader;
    mongo::stdx::unordered_set<std::pair<CheckpointId, OperatorId>> _finalizedWriters;
    mongo::stdx::unordered_set<std::pair<CheckpointId, OperatorId>> _finalizedReaders;
    boost::optional<ActiveCheckpointSave> _activeCheckpointSave;
    std::unique_ptr<Restorer> _activeRestorer;
};

}  // namespace streams
