/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "queryable_mmap_v1_extent_manager.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <regex>
#include <utility>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "../blockstore/list_dir.h"
#include "../blockstore/reader.h"
#include "queryable_evictor.h"
#include "queryable_mmap_v1_fs_helpers.h"

namespace mongo {
namespace queryable {

BlockstoreBackedExtentManager::Factory::Factory(Context&& context, std::uint64_t memoryQuotaBytes)
    : _context(std::move(context)), _allocState(stdx::make_unique<AllocState>(memoryQuotaBytes)) {

    // Download all the .ns files.
    log() << "Downloading .ns files...";

    auto httpClient(createHttpClient(getContext()->apiUri(), getContext()->snapshotId()));
    auto swFiles = listDirectory(httpClient.get());
    uassertStatusOK(swFiles);

    std::vector<struct File> nsFiles;
    std::copy_if(
        std::begin(swFiles.getValue()),
        std::end(swFiles.getValue()),
        std::back_inserter(nsFiles),
        [](const struct File& file) -> bool { return StringData(file.filename).endsWith(".ns"); });

    for (const auto& file : nsFiles) {
        log() << "downloading: " << file.filename;

        // FileName from blockstore is relative to the dbpath.
        boost::filesystem::path fullPath =
            boost::filesystem::path(storageGlobalParams.dbpath) / removeDirectory(file.filename);

        queryable::Reader nsFileReader(
            createHttpClient(getContext()->apiUri(), getContext()->snapshotId()),
            file.filename,
            file.fileSize,
            file.blockSize);
        std::ofstream nsFileWriter;
        nsFileWriter.open(fullPath.c_str(),
                          std::ofstream::trunc | std::ofstream::binary | std::ofstream::out);
        uassertStatusOK(nsFileReader.readInto(&nsFileWriter));
        uassert(ErrorCodes::FileStreamFailed,
                str::stream() << "Writing NS file failed. File: " << fullPath.c_str(),
                nsFileWriter.good());
    }


    startQueryableEvictorThread(_allocState.get());
}

std::unique_ptr<ExtentManager> BlockstoreBackedExtentManager::Factory::create(StringData dbname,
                                                                              StringData path,
                                                                              bool directoryPerDB) {
    return stdx::make_unique<BlockstoreBackedExtentManager>(
        this, dbname, path, directoryPerDB, _allocState.get());
}

BlockstoreBackedExtentManager::BlockstoreBackedExtentManager(Factory* factory,
                                                             StringData dbname,
                                                             StringData path,
                                                             bool directoryPerDB,
                                                             AllocState* const allocState)
    : MmapV1ExtentManager(dbname, path, directoryPerDB),
      _factory(factory),
      _dbname(dbname.toString()),
      _directoryPerDB(directoryPerDB),
      _dataFiles(),
      _allocState(allocState) {}

Status BlockstoreBackedExtentManager::init(OperationContext* txn) {
    invariant(_dataFiles.empty());

    auto httpClient(
        createHttpClient(_factory->getContext()->apiUri(), _factory->getContext()->snapshotId()));
    auto swFiles = listDirectory(httpClient.get());
    if (!swFiles.isOK()) {
        return swFiles.getStatus();
    }

    // Can't count on what order we get the ids back in.
    std::vector<std::size_t> dataFileIds;
    std::vector<struct File> dataFiles;
    for (auto&& file : swFiles.getValue()) {
        std::regex dbFilesRegex = getMMAPV1DatafileRegex(_dbname);
        std::smatch matcher;
        bool matches = std::regex_match(file.filename, matcher, dbFilesRegex);
        if (!matches) {
            continue;
        }

        dataFileIds.emplace_back(std::stoi(matcher[1].str()));
        dataFiles.emplace_back(file);
    }

    invariant(!dataFiles.empty());
    invariant(dataFiles.size() == dataFileIds.size());

    _dataFiles.resize(dataFiles.size());

    auto df = std::begin(dataFiles);
    auto id = std::begin(dataFileIds);
    for (; df != std::end(dataFiles) && id != std::end(dataFileIds); ++df, ++id) {
        _dataFiles[*id] = stdx::make_unique<queryable::DataFile>(
            stdx::make_unique<queryable::Reader>(
                createHttpClient(_factory->getContext()->apiUri(),
                                 _factory->getContext()->snapshotId()),
                df->filename,
                df->fileSize,
                df->blockSize),
            _allocState);
    }

    return Status::OK();
}


MmapV1RecordHeader* BlockstoreBackedExtentManager::recordForV1(const DiskLoc& loc) const {
    auto& dataFile = _dataFiles[loc.a()];
    auto offset = loc.getOfs();

    uassertStatusOK(dataFile->ensureRange(offset, MmapV1RecordHeader::HeaderSizeValue::HeaderSize));

    auto recordPtr = static_cast<char*>(dataFile->getBasePtr()) + offset;
    auto recordHeader = reinterpret_cast<MmapV1RecordHeader*>(recordPtr);
    uassertStatusOK(dataFile->ensureRange(offset, recordHeader->lengthWithHeaders()));
    return recordHeader;
}

Extent* BlockstoreBackedExtentManager::getExtent(const DiskLoc& loc, bool) const {
    auto& dataFile = _dataFiles[loc.a()];
    auto offset = loc.getOfs();

    uassertStatusOK(dataFile->ensureRange(offset, Extent::HeaderSize()));
    auto extentPtr = static_cast<char*>(dataFile->getBasePtr()) + offset;
    return reinterpret_cast<Extent*>(extentPtr);
}

}  // namespace queryable
}  // namespace mongo
