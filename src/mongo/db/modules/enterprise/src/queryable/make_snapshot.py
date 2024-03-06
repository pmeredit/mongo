#!/usr/bin/env python

# Copyright (C) 2016 MongoDB Inc.

"""
Saves a snapshot of files in a dbpath on disk into a Backup MongoDB process running on port 27017.
Outputs the SnapshotID that can be used to run a queryable restore mongod process.

  Usage:
    python make_snapshot.py <dbpath>
"""

import pymongo
import gzip
import hashlib
import re
import os
import sys
import time
from StringIO import StringIO
from bson.binary import Binary
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
import boto3
import bson

dir_to_snapshot = sys.argv[-1]
dir_to_snapshot = dir_to_snapshot.rstrip('/') + '/'

jobId = ObjectId()
groupId = ObjectId()
snapshot_id = ObjectId()

backupdb_uri = "mongodb://127.0.0.1:27017"
blockstore_uri = "mongodb://127.0.0.1:27017"
blocksize = 64 * 1024

print "Inserting snapshot data into " + backupdb_uri
print "Inserting BlockFiles/Blocks into " + blockstore_uri
print 'SnapshotId: ObjectId("%s")' % (str(snapshot_id),)
print "Blocksize: " + str(blocksize)

backupdb = pymongo.MongoClient(backupdb_uri)
blockstore = pymongo.MongoClient(blockstore_uri)

snapshots = backupdb.backupjobs.snapshots
files = blockstore.backupstore.files
blocks = blockstore[str(jobId) + "_A"].blocks

def compress(data):
    buf = StringIO()
    zipper = gzip.GzipFile(mode='wb', fileobj=buf)
    zipper.write(data)
    zipper.close()
    buf.seek(0)

    return buf.read()

def sha256(data):
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()

BLOCKSTORE = True
s3Bucket = None
if BLOCKSTORE == False:
    s3Bucket = boto3.resource('s3').Bucket("mms-backup-test")

zero_block_compressed = compress('\x00' * blocksize)
zero_block_id = sha256('\x00' * blocksize)
zero_block = {"_id": zero_block_id,
              "zippedSize": len(zero_block_compressed),
              "size": blocksize,
              "bytes": Binary(zero_block_compressed)}
if BLOCKSTORE:
    try:
        blocks.insert(zero_block, continue_on_error=True)
    except DuplicateKeyError:
        pass
else:
    key = str(jobId)[::-1] + "_A/" + zero_block_id
    s3Bucket.put_object(Key=key, Body=bson.BSON.encode(zero_block))

def is_zero_block(data):
    return data.count('\x00') == len(data)

def save_file(filename):
    reader = open(dir_to_snapshot + filename)
    filesize = 0
    block = reader.read(blocksize)
    blockData = []
    while block:
        filesize += len(block)
        if is_zero_block(block):
            blockData.append({"hash": zero_block_id,
                              "size": len(zero_block_compressed)})
            block = reader.read(blocksize)
            continue

        _id = sha256(block)
        zipped = compress(block)
        blockData.append({"hash": _id, "size": len(zipped)})
        to_insert = {"_id": _id,
                     "zippedSize": len(zipped),
                     "size": len(block),
                     "bytes": Binary(zipped)}

        if BLOCKSTORE:
            try:
                blocks.insert(to_insert, continue_on_error=True)
            except DuplicateKeyError:
                pass
        else:
            key = str(jobId)[::-1] + "_A/" + _id
            s3Bucket.put_object(Key=key, Body=bson.BSON.encode(to_insert))

        block = reader.read(blocksize)

    file_id = ObjectId()
    files.insert({"_id": file_id,
                  "filename": filename,
                  "size": filesize,
                  "blockSize": blocksize,
                  "blockstoreDBRoot": str(jobId),
                  "phase": "A",
                  "blocks": blockData,
                  "backingFileObj": None})
    print '\t' + filename + ': ' + str(file_id)
    return file_id

data_file_re = re.compile("\.\d+$")
snapshot_files = {}
for filename in os.listdir(dir_to_snapshot):
    if filename.startswith("local."):
        continue

    if filename.endswith(".ns") or data_file_re.findall(filename):
        snapshot_files[filename.replace('.', ' ')] = {"fileId": save_file(filename)}

ssType = "blockstore"
ssId = "blockstore1"
if BLOCKSTORE == False:
    ssType = "S3"
    ssId = "s3blockstore"

snapshots.insert({"_id": snapshot_id,
                  "completed": True,
                  "deleteAt": Timestamp(int(time.time()) + (365 * 86400), 1),
                  "groupId": groupId,
                  "jobId": jobId,
                  "snapshotStoreType": ssType,
                  "snapshotStoreId": ssId,
                  "timestamp": Timestamp(int(time.time()), 1),
                  "files": snapshot_files,
                  "storageEngine": "mmapv1",
                  "deleted": False,
                  "blockstoreId": "blockstore1",
                  "rsId": "rs0",
                  "lastOplog": Timestamp(int(time.time()), 1)})
