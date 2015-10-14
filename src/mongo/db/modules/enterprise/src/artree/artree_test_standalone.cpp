//@file art_tree_test.cpp
/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

//
// Some of this is derived code.  The original code is in the public domain:
//    ARTful5: Adaptive Radix Trie key-value store
//    Author: Karl Malbrain, malbrain@cal.berkeley.edu
//    Date:   13 JAN 15
//

#pragma warning(disable : 4267)
#pragma warning(disable : 4244)

#ifndef STANDALONE
#include "mongo/base/status.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/unittest/unittest.h"
#endif

#include "artree.h"
#include "artree_common.h"
#include "artree_cursor.h"
#include "artree_index.h"
#include "artree_iterator.h"
#include "artree_records.h"
#include "artree_util.h"

#ifndef _WIN32
#include <pthread.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#define srandom srand
#define random rand
#endif

#include <stdint.h>
#include <limits.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>
#include <string.h>

using namespace std;

namespace mongo {

class ArtTreeTestDriver {
public:
    double getCpuTime(int type) {
#ifndef _WIN32
        struct rusage used[1];
        struct timeval tv[1];

        switch (type) {
            case 0: {
                gettimeofday(tv, nullptr);
                return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;
            }
            case 1: {
                getrusage(RUSAGE_SELF, used);
                return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;
            }
            case 2: {
                getrusage(RUSAGE_SELF, used);
                return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
            }
        }
#else
#endif
        return 0;
    }

    //
    //  count the number of keys stored in the ART
    //
    static uint64_t count(ARTree* art, ARTSlot* slot) {
        switch (slot->type) {
            case SpanNode: {
                ARTspan* spanNode = (ARTspan*)(art->arenaSlotAddr(slot));
                uint64_t children = count(art, spanNode->next);
                return children;
            }
            case Array4: {
                ARTnode4* radix4Node = (ARTnode4*)(art->arenaSlotAddr(slot));
                uint64_t children = 0;

                for (uint32_t idx = 0; idx < 4; idx++) {
                    if (radix4Node->alloc & (1 << idx))
                        children += count(art, radix4Node->radix + idx);
                }

                return children;
            }
            case Array14: {
                ARTnode14* radix14Node = (ARTnode14*)(art->arenaSlotAddr(slot));
                uint64_t children = 0;

                for (uint32_t idx = 0; idx < 14; idx++) {
                    if (radix14Node->alloc & (1 << idx))
                        children += count(art, radix14Node->radix + idx);
                }

                return children;
            }
            case Array64: {
                ARTnode64* radix64Node = (ARTnode64*)(art->arenaSlotAddr(slot));
                uint64_t children = 0;

                for (uint32_t idx = 0; idx < 64; idx++) {
                    if (radix64Node->alloc & (1ULL << idx))
                        children += count(art, radix64Node->radix + idx);
                }

                return children;
            }
            case Array256: {
                ARTnode256* radix256Node = (ARTnode256*)(art->arenaSlotAddr(slot));
                uint64_t children = 0;

                for (uint32_t idx = 0; idx < 256; idx++) {
                    if (radix256Node->alloc[idx / 64] & (1ULL << (idx % 64)))
                        children += count(art, radix256Node->radix + idx);
                }

                return children;
            }
            case EndKey:
                return 1;

        }  // end switch

        return 0;
    }

#ifndef _WIN32
    void printRUsage(ARTree* art) {
        struct rusage used[1];
        getrusage(RUSAGE_SELF, used);

        cerr << "\nProcess resource usage:"
             << "\nARTful trie node set size = " << art->_arena_next / 1024
             << "\nARTful record array  size = " << *art->_arenaRec * sizeof(ARTRecord) / 1024
             << "\nmaximum resident set size = " << used->ru_maxrss
             //    << "\nintegral shared memory size = " << used->ru_ixrss
             //    << "\nintegral unshared data size = " << used->ru_idrss
             //    << "\nintegral unshared stack size = " << used->ru_isrss
             << "\npage reclaims (soft page faults) = " << used->ru_minflt
             << "\npage faults (hard page faults) = " << used->ru_majflt
             << "\nswaps = " << used->ru_nswap
             //    << "\nblock input operations = " << used->ru_inblock
             //    << "\nblock output operations = " << used->ru_oublock
             //    << "\nIPC messages sent = " << used->ru_msgsnd
             //    << "\nIPC messages received = " << used->ru_msgrcv
             //    << "\nsignals received = " << used->ru_nsignals
             << "\nvoluntary context switches = " << used->ru_nvcsw
             << "\ninvoluntary context switches = " << used->ru_nivcsw << endl;
    }
#endif

    typedef struct {
        uint32_t idx;
        uint32_t max;
        uint32_t cycle;
        bool sparse;
        char* type;
        char* infile;
        ARTree* art;
        ARTreeIndex* index;
    } ThreadArg;

//
// thread callback
//
#ifndef _WIN32
    static void* indexOp(void* arg) {
#else
    static uint32_t indexOp(void* arg) {
#endif

        const char* base64 =
            "0123456789`[];',)@#$%^&*(~{}:<>"
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-"
            "_=+|?/ ";
        ThreadArg* args = (ThreadArg*)arg;
        int type = args->type[1] | 0x20;
        char key[ARTmaxkey];
        int next[1];

#ifdef LINUX
        char state[64];
        struct random_data buf[1];
#endif

        ARTree* art = args->art;

        ARTreeCursor* cursor = ARTreeCursor::newCursor(args->index, 1024);
        switch (args->type[0] | 0x20) {
            case 'c': {  // count keys
                if (args->idx)
                    break;
                fprintf(stderr, "started counting\n");
                uint64_t found = count(args->art, args->index->_root);
                fprintf(stderr, "cycle %d finished counting, found %ld keys\n", args->cycle, found);
                break;
            }
            case 'g': {  // generate pennysort random files
                uint32_t size = atoi(args->infile);
                if (args->idx)
                    break;

#ifdef LINUX
                memset(buf, 0, sizeof(buf));
                memset(state, 0, sizeof(state));
                initstate_r(time(nullptr), state, 64, buf);
#else
                srandom(time(nullptr));
#endif

                memcpy(key + 10,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 90,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 170,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 250,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 330,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 410,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 490,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 570,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 650,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 730,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 810,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 890,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);
                memcpy(key + 970,
                       "This is a record that is very very long for testing "
                       "purposes it is 80 characters",
                       80);

                for (uint32_t line = 0; line < size; line++) {
#ifdef LINUX
                    random_r(buf, next);
#else
                    *next = random();
#endif
                    key[9] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[8] = base64[next[0] % 91];
                    next[0] >>= 8;
#ifdef LINUX
                    random_r(buf, next);
#else
                    *next = random();
#endif
                    key[7] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[6] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[5] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[4] = base64[next[0] % 91];
#ifdef LINUX
                    random_r(buf, next);
#else
                    *next = random();
#endif
                    key[3] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[2] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[1] = base64[next[0] % 91];
                    next[0] >>= 8;
                    key[0] = base64[next[0] % 91];

                    key[256 - 48] = 0x0a;
                    fwrite(key, 256 - 48 + 1, 1, stdout);
                }

                fprintf(stderr, "finished generating %d pennysort records\n", size);
                break;
            }
            case '4': {  // 4 byte random keys
                if (type == 'd') {
                    fprintf(stderr, "started 4 byte random delete for %s\n", args->infile);
                } else if (type == 'i') {
                    fprintf(stderr, "started 4 byte random insert for %s\n", args->infile);
                } else if (type == 'f') {
                    fprintf(stderr, "started 4 byte random find for %s\n", args->infile);
                }
                uint32_t size = strtoul(args->infile, nullptr, 10);

#ifdef LINUX
                memset(buf, 0, sizeof(buf));
                memset(state, 0, sizeof(state));
                initstate_r(args->idx * 100 + 100, state, 64, buf);
#else
                srandom(args->idx * 100 + 100);
#endif

                uint64_t found = 0UL;
                for (uint32_t line = 0; line < size; line++) {
                    if (args->sparse)
                        random_r(buf, next);
                    else
                        *next = line;

                    key[3] = next[0];
                    next[0] >>= 8;
                    key[2] = next[0];
                    next[0] >>= 8;
                    key[1] = next[0];
                    next[0] >>= 8;
                    key[0] = next[0];

                    if (type == 'i') {
                        int64_t recId = art->storeRec(args->idx, (uint8_t*)key, 4);
                        args->index->insertDocKey(
                            args->idx, (uint8_t*)key, 4, true, recId, nullptr, 0);
                        art->assignTxnId(recId);
                        continue;
                    }

                    if (type == 'f') {
                        if (cursor->findDocKey((uint8_t*)key, 4, line + 1, 1))
                            found++;
                        continue;
                    }

                    if (type == 'd') {
                        if (args->index->deleteDocKey(args->idx, (uint8_t*)key, 4, line + 1))
                            found++;
                        continue;
                    }
                }

                fprintf(stderr, "finished %d keys, found %ld\n", size, found);
                break;
            }

#ifdef LINUX
            case '8': {  // 8 byte random keys of random length
                if (type == 'd') {
                    fprintf(stderr, "started 8 byte random delete for %s\n", args->infile);
                } else if (type == 'i') {
                    fprintf(stderr, "started 8 byte random insert for %s\n", args->infile);
                } else if (type == 'f') {
                    fprintf(stderr, "started 8 byte random find for %s\n", args->infile);
                }
                uint32_t size = strtoul(args->infile, nullptr, 10);
                memset(buf, 0, sizeof(buf));
                memset(state, 0, sizeof(state));
                initstate_r(args->idx * 100 + args->cycle, state, 64, buf);

                uint64_t found = 0UL;
                for (uint32_t line = 0; line < size; line++) {
                    random_r(buf, next);

                    key[0] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[1] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[2] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[3] = (next[0] & 0x7f) + 32;

                    random_r(buf, next);

                    key[4] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[5] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[6] = (next[0] & 0x7f) + 32;
                    next[0] >>= 8;
                    key[7] = (next[0] & 0x7f) + 32;

                    uint32_t keylen = (line % 4) + 5;
                    key[keylen++] = 0x04;

                    if (type == 'i') {
                        //                        int64_t recId = art->storeRec( args->idx,
                        //                        (uint8_t *)key, kenlen );
                        //                    	if( args->index->insertDocKey(
                        //                    args->idx, (uint8_t *)key, keylen, true, recId,
                        //                    nullptr, 0 )
                        //                          found++;
                        if (args->index->insertDocKey(
                                args->idx, (uint8_t*)key, keylen, true, line + 1, nullptr, 0))
                            found++;
                        //						art->assignTxnId( recId
                        //);
                        continue;
                    }

                    if (type == 'd') {
                        if (args->index->deleteDocKey(args->idx, (uint8_t*)key, keylen, line + 1))
                            found++;
                        continue;
                    }

                    if (type == 'f') {
                        if (cursor->findDocKey((uint8_t*)key, keylen, line + 1, 1))
                            found++;
                        continue;
                    }
                }

                fprintf(stderr, "finished %d keys, found %ld\n", size, found);
                break;
            }
#endif
            case 'p': {  // operate on pennysort records
                if (type == 'd') {
                    fprintf(stderr, "started pennysort delete for %s\n", args->infile);
                } else if (type == 'i') {
                    fprintf(stderr, "started pennysort insert for %s\n", args->infile);
                } else if (type == 'f') {
                    fprintf(stderr, "started pennysort find for %s\n", args->infile);
                }

                uint64_t found = 0;
                uint32_t line = 0;
                FILE* in;
                int len;

                if ((in = fopen(args->infile, "rb")))
#ifdef LINUX
                    while (fgets_unlocked(key, sizeof(key), in)) {
#else
                    while (fgets(key, sizeof(key), in)) {
#endif
                        line++;
                        len = strlen(key);
                        key[--len] = 0;  // remove new-line

                        if (type == 'i') {
                            int64_t recId = art->storeRec(args->idx, (uint8_t*)key, len);
                            if (!args->index->insertDocKey(
                                    args->idx, (uint8_t*)key, 10, false, recId, nullptr, 0))
                                found++;
                            art->assignTxnId(recId);
                            continue;
                        }

                        if (type == 'd') {
                            if (args->index->deleteDocKey(args->idx, (uint8_t*)key, 10, line))
                                found++;

                            art->deleteRec(args->idx, line);
                            continue;
                        }

                        if (type == 'f') {
                            if (cursor->findDocKey((uint8_t*)key, 10, line, 1))
                                found++;
                            continue;
                        }
                    }

                fprintf(stderr, "finished %s for %d keys, found %ld\n", args->infile, line, found);
                break;
            }
            case 'k': {  // operate on keys
                if (type == 'd') {
                    fprintf(stderr, "started key delete for %s\n", args->infile);
                } else if (type == 'i') {
                    fprintf(stderr, "started key insert for %s\n", args->infile);
                } else if (type == 'f') {
                    fprintf(stderr, "started key find for %s\n", args->infile);
                }

                uint64_t found = 0;
                uint64_t ts = 1;
                uint32_t line = 0;
                int len = 0;
                FILE* in;

                if ((in = fopen(args->infile, "r")))
#ifdef LINUX
                    while (fgets_unlocked(key, sizeof(key), in)) {
#else
                    while (fgets(key, sizeof(key), in)) {
#endif
                        len = strlen(key);
                        key[--len] = 0;
                        line++;
                        if (type == 'i') {
                            if (args->index->insertDocKey(
                                    args->idx, (uint8_t*)key, len, true, line, nullptr, 0))
                                found++;
                            continue;
                        }

                        if (type == 'f') {
                            if (cursor->findDocKey((uint8_t*)key, len, line, 1))
                                found++;
                            continue;
                        }

                        if (type == 'd') {
                            if (args->index->deleteDocKey(args->idx, (uint8_t*)key, len, line))
                                found++;
                            continue;
                        }
                    }

                fprintf(stderr, "finished %s for %d keys, found %ld\n", args->infile, line, found);
                break;
            }
            case 'f': {  // iterate records and find keys
                fprintf(stderr, "started find %c scan %d\n", type, args->idx);
                ARTreeIterator* it = new ARTreeIterator(art, 0xffffffffffffffff, true);
                uint64_t found = 0;
                uint32_t ksize = 10;
                uint32_t line = 0;

                int64_t recId;

                while (recId = it->next()) {
                    if (it->eof())
                        break;
                    line++;
                    ARTDocument* doc = art->fetchDoc(recId);
                    if (type != 'p')
                        ksize = doc->_docLen;

                    if (args->idx == recId % args->max) {
                        if (cursor->findDocKey(doc->_document, ksize, recId, true))
                            found++;
                    }
                }

                fprintf(stderr,
                        "finished finding %d %c keys, found %ld\n",
                        line / args->max,
                        type,
                        found);
                break;
            }
            case 'd': {  // iterate records and delete keys
                fprintf(stderr, "started delete %c scan %d\n", type, args->idx);
                ARTreeIterator* it = new ARTreeIterator(art, 0xffffffffffffffff, true);
                uint64_t found = 0;
                uint32_t ksize = 10;
                uint32_t line = 0;
                int64_t recId;

                while (recId = it->next()) {
                    if (it->eof())
                        break;
                    line++;
                    ARTDocument* doc = art->fetchDoc(recId);
                    if (type != 'p')
                        ksize = doc->_docLen;
                    if (args->idx == recId % args->max) {
                        if (args->index->deleteDocKey(args->idx, doc->_document, ksize, recId))
                            found++;
                        art->deleteRec(args->idx, recId);
                        // reclaim old record:
                        //   place the old record on the tail of the waiting recId frame
                        ARTSlot slot[1];
                        slot->bits = 0;
                        slot->off = recId;
                        art->addSlotToFrame(
                            &art->_headRecId[args->idx], &art->_tailRecId[args->idx], slot);
                    }
                }

                fprintf(stderr,
                        "finished deleting %d %c keys, found %ld\n",
                        line / args->max,
                        type,
                        found);
                break;
            }
            case 's': {  // scan keys
                if (args->idx)
                    break;
                fprintf(stderr, "started forward scan\n");
                uint32_t cnt = 0;

                cursor->resetCursor();

                while (cursor->nextKey(1)) {
                    ARTDocument* doc = args->index->_art->fetchDoc(cursor->_recordId);

                    fwrite(doc->_document, doc->_docLen, 1, stdout);

                    fputc('\n', stdout);
                    cnt++;
                }

                fprintf(stderr, " Total keys scanned %d\n", cnt);
                break;
            }
            case 'r': {  // reverse scan keys
                if (args->idx)
                    break;
                fprintf(stderr, "started reverse scan\n");
                uint32_t cnt = 0;

                cursor->resetCursor();

                while (cursor->prevKey(1)) {
                    ARTDocument* doc = args->index->_art->fetchDoc(cursor->_recordId);
                    fwrite(doc->_document, doc->_docLen, 1, stdout);

                    fputc('\n', stdout);
                    cnt++;
                }

                fprintf(stderr, " Total keys read %d\n", cnt);
                break;
            }
        }  // end switch

        ARTreeCursor::endCursor(cursor);
#ifndef _WIN32
        return nullptr;
#else
        return false;
#endif
    }

    typedef struct timeval timer;

    static bool samePlace(ARTreeCursor* cursor, ARTreeCursor* cursor2) {
        if (cursor->_atEOF && cursor2->_atEOF)
            return true;
        if (cursor->_atEOF || cursor2->_atEOF)
            return false;
        if (cursor->_recordId != cursor2->_recordId)
            return false;
        return true;
    }

    //
    // common code for unit test blocks and main().
    // For main(), parameters are parsed from the command line;
    // for unit tests, the parameters are hard-wired.
    //
    int drive(ARTree* art,  // radix index
              ARTreeIndex* index,
              bool sparse,
              uint32_t cycle,
              const std::vector<std::string>& cmdv,  // cmds: w/r/s
              const std::vector<std::string>& srcv)  // key file names
    {
        std::cerr << "cmdv.size() = " << cmdv.size() << std::endl;
        std::cerr << "srcv.size() = " << srcv.size() << std::endl;

        double start[3];
        uint32_t cnt = srcv.size();
#ifndef _WIN32
        ThreadArg args[cnt];
        memset(args, 0, sizeof(ThreadArg) * cnt);
        pthread_t threads[cnt];
#else
        ThreadArg args[15];
        memset(args, 0, sizeof(args));
        HANDLE* threads = (HANDLE*)GlobalAlloc(GMEM_FIXED | GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif

        // start thread runs
        for (uint32_t run = 0; run < cmdv.size(); ++run) {
            cerr << ">>Run: " << cmdv[run] << "<<" << endl;
            start[0] = getCpuTime(0);
            start[1] = getCpuTime(1);
            start[2] = getCpuTime(2);

            if (cnt > 1) {
                for (uint32_t i = 0; i < cnt; ++i) {
                    args[i].infile = (char*)srcv[i].c_str();
                    args[i].type = (char*)cmdv[run].c_str();
                    args[i].index = index;
                    args[i].sparse = sparse;
                    args[i].art = art;
                    args[i].cycle = cycle;
                    args[i].max = cnt;
                    args[i].idx = i;
#ifndef _WIN32
                    int err = pthread_create(
                        &threads[i], nullptr, ArtTreeTestDriver::indexOp, (void*)&args[i]);
                    if (err) {
#ifndef STANDALONE
                        return int(ErrorCodes::InternalError, "Error creating thread");
#else
                        return -1;
#endif
                    }
#else
                    threads[i] = (HANDLE)_beginthreadex(
                        nullptr, 131072, ArtTreeTestDriver::indexOp, args + i, 0, nullptr);
#endif
                }
            } else {
                args[0].infile = (char*)srcv[0].c_str();
                args[0].type = (char*)cmdv[run].c_str();
                args[0].index = index;
                args[0].sparse = sparse;
                args[0].cycle = cycle;
                args[0].art = art;
                args[0].max = cnt;
                args[0].idx = 0;
                indexOp(args);
            }

// wait for termination
#ifndef _WIN32
            if (cnt > 1) {
                for (uint32_t idx = 0; idx < cnt; ++idx) {
                    pthread_join(threads[idx], nullptr);
                }
            }
#else
            if (cnt > 1) {
                WaitForMultipleObjects(cnt, threads, TRUE, INFINITE);
            }

            if (cnt > 1) {
                for (uint32_t idx = 0; idx < cnt; ++idx) {
                    CloseHandle(threads[idx]);
                }
            }
#endif

            float elapsed0 = getCpuTime(0) - start[0];
            float elapsed1 = getCpuTime(1) - start[1];
            float elapsed2 = getCpuTime(2) - start[2];

            std::cerr << " real " << (int)(elapsed0 / 60) << "m "
                      << elapsed0 - (int)(elapsed0 / 60) * 60 << 's' << "\n user "
                      << (int)(elapsed1 / 60) << "m " << elapsed1 - (int)(elapsed1 / 60) * 60 << 's'
                      << "\n sys  " << (int)(elapsed2 / 60) << "m "
                      << elapsed2 - (int)(elapsed2 / 60) * 60 << 's' << std::endl;

#ifndef _WIN32
            printRUsage(art);
#endif
        }  // end thread runs

        return 0;
    }
};

#ifndef STANDALONE

//
//  TESTS
//

TEST(ArtTree, BasicWriteTest) {
    ARTree* art = ARTree::create();
    ArtTreeTestDriver driver;
    std::vector<std::string> cmdv;
    std::vector<std::string> srcv;

    // four insert threads
    // two commands each.

    cmdv.clear();
    srcv.clear();

    cmdv.push_back("4");  //[0]
    cmdv.push_back("x");  //[1]

    srcv.push_back("100000");
    srcv.push_back("100000");
    srcv.push_back("100000");
    srcv.push_back("100000");

    ASSERT_OK(driver.drive(art, cmdv, srcv));
}

#endif

}  // namespace mongo

#ifdef STANDALONE

void usage(const char* arg0) {
    cout << "Usage: " << arg0 << "OPTIONS:\n"
                                 "  -s 1		   - sparse random key files\n"
                                 "  -f fname    - the name of the ARTful tree file\n"
                                 "  -c cmds     - string of:  (w)rite\n"
                                 "                           |(s)can\n"
                                 "                           |(r)everse scan\n"
                                 "                           |(d)elete\n"
                                 "                           |(f)ind\n"
                                 "                           |(p)ennysort\n"
                                 "                           |(c)ount\n"
                                 "                           |(4) bit random key inserts\n"
                                 "                           |(8) bit random key inserts\n,"
                                 "                  executed in sequence across key files\n"
                                 "  -k keyFiles - list of key files, one per thread, per command\n";
}

int main(int argc, char* argv[]) {
    cerr << "sizeof(ARTSlot) = " << sizeof(mongo::ARTSlot) << '\n';

    bool sparse = false;  // sparse random files
    string fname;         // index file name

    mongo::ArtTreeTestDriver driver;

    uint32_t cycle = time(nullptr) / 30;
    vector<string> srcv;  // source files containing keys
    vector<string> cmdv;  // corresponding commands

#ifdef NOGETOPT
    char cmd;
    int i;

    for (i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            cmd = argv[i][1];
            continue;
        }

        switch (cmd) {
            case 's': {
                sparse = true;
                continue;
            }
            case 'f': {
                fname = argv[i];
                continue;
            }
            case 'c': {
                cmdv.push_back(argv[i]);
                continue;
            }
            case 'k': {
                srcv.push_back(argv[i]);
                continue;
            }
            case 'y': {
                cycle = atoi(argv[i]);
                continue;
            }
            case '?': {
                usage(argv[0]);
                return 1;
            }
        }
    }
#else
    opterr = 0;
    char c;

    while ((c = getopt(argc, argv, "y:f:c:k:s:")) != -1) {
        switch (c) {
            case 's': {  // -s sparse
                sparse = atoi(optarg) ? true : false;
                break;
            }
            case 'y':
                cycle = atoi(optarg);
                break;
            case 'f': {  // -f fName
                fname = optarg;
                break;
            }
            case 'c': {  // -c cmd,cmd,...
                char sep[] = ",";
                char* tok = strtok(optarg, sep);
                while (tok) {
                    if (strspn(tok, " \t") == strlen(tok))
                        continue;
                    cmdv.push_back(tok);
                    tok = strtok(0, sep);
                }
                break;
            }
            case 'k': {  // -k keyfile,keyfile,...
                char sep[] = ",";
                char* tok = strtok(optarg, sep);
                while (tok) {
                    if (strspn(tok, " \t") == strlen(tok))
                        continue;
                    srcv.push_back(tok);
                    tok = strtok(0, sep);
                }
                break;
            }
            case '?': {
                usage(argv[0]);
                return 1;
            }
            default:
                exit(-1);
        }
    }
#endif

    mongo::ARTree* art = mongo::ARTree::create();
    mongo::ARTreeIndex* index = new mongo::ARTreeIndex(art);

    if (driver.drive(art, index, sparse, cycle, cmdv, srcv)) {
        cerr << "driver returned error" << endl;
    }
}
#endif
