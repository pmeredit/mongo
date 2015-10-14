//@file artree_debug.cpp

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

#include "artree_debug.h"

#ifndef _WIN32
#include <cxxabi.h>
#include <execinfo.h>
#endif

#include <stdint.h>
#include <cstdlib>
#include <iostream>
#include <memory>

namespace mongo {

#ifndef _WIN32
void ARTreeDebug::backtrace(uint32_t maxFrames) {
    std::cout << "\n\nStack trace:" << std::endl;

    void* addrlist[maxFrames + 1];
    int addrlen = ::backtrace(addrlist, sizeof(addrlist) / sizeof(void*));
    if (addrlen == 0) {
        std::cout << "  <empty, possibly corrupt>" << std::endl;
        return;
    }
    char** symbollist = ::backtrace_symbols(addrlist, addrlen);

    size_t funcnamesize = 1024;
    char* funcname = (char*)malloc(funcnamesize);

    for (int i = 1; i < addrlen; i++) {
        std::string p(symbollist[i]);

        std::size_t a = p.find("    0x");
        std::size_t b = p.find("_", a);
        std::size_t c = p.find("+", b);

        if (a != std::string::npos && b != std::string::npos && c != std::string::npos) {
            std::string module = p.substr(0, a - 1);
            std::string fname = p.substr(b, c - b - 1);
            std::string offset = p.substr(c + 1);

            int status;
            char* ret = abi::__cxa_demangle(fname.c_str(), funcname, &funcnamesize, &status);
            if (status == 0) {
                funcname = ret;  // use possibly realloc()-ed string
                std::cout << " #" << module << " : " << funcname << " +" << offset << std::endl;
            } else {
                // demangling failed: output as a C function with no arguments.
                std::cout << " #" << module << " : " << fname << "()"
                          << " +" << offset << std::endl;
            }
        } else {  // couldn't parse the line: print the whole line.
            std::cout << "  " << symbollist[i] << std::endl;
        }
    }
    std::cout << std::endl;

    free(funcname);
    free(symbollist);
}
#endif

}  // namespace mongo
