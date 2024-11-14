# Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.

# Run on gdb with 'gdb ./mongo_crypt_shlib_test --batch -ex "source debuggability_test.py"'
import os
import re
import traceback

import gdb

DBGPROG = "mongo_crypt_shlib_test"
LIBNAME = "mongo_crypt_v1.so"
print("Running as PID {}".format(os.getpid()))


def set_breakpoint_and_continue(addr):
    bp = gdb.Breakpoint(addr)
    # older gdb versions don't have the 'pending' attribute, so check first
    if hasattr(bp, "pending"):
        assert not bp.pending, "Failed to set breakpoint on {}".format(bp.location)
    gdb.execute("continue")
    return bp


def assert_at_breakpoint(bp):
    atfunc = gdb.execute("print $pc", False, True)
    matches = re.findall(re.escape(bp.location), atfunc)
    assert matches, "Program counter is not at expected breakpoint: {}".format(bp.location)


def debuggability_test():
    # Verify the correct executable is loaded
    inferiors = gdb.execute("info inferiors", False, True)
    matches = re.findall(DBGPROG, inferiors)
    assert DBGPROG in matches, "Current program is not the expected binary file: {}".format(DBGPROG)

    # Start the program
    gdb.execute("start")

    # Assert the mongo_crypt library is not loaded yet
    shlibs = gdb.execute("info sharedlibrary", False, True)
    matches = re.findall(LIBNAME, shlibs)
    assert LIBNAME not in matches, "Shared library {} is dynamically linked".format(LIBNAME)

    # Continue until 'dlopen'
    bp = set_breakpoint_and_continue("dlopen")
    assert_at_breakpoint(bp)

    # Step over 'dlopen' and verify the library is now loaded
    gdb.execute("finish")
    shlibs = gdb.execute("info sharedlibrary", False, True)
    matches = re.findall(LIBNAME, shlibs)
    assert LIBNAME in matches, "Shared library {} did not get dynamically loaded".format(LIBNAME)

    breaks = [
        "(anonymous namespace)::MongoCryptTest::checkAnalysisSuccess",
        "mongo_crypt_v1_analyze_query",
        "mongo::analyzeQuery",
        "mongo::analyzeNonExplainQuery",
        "mongo::query_analysis::(anonymous namespace)::processQueryCommand",
    ]
    for func in breaks:
        print("Setting breakpoint and continuing to {} ...".format(func))
        bp = set_breakpoint_and_continue(func)
        assert_at_breakpoint(bp)

    # step in processQueryCommand
    print("Stepping into {} ...".format(bp.location))
    gdb.execute("next")
    # break on callback function pointer func
    print("Setting breakpoint and continuing to *func ...")
    bp = set_breakpoint_and_continue("*func")
    print("Stepping into {} ...".format(bp.location))
    gdb.execute("next")

    matches = None
    steps = 0
    while not matches:
        gdb.execute("next")
        atfunc = gdb.execute("p $pc", False, True)
        matches = re.findall("mongo::analyzeQuery", atfunc)
        steps += 1

    print("Stepped over {} lines of code".format(steps))
    assert steps > 40

    # in analyzeQuery lambda, change the output to NULL to force a
    # uassert to throw an exception. Then, verify that handleException
    # catches it.
    gdb.execute("next")
    gdb.execute("next")
    gdb.execute("next")
    gdb.execute("set variable output=0")

    bp = set_breakpoint_and_continue("mongo::handleException<mongo_crypt_v1_error>")
    assert_at_breakpoint(bp)

    gdb.execute("next")
    gdb.execute("next")
    status = gdb.execute("print status.exception_code", False, True)
    print("Got exception code {}".format(status))
    matches = re.findall("= 146", status)
    assert matches, "Failed to get the expected exception code"

    # disable all breakpoints and continue til the end
    gdb.execute("disable")
    gdb.execute("continue")


try:
    gdb.execute("disable pretty-printer")
    debuggability_test()
    print("GDB debuggability test succeeded")
    quit()
except Exception:
    print(traceback.format_exc())
    print("GDB debuggability test failed")
    quit(1)
