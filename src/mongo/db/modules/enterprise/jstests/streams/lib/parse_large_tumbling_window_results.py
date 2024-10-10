# This script organizes the stats and heap profile information logged by the largeTumblingWindow test in checkpoint.js.
# It prints results to stdout.
# Run it like: python parse_large_tumbling_window_results.py {output of checkpoint.js} >> results.json

import json
import re
import sys


# Sorts a dictionary of stacks by activeBytes.
def sort_stacks(stacks):
    def compare_stack(s):
        b = s[1]["activeBytes"]
        if type(b) == int:
            return b
        elif type(b) == dict:
            return int(b["$numberLong"])
        else:
            raise RuntimeError(f"Unexpected {s}")

    l = list(stacks.items())
    l.sort(key=compare_stack, reverse=True)
    return l


# Finds the stack traces from logs based on the stack_name.
def find_stack_trace(stack_name, stack_traces):
    for trace in stack_traces:
        num = "stack" + str(trace["stackNum"])
        if num == stack_name:
            return trace["stackObj"]
    raise RuntimeError(f"trace was none for {stack_name}")


# Puts together the stack information from runCommand({serverStatus: 1}).heapProfile and the stack traces printed in the log.
def join_stack_information(heap_stacks, stack_traces):
    results = []
    for stack in heap_stacks:
        stack_name = stack[0]
        active_bytes = stack[1]["activeBytes"]
        trace = find_stack_trace(stack_name, stack_traces)
        result = {"stack_name": stack_name, "active_bytes": active_bytes, "trace": trace}
        results.append(result)
    return results


# Parse the input log file. The input file should be the output of checkpoint.js
input_file = sys.argv[1]
results = []
stack_traces = []
with open(input_file) as file:
    for line in file:
        if "opName" in line and "innerStopStartHoppingWindowTest" in line:
            result = json.loads(re.sub(r".*\[jsTest\] ", "", line))
            result["heapProfileBeforeCheckpoint"]["stacks"] = sort_stacks(
                result["heapProfileBeforeCheckpoint"]["stacks"]
            )
            result["heapProfileAfterCheckpoint"]["stacks"] = sort_stacks(
                result["heapProfileAfterCheckpoint"]["stacks"]
            )
            result["heapProfileAfterWindowClose"]["stacks"] = sort_stacks(
                result["heapProfileAfterWindowClose"]["stacks"]
            )
            results.append(result)

        if "heapProfile stack" in line:
            line = line[line.index("{") :]
            stack = json.loads(line)
            stack_traces.append(
                {"stackNum": stack["attr"]["stackNum"], "stackObj": stack["attr"]["stackObj"]}
            )

for idx, result in enumerate(results):
    result["heapProfileBeforeCheckpoint"]["stacks"] = join_stack_information(
        result["heapProfileBeforeCheckpoint"]["stacks"],
        stack_traces,
    )
    result["heapProfileAfterCheckpoint"]["stacks"] = join_stack_information(
        result["heapProfileAfterCheckpoint"]["stacks"],
        stack_traces,
    )
    result["heapProfileAfterWindowClose"]["stacks"] = join_stack_information(
        result["heapProfileAfterWindowClose"]["stacks"],
        stack_traces,
    )

# Print the results json to stdout.
print(json.dumps(results, indent=2))
