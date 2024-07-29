"""Unit tests for buildscripts/resmokelib/testing/hooks/fuzz_runtime_parameters.py."""

import logging
import os
import unittest
import random
import sys

import mock

from buildscripts.resmokelib import errors
from buildscripts.resmokelib.testing.hooks import fuzz_runtime_parameters as _runtime_fuzzer
from buildscripts.resmokelib.testing.hooks import lifecycle as lifecycle_interface

# pylint: disable=protected-access


class TestRuntimeFuzzGeneration(unittest.TestCase):
    def assert_parameter_values_ok(self, spec, generated_values):
        for name, val in generated_values.items():
            options = spec[name]
            if "isRandomizedChoice" in options:
                lb = options["lower_bound"]
                ub = options["upper_bound"]
                self.assertTrue(lb <= val <= ub)
            elif "choices" in options:
                self.assertIn(val, options["choices"])
            elif "min" and "max" in options:
                self.assertTrue(options["min"] <= val <= options["max"])
            else:
                self.assertIn("default", options)
                self.assertEqual(val, options["default"])

    @mock.patch("buildscripts.resmokelib.testing.hooks.fuzz_runtime_parameters.time.time")
    def test_frequency_respected(self, mock_time):
        start_time = 1625140800
        mock_time.return_value = start_time

        from buildscripts.resmokelib.config_fuzzer_limits import (
            runtime_parameter_fuzzer_params,
        )

        mongod_spec = runtime_parameter_fuzzer_params["mongod"]
        runtimeFuzzerParamState = _runtime_fuzzer.RuntimeParametersState(
            mongod_spec, random.randrange(sys.maxsize)
        )
        # No time has passed; we wouldn't want to set any of these yet.
        ret = runtimeFuzzerParamState.generate_parameters()
        self.assertEqual(ret, {})

        mock_time.return_value = start_time + 1
        ret = runtimeFuzzerParamState.generate_parameters()

        # We should set ingressAdmissionControllerTicketPoolSize now, but not ingressAdmissionControlEnabled or ShardingTaskExecutorPoolMinSize
        param_names_to_set = ret.keys()
        self.assertIn("ingressAdmissionControllerTicketPoolSize", param_names_to_set)
        self.assertNotIn("ingressAdmissionControlEnabled", param_names_to_set)
        self.assertNotIn("ShardingTaskExecutorPoolMinSize", param_names_to_set)
        self.assert_parameter_values_ok(mongod_spec, ret)

        # Don't advance time, and generate the values again. Since no time has passed, nothing should be set.
        ret = runtimeFuzzerParamState.generate_parameters()
        self.assertEqual(ret, {})

        # Now advance the time enough such that ShardingTaskExecutorPoolMinSize should be set also.
        mock_time.return_value = start_time + 5
        ret = runtimeFuzzerParamState.generate_parameters()
        param_names_to_set = ret.keys()
        self.assertIn("ingressAdmissionControllerTicketPoolSize", param_names_to_set)
        self.assertIn("ShardingTaskExecutorPoolMinSize", param_names_to_set)
        self.assertNotIn("ingressAdmissionControlEnabled", param_names_to_set)
        self.assert_parameter_values_ok(mongod_spec, ret)

        # Don't advance time, and generate the values again. Since no time has passed, nothing should be set.
        ret = runtimeFuzzerParamState.generate_parameters()
        self.assertEqual(ret, {})

        # Now advance the time enough such that all 3 should be set.
        mock_time.return_value = start_time + 10
        ret = runtimeFuzzerParamState.generate_parameters()
        param_names_to_set = ret.keys()
        self.assertIn("ingressAdmissionControllerTicketPoolSize", param_names_to_set)
        self.assertIn("ShardingTaskExecutorPoolMinSize", param_names_to_set)
        self.assertIn("ingressAdmissionControlEnabled", param_names_to_set)
        self.assert_parameter_values_ok(mongod_spec, ret)

    def test_runtime_param_spec_validation(self):
        bad_spec_value_not_dict = {"fakeRuntimeParam": 1}
        bad_spec_value_no_period = {"fakeRuntimeParam": {"max": 50, "min": 10}}
        good_spec = {"fakeRuntimeParam": {"max": 50, "min": 10, "period": 5}}

        with self.assertRaises(ValueError):
            _runtime_fuzzer.validate_runtime_parameter_spec(bad_spec_value_not_dict)

        with self.assertRaises(ValueError):
            _runtime_fuzzer.validate_runtime_parameter_spec(bad_spec_value_no_period)
        # No exception for good dict
        _runtime_fuzzer.validate_runtime_parameter_spec(good_spec)
