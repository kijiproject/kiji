#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tests for module base.base.Flags"""

import io
import sys
import tempfile

from base import base
from base import unittest


class TestFlags(unittest.BaseTestCase):
    """Tests for the base module."""

    def setUp(self):
        super().setUp()

        self.flags = base.Flags(name="test flags")
        self.flags.add_integer("int", default=0, help="Some help for int.")
        self.flags.add_boolean("bool", default=False)
        self.flags.add_string("str_value", default="default")
        self.flags.add_list("list", default=["x", "y", "z"], help="Some help for list.")
        self.flags.add_list("delimited-list", default=None, delimiter=',')

        self.child_flags = base.Flags(name="child test flags", parent=self.flags)
        self.child_flags.add_boolean("child_bool", default=True)
        self.child_flags.add_integer("child_int", default=0)
        self.child_flags.add_list("child_list", default=None)

    def test_cannot_add_already_existing_flag(self):
        """Should not be able to add a flag that already exists."""
        self.assertRaises(AssertionError, self.flags.add_string, "int")

    def test_cannot_add_parent_flag_to_child(self):
        """Should not be able to add a flag to a child that already exists in parent."""
        self.assertRaises(AssertionError, self.child_flags.add_string, "str_value")

    def test_defaults(self):
        """Flags parameters not parsed should return default values."""
        self.flags.parse("".split())
        self.assertEqual(0, self.flags.int)
        self.assertEqual(False, self.flags.bool)
        self.assertEqual("default", self.flags.str_value)
        self.assertListEqual(["x", "y", "z"], self.flags.list)
        self.assertIsNone(self.flags.delimited_list)

    def test_child_params(self):
        """Flags parameters not on child should be retrieved from parent."""
        self.flags.parse("".split())
        self.child_flags.parse("".split())
        self.assertEqual(0, self.child_flags.int)
        self.assertEqual(False, self.child_flags.bool)
        self.assertEqual("default", self.child_flags.str_value)
        self.assertListEqual(["x", "y", "z"], self.child_flags.list)
        self.assertIsNone(self.child_flags.delimited_list)
        self.assertEqual(True, self.child_flags.child_bool)

    def test_get(self):
        """Flags should support get for string lookup of parameters that have been registered
        with the Flags.  Parameters that have been parsed should return their parsed value.
        Unparsed parameters should return their default.  Unknown parameters should return the
        the default second argument to get if provided, or UNDEFINED if default is not specified.
        """
        self.flags.parse("--int=9".split())
        self.assertEqual(9, self.flags.get("int", None))
        self.assertEqual("default", self.flags.get("str_value", None))
        self.assertIsNone(self.flags.get("unknown", None))
        self.assertIs(base.UNDEFINED, self.flags.get("unknown"))

    def test_child_get(self):
        """Get should look up values in Flags and then its parent, if available."""
        self.flags.parse("--int=9".split())
        self.child_flags.parse("--child-bool=False".split())
        self.assertEqual(9, self.child_flags.get("int", None))
        self.assertEqual("default", self.child_flags.get("str_value", None))
        self.assertEqual(False, self.child_flags.get("child_bool"))
        self.assertIsNone(self.child_flags.get("unknown", None))
        self.assertIs(base.UNDEFINED, self.child_flags.get("unknown"))
        # child flag is only on child_flags, not flags.
        self.assertIs(base.UNDEFINED, self.flags.get("child_bool"))

    def test_getitem(self):
        """Flags registered with instance should be available through [] op."""
        self.flags.parse("--int=9".split())
        self.assertEqual(9, self.flags["int"])
        self.assertEqual("default", self.flags["str_value"])
        self.assertRaises(KeyError, lambda: self.flags["unknown"])

    def test_child_getitem(self):
        """Flags registered with instance or its parent should be available through [] op."""
        self.flags.parse("--int=9 --child-bool=False".split())
        self.child_flags.parse(self.flags.get_unparsed())

        self.assertEqual(9, self.child_flags["int"])
        self.assertEqual(["x", "y", "z"], self.child_flags["list"])
        self.assertEqual("default", self.child_flags["str_value"])
        self.assertEqual(False, self.child_flags["child_bool"])
        self.assertRaises(KeyError, lambda: self.child_flags["unknown"])
        # child flag is only on child_flags, not flags.
        self.assertRaises(KeyError, lambda: self.flags["child_bool"])

    def test_parse(self):
        """Flags should return parsed values from command-line arguments for each type of
        parameter.
        """
        self.flags.parse(
            "--int=9 --bool=yes --str-value=string "
            "--list=a,b --list=c,d "
            "--delimited-list=a,b --delimited-list=c,d"
            .split()
        )
        self.assertTupleEqual(tuple(), self.flags.get_unparsed())
        self.assertEqual(9, self.flags.int)
        self.assertEqual(True, self.flags.bool)
        self.assertEqual("string", self.flags.str_value)
        self.assertListEqual(["a,b", "c,d"], self.flags.list)
        self.assertListEqual(["a", "b", "c", "d"], self.flags.delimited_list)

    def test_child_parse(self):
        """Flags.get_unparsed() from flags should be usable by another flags.parse."""
        self.flags.parse("--child_bool=False --child-int=1 --child-list=a --child-list=b".split())
        self.child_flags.parse(self.flags.get_unparsed())

        self.assertTupleEqual(tuple(), self.child_flags.get_unparsed())
        self.assertEqual(False, self.child_flags.child_bool)
        self.assertEqual(1, self.child_flags.child_int)
        self.assertListEqual(["a", "b"], self.child_flags.child_list)

    def test_delimited_list_stripping(self):
        """Delimited string list flags should handle stripping spacing between listed values."""
        self.flags.parse(["--delimited_list=\na,\nb\n,,"])
        self.assertTupleEqual(tuple(), self.flags.get_unparsed())
        self.assertListEqual(["a", "b"], self.flags.delimited_list)

    def test_parse_config_uri_parameter(self):
        """Flags should be able to read parameters from a JSON config uri."""
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as f:
            f.write(
                base.json_encode(
                    {
                        "int": "09",
                        "str-value": "string",
                        "bool": "True",
                        "list": ["a", "b", "c"],
                        "delimited-list": "1, 2, 3",
                    },
                    pretty=True))
            f.flush()
            self.flags.parse([], config_uri="file:" + f.name)
        self.assertEqual(9, self.flags.int)
        self.assertEqual(True, self.flags.bool)
        self.assertEqual("string", self.flags.str_value)
        self.assertListEqual(["a", "b", "c"], self.flags.list)
        self.assertListEqual(["1", "2", "3"], self.flags.delimited_list)

    def test_parse_config_uri_parameter_for_child(self):
        """Unparsed flags read in a JSON config uri should be available to child_flags.parse."""
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as f:
            f.write(
                base.json_encode(
                    dict(
                        child_int=1,
                        child_bool=False,
                        child_list=["a", "b", "c"]
                    ),
                    pretty=True))
            f.flush()
            self.flags.parse([], config_uri="file:" + f.name)
        self.child_flags.parse(self.flags.get_unparsed())
        self.assertEqual(1, self.child_flags.child_int)
        self.assertEqual(False, self.child_flags.child_bool)
        self.assertListEqual(["a", "b", "c"], self.child_flags.child_list)

    def test_reset(self):
        """Flags should be able to reset to their unparsed state."""
        self.flags.parse(
            "--int=9 --bool=yes --str-value=string "
            "--list=a,b --list=c,d "
            "--delimited-list=a,b --delimited-list=c,d "
            "--unparsed-int=10"
            .split()
        )
        self.flags.reset()
        self.flags.parse(
            "--str-value=string "
            "--list=a,b "
            "--delimited-list=a,b"
            .split()
        )
        self.assertTupleEqual(tuple(), self.flags.get_unparsed())
        self.assertEqual(0, self.flags.int)
        self.assertEqual(False, self.flags.bool)
        self.assertEqual("string", self.flags.str_value)
        self.assertListEqual(["a,b"], self.flags.list)
        self.assertListEqual(["a", "b"], self.flags.delimited_list)

    def test_help(self):
        """Flags should be able to print the name, data type, and help message for each added
        flag.
        """
        real_stdout = sys.stdout
        try:
            sys.stdout = io.StringIO()
            self.flags.print_usage()
            contents = sys.stdout.getvalue()
            self.assertIn("\n --int: integer = 0\n", contents)
            self.assertIn("Some help for int.\n", contents)
            self.assertIn("\n --bool: boolean = False\n", contents)
            self.assertIn("\n --str-value: string = 'default'\n", contents)
            self.assertIn(
                "\n --list: all occurrences appended to list = {!r}\n".format(["x", "y", "z"]),
                contents
            )
            self.assertIn("Some help for list.\n", contents)
            self.assertIn("\n --delimited-list: comma-delimited list = None\n", contents)
            self.assertIn("\n --config: string = None\n", contents)
            print(contents, file=real_stdout)
        finally:
            sys.stdout = real_stdout

    def test_list_set_flags(self):
        self.flags.parse("--int=9".split())

        self.assertSetEqual(
            frozenset([("int", 9)]),
            frozenset((name, desc.value) for (name, desc) in self.flags.list_set_flags())
        )
        self.assertSetEqual(
            frozenset([("int", 9), ("str_value", "default")]),
            frozenset((name, desc.value) for (name, desc) in
                      self.flags.list_set_flags(include=("str_value",)))
        )
        self.assertSetEqual(
            frozenset([("str_value", "default")]),
            frozenset((name, desc.value) for (name, desc) in
                      self.flags.list_set_flags(include=("str_value",), exclude=("int",)))
        )

    def test_config_uri_in_command_line_args(self):
        """Flags should recognize --config-uri flag to read additional parameters from a JSON
        dictionary at the given uri. Parameters on the command-line argument list
        before the --config-uri flag are overridden by values in the --config-uri JSON.
        Parameters after the --config-uri flag override values from the JSON config.
        """
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as fp:
            config_parameters = dict(
                int="9",
                bool="yes",
                str_value="string",
                unparsed="foo",
                delimited_list=["a", "b"],
                list="3",
            )
            fp.write(base.json_encode(config_parameters, pretty=True))
            fp.flush()

            self.flags.parse(
                ("--int=7 --list=1 --list=2 --config=file:%s --bool=no "
                 "--delimited_list=c,d " % fp.name)
                .split()
            )
            self.assertEqual(9, self.flags.int)
            self.assertEqual(False, self.flags.bool)
            self.assertEqual("string", self.flags.str_value)
            self.assertListEqual(["1", "2", "3"], self.flags.list)
            self.assertListEqual(["a", "b", "c", "d"], self.flags.delimited_list)
            self.assertGreater(
                len(self.flags.get_unparsed()),
                0,
                "There should still be unparsed data from the config file: {}".format(
                    self.flags.get_unparsed()
                )
            )

    def test_config_uri_in_command_line_args_for_child(self):
        """Unparsed flags on the command line and config file should be passed to the child
        flags object in their original order.
        """
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as fp:
            config_parameters = dict(
                child_int="9",
                child_list=["b", "c"],
            )
            fp.write(base.json_encode(config_parameters, pretty=True))
            fp.flush()

            self.flags.parse(
                (
                    "--child-int=8 --child-list=a --config=file:%s --child_bool=no --child-list=d"
                    % fp.name
                ).split()
            )
            self.child_flags.parse(self.flags.get_unparsed())
            self.assertEqual(9, self.child_flags.child_int)
            self.assertEqual(False, self.child_flags.child_bool)
            self.assertListEqual(["a", "b", "c", "d"], self.child_flags.child_list)

    def test_skip_parse(self):
        """All arguments in command line argument list should be skipped after the first occurrence
        of the string '--'.
        """
        self.flags.parse("--int=7 -- --str_value=skipped -- -- abcd".split())
        self.assertEqual(7, self.flags.int)
        self.assertEqual("default", self.flags.str_value)
        self.assertTupleEqual(
            ("--str_value=skipped", "--", "--", "abcd"),
            self.flags.get_unparsed()
        )

    def test_contains(self):
        """Flags registered with instance or its parent should be contained in the Flags."""
        self.assertIn("int", self.flags)
        self.assertIn("str_value", self.flags)
        self.assertIn("list", self.flags)
        self.assertNotIn("child_bool", self.flags)
        self.assertNotIn("unknown", self.flags)

        self.assertIn("int", self.child_flags)
        self.assertIn("str_value", self.child_flags)
        self.assertIn("list", self.child_flags)
        self.assertIn("child_bool", self.child_flags)
        self.assertNotIn("unknown", self.child_flags)

    def test_iter(self):
        """iter should return all the registered flags in the Flags instance and its parent(s)."""
        self.assertSetEqual(
            {"int", "bool", "str_value", "list", "delimited_list"},
            set(iter(self.flags))
        )
        self.assertSetEqual(
            {
                "child_bool",
                "child_list",
                "child_int",
                "int",
                "bool",
                "str_value",
                "list",
                "delimited_list"
            },
            set(iter(self.child_flags))
        )

    def test_get_unparsed(self):
        """get_unparsed() should return string representation of arguments."""
        with tempfile.NamedTemporaryFile(mode="wt", encoding="utf-8") as fp:
            config_parameters = {
                "unparsed_str": "abc",
                "unparsed-list": ["a", "b", "c"],
            }
            fp.write(base.json_encode(config_parameters, pretty=True))
            fp.flush()
            self.flags.parse(
                ("unparsed --config=file:%s --unparsed-int=1" % fp.name).split()
            )
        unparsed = self.flags.get_unparsed()
        self.assertEqual("unparsed", unparsed[0])
        self.assertEqual("--unparsed-int=1", unparsed[-1])
        self.assertGreater(
            len(unparsed),
            2,
            "there should be at least one command-line argument for missing value from config: {}"
            .format(unparsed)
        )


if __name__ == '__main__':
    base.run(unittest.base_main)
