#!/usr/bin/env python
# Copyright (c) 2014 Palantir Technologies
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""sqlite3worker test routines."""

__author__ = "Shawn Lee"
__email__ = "shawnl@palantir.com"
__license__ = "MIT"

import os
import tempfile
import time
import unittest

import sqlite3worker


class Sqlite3WorkerTests(unittest.TestCase):  # pylint:disable=R0904
    """Test out the sqlite3worker library."""
    def setUp(self):  # pylint:disable=C0103
        self.tmp_file = tempfile.NamedTemporaryFile(
            suffix="pytest", prefix="sqlite").name
        self.sqlite3worker = sqlite3worker.Sqlite3Worker(self.tmp_file)
        # Create sql db.
        self.sqlite3worker.execute(
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")

    def tearDown(self):  # pylint:disable=C0103
        self.sqlite3worker.close()
        os.unlink(self.tmp_file)

    def test_bad_select(self):
        """Test a bad select query."""
        query = "select THIS IS BAD SQL"
        self.assertEqual(
            self.sqlite3worker.execute(query),
            (
                "Query returned error: select THIS IS BAD SQL: "
                "[]: no such column: THIS"))

    def test_bad_insert(self):
        """Test a bad insert query."""
        query = "insert THIS IS BAD SQL"
        self.sqlite3worker.execute(query)
        # Give it one second to clear the queue.
        if self.sqlite3worker.queue_size != 0:
            time.sleep(1)
        self.assertEqual(self.sqlite3worker.queue_size, 0)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"), [])

    def test_valid_insert(self):
        """Test a valid insert and select statement."""
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow")])
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
        # Give it one second to clear the queue.
        if self.sqlite3worker.queue_size != 0:
            time.sleep(1)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow"), ("2011-02-02 14:14:14", "dog")])


if __name__ == "__main__":
    unittest.main()
