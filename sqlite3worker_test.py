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

import mox
import os
import sqlite3
import sqlite3worker
import tempfile
import unittest
import Queue

class StubQueue(Queue.Queue):
    """Stub out Queue to prevent Queue.get from blocking."""
    # TODO(shawnl): This is coupled too much to the implementation. Refactor to
    # remove coupling.
    # Queue.Queue does not inherit from object so it's an old-style object.
    def __init__(self):
        Queue.Queue.__init__(self)
        self.is_first = True

    def get(self, *args, **kwargs):
        """Add block=False as default to bypass blocking."""
        kwargs["block"] = False
        return Queue.Queue.get(self, *args, **kwargs)


class Sqlite3WorkerTests(unittest.TestCase):
    """Test out the sqlite3worker library."""
    def setUp(self):  # pylint:disable=C0103
        self.stubs = mox.stubout.StubOutForTesting()
        self.tmp_file = tempfile.NamedTemporaryFile(
            suffix="pytest", prefix="sqlite").name
        # Stop the thread from auto starting so we can call run() manually.
        self.stubs.Set(sqlite3worker.threading.Thread, "start", lambda x: True)
        self.sqlite3worker = sqlite3worker.Sqlite3Worker(self.tmp_file)
        self.sqlite3worker.write_queue = StubQueue()
        self.sqlite3_conn = sqlite3.connect(self.tmp_file)
        self.sqlite3_cursor = self.sqlite3_conn.cursor()

    def tearDown(self):  # pylint:disable=C0103
        self.stubs.UnsetAll()
        self.sqlite3_conn.close()
        os.unlink(self.tmp_file)

    def test_execute_insert(self):
        """Insert and Select."""
        # Create sql db.
        self.sqlite3_cursor.execute(
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")
        self.assertEqual(self.sqlite3worker.queue_size, 0)
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        self.assertEqual(self.sqlite3worker.queue_size, 1)
        self.assertEqual(self.sqlite3worker.execute("SELECT * from tester"), [])
        self.sqlite3worker.execute(
            "INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
        self.assertEqual(self.sqlite3worker.queue_size, 2)
        self.assertEqual(self.sqlite3worker.execute("SELECT * from tester"), [])
        # Raises Queue.Empty only because of the StubQueue class.  Will not
        # raise in production code.
        self.assertRaises(Queue.Empty, self.sqlite3worker.run)
        self.assertEqual(self.sqlite3worker.queue_size, 0)
        self.assertEqual(
            self.sqlite3worker.execute("SELECT * from tester"),
            [("2010-01-01 13:00:00", "bow"), ("2011-02-02 14:14:14", "dog")])


if __name__ == "__main__":
    unittest.main()