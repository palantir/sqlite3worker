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

"""Thread safe sqlite3 interface."""

__author__ = "Shawn Lee"
__email__ = "shawnl@palantir.com"

import logging
import Queue
import sqlite3
import threading

class Sqlite3Worker(threading.Thread):
    """Sqlite thread safe object.

    Example:
        from sqlite3worker import Sqlite3Worker
        sql_worker = Sqlite3Worker("/tmp/test.sqlite")
        sql_worker.execute(
            "CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")
        sql_worker.execute(
            "INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
        sql_worker.execute(
            "INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))
        sql_worker.execute("SELECT * from tester")
    """
    def __init__(self, file_name, logger=None, max_queue_size=100):
        """Automatically starts the thread.

        Args:
            file_name: The name of the file.
            logger: A logging object or will just grab the local logger.
            max_queue_size: The max queries that will be queued.
        """
        self.logger = logger or logging.getLogger(__name__)
        threading.Thread.__init__(self)
        self.daemon = True
        self.sqlite3_conn = sqlite3.connect(file_name, check_same_thread=False)
        self.sqlite3_cursor = self.sqlite3_conn.cursor()
        self.write_queue = Queue.Queue(maxsize=max_queue_size)
        self.max_queue_size = max_queue_size
        self.start()

    def run(self):
        """Thread loop.

        This is an infinite loop.  The iter method calls self.write_queue.get()
        which blocks if there are not values in the queue.  As soon as values
        are placed into the queue the process will continue.

        If many execute's happen at once it will churn through them all before
        calling commit() to speed things up by reducing the number of times
        commit is called.
        """
        self.logger.debug("run: Thread started")
        execute_count = 0
        for query, values in iter(self.write_queue.get, None):
            self.logger.debug("run: %s", query)
            self.sqlite3_cursor.execute(query, values)
            execute_count += 1
            # Let the executes build up a little before commiting to disk to
            # speed things up.
            if self.write_queue.empty() or execute_count == self.max_queue_size:
                self.logger.debug("run: commit")
                self.sqlite3_conn.commit()
                execute_count = 0

    @property
    def queue_size(self):
        """Return the queue size."""
        return self.write_queue.qsize()

    def execute(self, query, values=None):
        """Execute a query.

        Args:
            query: The sql string using ? for placeholders of dynamic values.
            values: A tuple of values to be replaced into the ? of the query.

        Returns:
            If it's a select query it will return
            self.sqlite3_cursor.fetchall().
        """
        values = values or []
        # If it's a select we can just do it but otherwise we want to queue the
        # operation to keep thread collisions away.
        if query.lower().strip().startswith("select"):
            self.logger.debug("execute: %s", query)
            self.sqlite3_cursor.execute(query, values)
            return self.sqlite3_cursor.fetchall()
        else:
            self.write_queue.put((query, values))