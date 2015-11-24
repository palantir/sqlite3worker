# Sqlite3Worker

A threadsafe sqlite worker.

This library implements a thread pool pattern with sqlite3 being the desired
output.

sqllite3 implementation lacks the ability to safely modify the sqlite3 database
with multiple threads outside of the compile time options.  This library was
created to address this by bringing the responsibility of managing the threads
to the python layer and is agnostic to the server setup of sqlite3.

## Install
Installation is via the usual ``setup.py`` method:

```sh
sudo python setup.py install
```

Alternatively one can use ``pip`` to install directly from the git repository
without having to clone first:

```sh
sudo pip install git+https://github.com/palantir/sqlite3worker#egg=sqlite3worker
```

One may also use ``pip`` to install on a per-user basis without requiring
super-user permissions:

```sh
pip install --user git+https://github.com/palantir/sqlite3worker#egg=sqlite3worker
```

## Example
```python
from sqlite3worker import Sqlite3Worker

sql_worker = Sqlite3Worker("/tmp/test.sqlite")
sql_worker.execute("CREATE TABLE tester (timestamp DATETIME, uuid TEXT)")
sql_worker.execute("INSERT into tester values (?, ?)", ("2010-01-01 13:00:00", "bow"))
sql_worker.execute("INSERT into tester values (?, ?)", ("2011-02-02 14:14:14", "dog"))

results = sql_worker.execute("SELECT * from tester")
for timestamp, uuid in results:
    print(timestamp, uuid)

sql_worker.close()
```

## When to use sqlite3worker
If you have multiple threads all needing to write to a sqlite3 database this
library will serialize the sqlite3 write requests.

## When NOT to use sqlite3worker
If your code DOES NOT use multiple threads then you don't need to use a thread
safe sqlite3 implementation.

If you need multiple applications to write to a sqlite3 db then sqlite3worker
will not protect you from corrupting the data.

## Internals
The library creates a queue to manage multiple queries sent to the database.
Instead of directly calling the sqlite3 interface, you will call the
Sqlite3Worker which inserts your query into a Queue.Queue() object.  The queries
are processed in the order that they are inserted into the queue (first in,
first out).  In order to ensure that the multiple threads are managed in the
same queue, you will need to pass the same Sqlite3Worker object to each thread.

## Python docs for sqlite3
https://docs.python.org/2/library/sqlite3.html
