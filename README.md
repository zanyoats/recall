# recall

a proof of concept datalog engine using rocksdb.

```sh
% cargo run -- -h
program path: target/debug/recall
usage: <program> [ options ] [ db ]

To start a repl leave out any options and (optional) supply
a positional argument to a database root folder. If no folder
is supplied it will create a fresh database just for this run.
Upon exiting, it will drop the database.

Options:
-h:            help
-f file:       read in datalog program from file
-s '...':      read in datalog program from string

Argument:
[db]:          path to db root folder, if not supplied, create
               a fresh database in a temp location, then drop
               it before the program exits.
```
