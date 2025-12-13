# recall

[![CI](https://github.com/zanyoats/recall/actions/workflows/ci.yml/badge.svg)](https://github.com/zanyoats/recall/actions/workflows/ci.yml)

## install

```sh
cargo install --path . # install to default path
```

## overview

Recall is a small toy Datalog engine backed by RocksDB. It supports
stratified negation, adornment-based QSQ evaluation (so bound queries can prune
search early), and functor terms for simple data structures like Peano numbers.
You can point it at a database on disk for persistence or omit the path to spin
up an in-memory scratch DB for quick experiments.

See the below [examples](#examples) and the [test](tests/) files.

```plaintext
% ./target/debug/recall -h
program path: ./target/debug/recall
usage: <program> [ options ] [ db ]

To start a repl leave out any options and (optional) supply
a positional argument to a database root folder. If no folder
is supplied it will create a fresh database just for this run.
Upon exiting, it will drop the database.

Options:
-h:            help
-v:            verbose (debug info)
-i:            info
-f file:       read in datalog program from file
-s '...':      read in datalog program from string
-c:            analysis only (check), prints out inferred declarations

Argument:
[db]:          path to db root folder, if not supplied, create
               a fresh database in a temp location, then drop
               it before the program exits.
```

## examples

Run the examples from the repo root; paths below are relative to `tests/`.

```sh
# reachability over a small graph
cargo run -- -f tests/graph.pl -s "reachable(X,Y)?"
# reachable(x, y)
# reachable(y, z)
# reachable(x, z)

# indirect edges (reachable but not directly linked)
cargo run -- -f tests/graph.pl -s "indirect(X,Y)?"
# indirect(x, z)

# unreachable node pairs (uses negation and stratum evaluation)
cargo run -- -f tests/graph.pl -s "unreachable(X,Y)?"
# unreachable(x, x)
# unreachable(y, x)
# unreachable(y, y)
# unreachable(z, x)
# unreachable(z, y)
# unreachable(z, z)

# working with lists and finding the head of a list functor
cargo run -- -f ./tests/functors.pl -s "my_first_fruit(Name, Num)?"
# my_first_fruit(bob, lemon)
# my_first_fruit(alice, apple)

# functor-based Peano length over fruit lists
cargo run -- -f tests/functors.pl -s "num_fruits(Name, Num)?"
# num_fruits(alice, s(s(z)))
# num_fruits(bob, s(z))
# num_fruits(dave, z)
```
