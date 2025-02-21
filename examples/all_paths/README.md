# Example 1

All pairs reachable paths example.

Run `cargo run -- rules.pl`

```
% facts
link("a","b").
link("b","c").
link("c","c").
link("c","d").

% rules
reachable(X, Y) :- link(X, Y).
reachable(X, Z) :- link(X, Y), reachable(Y, Z).

% results
repl> reachable(X,Y).
DEBUG: completed in 4 iterations
=> reachable("b", "d")
=> reachable("a", "c")
=> reachable("a", "d")
=> reachable("c", "c")
=> reachable("a", "b")
=> reachable("c", "d")
=> reachable("b", "c")
```
