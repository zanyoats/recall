reachable(X, Y) :- link(X, Y).
reachable(X, Z) :- link(X, Y), reachable(Y, Z).
