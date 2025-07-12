# The active domain (all constants in db) are {x,y,z}

link(x,y).
link(y,z).

node(X) :- link(X, Y).
node(Y) :- link(X, Y).
reachable(X, Y) :- link(X, Y).
reachable(X, Y) :- link(X, Z), reachable(Z, Y).
indirect(X, Y) :- reachable(X, Y), not link(X, Y).
unreachable(X, Y) :- node(X), node(Y), not reachable(X, Y).
