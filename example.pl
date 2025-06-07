%! link(atom, atom).
link(a, b).
link(b, c).
link(c, c).
link(c, d).
link(e, f).
same(X) :- link(X, X).
link(X, Y)?
same(X)?
same(a) :- link(a,a)!
same(X)?
