# facts
person(
  alice,
  ::(apple, ::(pear, nil))
).
person(
  bob,
  ::(lemon, nil)
).
person(
  dave,
  nil
).

# person(X, Y)?           -> alice, bob, dave
# person(X, ::(H, nil))?  -> bob
# person(X, ::(H, R))?    -> alice, bob


# length with constructors only:
#   z, s(z), s(s(z)), ...
#
#len(nil, z).
#len(::(H, T), s(N)) :- len(T, N).

first(::(H, T), H).
rest(::(H, T), T).

#num_fruits(Name, C) :-
#  person(Name, Fs),
#  len(Fs, C).

my_first_fruit(Name, F) :-
  person(Name, Fs),
  first(Fs, F).
