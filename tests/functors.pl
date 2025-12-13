# facts
person(
  alice,
  cons(apple, cons(pear, nil))
).
person(
  bob,
  cons(lemon, nil)
).
person(
  dave,
  nil
).

# length with constructors only:
#   z, s(z), s(s(z)), ...
len(nil, z).
len(cons(H, T), s(N)) :- len(T, N).

first(cons(H, T), H).
rest(cons(H, T), T).

my_first_fruit(Name, F) :-
  person(Name, Fs),
  first(Fs, F).

num_fruits(Name, Num) :-
 person(Name, Fs),
 len(Fs, Num).
