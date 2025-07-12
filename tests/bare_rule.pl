true.
succeed :- true.
fail :- fruit(oats).

fruit(pear).
all_fruits(X) :- fruit(X), true.
all_fruits2(X) :- fruit(X), succeed.
all_fruits3(X) :- fruit(X), fail.
