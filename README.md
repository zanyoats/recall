# recall

a proof of concept datalog engine using a terrible storage engine.

it contains the following high level components
- scanner/parser
- unification (really part of evaluation)
- naive datalog evaluation
- on disk pages for each predicate connected in a linked list
  - plan to switch to btree using a single file for all predicates (a la sqlite)
