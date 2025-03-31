use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

struct LLGrammar<'a> {
    non_terminals: HashSet<&'a str>,
    terminals: HashSet<&'a str>,
    table: LLGrammarTable<'a>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct GrammarRule {
    lhs: String,
    rhs: Vec<String>,
}

impl fmt::Display for GrammarRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", self.lhs, self.rhs.join(" "))?;
        Ok(())
    }
}

type LLGrammarTable<'a> = HashMap<(&'a str, &'a str), Vec<&'a GrammarRule>>;

type LLGrammarSets<'a> = (
    HashMap<&'a str, bool>,
    HashMap<&'a str, HashSet<&'a str>>,
    HashMap<&'a str, HashSet<&'a str>>,
);

impl GrammarRule {
    fn new(lhs: &str, rhs: &[&str]) -> Self {
        let rhs: Vec<String> =
            rhs
            .iter()
            .map(|x| x.to_string())
            .collect();

        GrammarRule { lhs: lhs.to_string(), rhs }
    }

}

impl<'a> LLGrammar<'a> {
    fn new(grammar: &'a [GrammarRule]) -> Self {
        let non_terminals: HashSet<&str> =
            grammar
            .iter()
            .map(|rule| rule.lhs.as_str())
            .collect();
        let terminals: HashSet<&str> =
            grammar
            .iter()
            .flat_map(|rule| { &rule.rhs })
            .filter_map(|elem| {
                let elem = elem.as_str();
                if non_terminals.contains(elem) { None }
                else { Some(elem) }
            })
            .collect();
        let table =
            Self::build_parsing_table(grammar, &non_terminals, &terminals);

        LLGrammar { non_terminals, terminals, table }
    }

    fn terminals_for_rule(rule: &'a GrammarRule, sets: &LLGrammarSets<'a>) -> HashSet<&'a str> {
        let x = rule.lhs.as_str();
        let expr_iter = rule.rhs.iter().map(|x| x.as_str());
        let (
            nullable,
            first,
            follow,
        ) = sets;

        let mut expr_nullable = true;
        let mut result = HashSet::new();

        for elem in expr_iter {
            result.extend(
                first
                .get(elem)
                .unwrap()
                .iter()
            );

            if !nullable.get(elem).unwrap() {
                expr_nullable = false;
                break;
            }
        }

        if expr_nullable {
            result.extend(
                follow
                .get(x)
                .unwrap()
                .iter()
            );
        }

        result
    }

    fn build_parsing_table(
        grammar: &'a [GrammarRule],
        non_terminals: &HashSet<&'a str>,
        terminals: &HashSet<&'a str>,
    ) -> LLGrammarTable<'a> {
        let mut table: LLGrammarTable<'a>= HashMap::new();
        let sets = Self::compute_sets(grammar, &non_terminals, &terminals);

        for rule in grammar.iter() {
            let columns = Self::terminals_for_rule(rule, &sets);

            for column in columns {
                table
                .entry((rule.lhs.as_str(), column))
                .and_modify(|e: &mut Vec<&GrammarRule>| {
                    e.push(rule);
                })
                .or_insert(vec![rule]);
            }
        }

        table
    }

    fn print_table(&self) {
        for non_terminal in self.non_terminals.iter() {
            for terminal in self.terminals.iter() {
                let result =
                    self
                    .table
                    .get(&(*non_terminal, *terminal))
                    .map_or_else(|| "no entry".to_string(), |rules| {
                        rules
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                    });

                println!("[{}, {}] {}", non_terminal, terminal, result);
            }
            println!();
        }
    }

    fn compute_sets(
        grammar: &'a [GrammarRule],
        non_terminals: &HashSet<&'a str>,
        terminals: &HashSet<&'a str>,
    ) -> LLGrammarSets<'a> {
        let mut nullable = HashMap::new();
        let mut first = HashMap::new();
        let mut follow = HashMap::new();
        for &non_terminal in non_terminals.iter() {
            assert!(nullable.insert(non_terminal, false).is_none());
            assert!(first.insert(non_terminal, HashSet::new()).is_none());
            assert!(follow.insert(non_terminal, HashSet::new()).is_none());
        }
        for &terminal in terminals.iter() {
            assert!(nullable.insert(terminal, false).is_none());
            first.insert(
                terminal,
                HashSet::from_iter(vec![terminal].into_iter()),
            );
            follow.insert(terminal, HashSet::new());
        }

        // loop until fixed point is found
        loop {
            let mut no_change = true;

            for rule in grammar.iter() { /* for each X -> Y_0 Y_1 .. Y_k */
                let x = rule.lhs.as_str();
                let ys = &rule.rhs;
                let k = ys.len();

                if (0..k).all(|i| { /* if all Y_i are nullable for X -> X is nullable */
                    *nullable.get(ys[i].as_str()).unwrap()
                }) {
                    let x_nullable = nullable.get_mut(x).unwrap();
                    if !*x_nullable {
                        *x_nullable = true;
                        no_change = false;
                    }
                }

                for i in 0..k {
                    if (0..i).all(|i| { /* if Y0 .. Y_i-1 are nullable */
                        *nullable.get(ys[i].as_str()).unwrap()
                    }) { /* FIRST(X) = FIRST(X) union FIRST(Y_i) */
                        let new_elems_iter =
                            first
                            .get(ys[i].as_str())
                            .unwrap()
                            .clone()
                            .into_iter();

                        let x_elems = first.get_mut(x).unwrap();

                        for elem in new_elems_iter {
                            if x_elems.contains(elem) {
                                continue;
                            }

                            x_elems.insert(elem);
                            no_change = false;
                        }
                    }

                    for j in i + 1..k { /* if Y_i+1 .. Y_j are nullable */
                        if (i+1..j).all(|i| {
                            *nullable.get(ys[i].as_str()).unwrap()
                        }) { /* FOLLOW(Y_i) = FOLLOW(Y_i) union FIRST(Y_j) */
                            let new_elems_iter =
                                first
                                .get(ys[j].as_str())
                                .unwrap()
                                .clone()
                                .into_iter();

                            let yi_elems = follow.get_mut(ys[i].as_str()).unwrap();

                            for elem in new_elems_iter {
                                if yi_elems.contains(elem) {
                                    continue;
                                }

                                yi_elems.insert(elem);
                                no_change = false;
                            }
                        }
                    }

                    if (i+1..k).all(|i| { /* if Y_i+1 .. Y_k are nullable */
                        *nullable.get(ys[i].as_str()).unwrap()
                    }) { /* FOLLOW(Y_i) = FOLLOW(Y_i) union FOLLOW(X) */
                        let new_elems_iter =
                            follow
                            .get(x)
                            .unwrap()
                            .clone()
                            .into_iter();

                        let yi_elems = follow.get_mut(ys[i].as_str()).unwrap();

                        for elem in new_elems_iter {
                            if yi_elems.contains(elem) {
                                continue;
                            }

                            yi_elems.insert(elem);
                            no_change = false;
                        }
                    }
                }
            }
            if no_change {
                break;
            }
        }

        (nullable, first, follow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_on_ll_1_grammar() {
        // Grammar
        //
        // non-terminal symbols are simply keys
        // terminal symbols are those rhs elements that have no corresponding lhs (production)
        let grammar = vec![
            // S -> E$
            GrammarRule::new("S", &["E", "$"]),
            // E -> T E'
            GrammarRule::new("E", &["T", "E'"]),
            // E' -> + T E'
            // E' -> - T E'
            // E' ->
            GrammarRule::new("E'", &["+", "T", "E'"]),
            GrammarRule::new("E'", &["-", "T", "E'"]),
            GrammarRule::new("E'", &[]),
            // T -> F T'
            GrammarRule::new("T", &["F", "T'"]),
            // T' -> * F T'
            // T' -> / F T'
            // T' ->
            GrammarRule::new("T'", &["*", "F", "T'"]),
            GrammarRule::new("T'", &["/", "F", "T'"]),
            GrammarRule::new("T'", &[]),
            // F -> id
            // F -> num
            // F -> ( E )
            GrammarRule::new("F", &["id"]),
            GrammarRule::new("F", &["num"]),
            GrammarRule::new("F", &["(", "E", ")"]),
        ];

        let ll = LLGrammar::new(&grammar);
        ll.print_table();
    }

    #[test]
    fn it_works_on_non_ll_1_grammar() {
        // Grammar
        //
        // non-terminal symbols are simply keys
        // terminal symbols are those rhs elements that have no corresponding lhs (production)
        let grammar = vec![
            // S -> Z$
            GrammarRule::new("S", &["Z", "$"]),         //0
            // Z -> XYZ
            // Z -> d
            GrammarRule::new("Z", &["X", "Y", "Z"]),    //1
            GrammarRule::new("Z", &["d"]),              //2
            // Y ->
            // Y -> c
            GrammarRule::new("Y", &[]),                 //3
            GrammarRule::new("Y", &["c"]),              //4
            // X -> Y
            // X -> a
            GrammarRule::new("X", &["Y"]),              //5
            GrammarRule::new("X", &["a"]),              //6
        ];

        let ll = LLGrammar::new(&grammar);
        ll.print_table();

    }
}
