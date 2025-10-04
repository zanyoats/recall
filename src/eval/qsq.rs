#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::HashMap;
use std::collections::HashSet;

use crate::lang::analysis;
use crate::lang::parse;
use crate::lang::unify;
use crate::lang::unify::Bindings;
use crate::lang::DBAux;
use crate::lang::QueryAnswer;
use crate::lang::{Atom, Defs, Rule, Strata};

#[derive(Debug, PartialEq, Eq)]
struct QueryProgram {
    query: Atom,
    strata: Strata,
}

#[derive(Debug)]
struct QueryResult {
    inputs: HashMap<String, Vec<Bindings>>,
    answers: HashMap<String, HashSet<Atom>>,
    inputs_changed: bool,
    answers_changed: bool,
}

fn collect_rules<DB: DBAux>(query: &Atom, db_helper: &DB) -> parse::Defs {
    let mut result = HashMap::new();

    let name = query.functor_name();
    let arity = query.functor_arity();
    let mut rule_added = false;

    if let Some(rule) = db_helper.get_rule(name, arity) {
        rule_added = result.insert((name.to_string(), arity), rule).is_none();
    }

    while rule_added {
        let mut candidates = vec![];

        for (_, rule) in result.iter() {
            for clause in rule.iter() {
                for atom in clause.body.iter() {
                    let name = atom.functor_name();
                    let arity = atom.functor_arity();

                    if let Some(rule) = db_helper.get_rule(name, arity) {
                        candidates.push(rule);
                    }
                }
            }
        }

        if {
            candidates.into_iter().any(|candidate| {
                let name = candidate[0].head.functor_name().to_string();
                let arity = candidate[0].head.functor_arity();

                result.insert((name, arity), candidate).is_none()
            })
        } {
            rule_added = true;
        } else {
            rule_added = false;
        }
    }

    result
}

fn rewrite_query_atom(atom: &Atom, bound_flags: &Vec<bool>) -> Atom {
    let suffix = bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let name = format!("{}_{}", atom.functor_name(), suffix);
    let args = atom.functor_args().iter().cloned().collect::<Vec<_>>();
    let negated = atom.functor_negated();

    parse::Term::Functor(name, args, negated)
}

fn rewrite_rule_atom(
    atom: &Atom,
    atom_bound_flags: &Vec<bool>,
    head_bound_flags: &Vec<bool>,
) -> Atom {
    let name_suffix = atom_bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let var_suffix = head_bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let name = format!("{}_{}", atom.functor_name(), name_suffix);
    let args = atom
        .functor_args()
        .iter()
        .cloned()
        .map(|atom| match atom {
            parse::Term::Var(var) => parse::Term::Var(format!("{}_{}", var, var_suffix)),
            other => other,
        })
        .collect::<Vec<_>>();
    let negated = atom.functor_negated();

    parse::Term::Functor(name, args, negated)
}

fn rewrite_fact_atom(atom: &Atom, head_bound_flags: &Vec<bool>) -> Atom {
    let var_suffix = head_bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let name = format!("{}", atom.functor_name());
    let args = atom
        .functor_args()
        .iter()
        .cloned()
        .map(|atom| match atom {
            parse::Term::Var(var) => parse::Term::Var(format!("{}_{}", var, var_suffix)),
            other => other,
        })
        .collect::<Vec<_>>();
    let negated = atom.functor_negated();

    parse::Term::Functor(name, args, negated)
}

fn adorn_program(query: &Atom, defs: Defs) -> (Atom, Defs) {
    let mut adorned_defs: Defs = HashMap::new();
    let mut adorned_rules = HashSet::new();

    // only rule atoms in `adorn_atoms` should be rules in defs
    let mut adorn_atoms = vec![];

    let query_name = query.functor_name().to_string();
    let query_arity = query.functor_arity();

    let query_bound_flags = query
        .functor_args()
        .iter()
        .map(|arg| {
            if let parse::Term::Var(_) = arg {
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();

    adorned_rules.insert((query_name.clone(), query_bound_flags.clone()));
    adorn_atoms.push((query_name.clone(), query_bound_flags.clone()));

    let adorned_query = rewrite_query_atom(&query, &query_bound_flags);

    while let Some((name, bound_flags)) = adorn_atoms.pop() {
        let functor_spec = format!("{}/{}", name, bound_flags.len());
        let rule = defs.get(&functor_spec).unwrap();

        let mut head_bounded_vars = vec![];
        for i in 0..bound_flags.len() {
            if bound_flags[i] {
                if let parse::Term::Var(var) = &rule.head.functor_args()[i] {
                    head_bounded_vars.push(var.to_string());
                }
            }
        }

        let head_ = rewrite_rule_atom(&rule.head, &bound_flags, &bound_flags);
        let mut clauses_ = Vec::with_capacity(rule.clauses.len());

        for clause in rule.clauses.iter() {
            let mut clause_ = Vec::with_capacity(clause.len());

            // starts with head bound vars for each rule clause
            let mut clause_bounded_vars = head_bounded_vars.clone();

            for atom in clause.iter() {
                let name = atom.functor_name();
                let arity = atom.functor_arity();
                let functor_spec = format!("{}/{}", name, arity);

                if defs.contains_key(&functor_spec) {
                    /* atom is a rule */
                    let mut atom_bound_flags = Vec::with_capacity(arity.into());

                    for i in 0..arity.into() {
                        if let parse::Term::Var(var) = &atom.functor_args()[i] {
                            atom_bound_flags.push(clause_bounded_vars.contains(var));
                        } else {
                            atom_bound_flags.push(true);
                        }
                    }

                    if !adorned_rules.contains(&(name.to_string(), atom_bound_flags.clone())) {
                        adorned_rules.insert((name.to_string(), atom_bound_flags.clone()));
                        adorn_atoms.push((name.to_string(), atom_bound_flags.clone()));
                    }

                    clause_.push(rewrite_rule_atom(atom, &atom_bound_flags, &bound_flags));
                } else {
                    /* atom is a fact */
                    clause_.push(rewrite_fact_atom(atom, &bound_flags));
                }

                // add all variables not bound to bounded vars
                for arg in atom.functor_args() {
                    if let parse::Term::Var(var) = arg {
                        if !clause_bounded_vars.contains(var) {
                            clause_bounded_vars.push(var.to_string())
                        }
                    }
                }
            }

            clauses_.push(clause_);
        }

        let adorned_rule_key = head_.functor_spec();
        adorned_defs.insert(
            adorned_rule_key,
            Rule {
                head: head_,
                clauses: clauses_,
            },
        );
    }

    (adorned_query, adorned_defs)
}

pub fn eval_query<DB: DBAux>(query: Atom, db_helper: &mut DB) -> QueryAnswer {
    let query_rules: parse::Defs = collect_rules(&query, db_helper);

    if query_rules.is_empty() {
        /* try eval query as fact */
        let answer = eval_db_fact(&query, HashMap::new(), db_helper);
        QueryAnswer { query, answer }
    } else {
        /* implies query is a rule */
        let renamed_vars: Defs = analysis::rename_vars(query_rules);
        let (adorned_query, adorned_rules) = adorn_program(&query, renamed_vars);
        let strata = analysis::stratify_rules(adorned_rules).unwrap();
        let query_program = QueryProgram {
            query: adorned_query,
            strata,
        };
        let mut query_result = initialize_query_result(&query_program);
        eval_until_fixpoint(&query_program, &mut query_result, db_helper);
        // println!("{query_program:?}");
        // println!("{query_result:?}");
        let answer = query_result
            .answers
            .get(&(query_program.query.functor_spec()))
            .unwrap()
            .iter()
            .filter_map(|atom| unify::unify(&query_program.query, atom))
            .collect::<Vec<Bindings>>();
        QueryAnswer { query, answer }
    }
}

fn initialize_query_result(program: &QueryProgram) -> QueryResult {
    let mut inputs = HashMap::new();
    let mut answers = HashMap::new();

    for stratum in program.strata.iter() {
        for (atom_spec, _) in stratum.iter() {
            inputs.insert(atom_spec.to_string(), vec![]);
            answers.insert(atom_spec.to_string(), HashSet::new());
        }
    }

    // pass initial bindings top-down from query
    // NOTE: we know `program.query` is a rule atom since this
    // function is only called on queries that are rule atoms.
    // We cannot say this about rule body atoms (below).
    let mut stack = vec![program.query.clone()];
    while let Some(atom) = stack.pop() {
        for stratum in program.strata.iter() {
            let atom_spec = atom.functor_spec();
            if let Some(rule) = stratum.get(&atom_spec) {
                if let Some(bindings) = unify::unify(&rule.head, &atom) {
                    let set = inputs.get_mut(&atom_spec).unwrap();
                    if set.is_empty() {
                        set.push(unify::filter_bound(&bindings));
                    }

                    for clause in rule.clauses.iter() {
                        for atom_ in clause.iter() {
                            if let Some(set) = inputs.get(&atom_.functor_spec()) {
                                if set.is_empty() {
                                    stack.push(unify::substitute_bindings(atom_, &bindings));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    QueryResult {
        inputs,
        answers,
        inputs_changed: false,
        answers_changed: false,
    }
}

fn eval_until_fixpoint<DB: DBAux>(
    program: &QueryProgram,
    result: &mut QueryResult,
    db_helper: &mut DB,
) {
    for stratum in program.strata.iter() {
        loop {
            result.inputs_changed = false;
            result.answers_changed = false;

            eval_program(&program.strata, stratum, result, db_helper);

            if !result.inputs_changed && !result.answers_changed {
                break;
            }
        }
    }
}

fn eval_program<DB: DBAux>(
    strata: &Strata,
    stratum: &Defs,
    result: &mut QueryResult,
    db_helper: &mut DB,
) {
    for (atom_spec, rule) in stratum.iter() {
        let mut derived_atoms = HashSet::new();

        let inputs = result
            .inputs
            .get(atom_spec)
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        for input_bindings in inputs {
            for output_bindings in
                eval_rule(strata, stratum, result, db_helper, &rule, input_bindings)
            {
                derived_atoms.insert(unify::substitute_bindings(&rule.head, &output_bindings));
            }
        }

        let set = result.answers.get_mut(atom_spec).unwrap();
        for derived_atom in derived_atoms {
            if set.insert(derived_atom) {
                result.answers_changed = true;
            }
        }
    }
}

fn eval_rule<DB: DBAux>(
    strata: &Strata,
    stratum: &Defs,
    result: &mut QueryResult,
    db_helper: &mut DB,
    rule: &Rule,
    input_bindings: Bindings,
) -> Vec<Bindings> {
    let mut output_bindings = vec![];

    for clause in rule.clauses.iter() {
        output_bindings.extend(eval_body(
            strata,
            stratum,
            result,
            db_helper,
            &clause[..],
            input_bindings.clone(),
        ));
    }

    output_bindings
}

fn eval_body<DB: DBAux>(
    strata: &Strata,
    stratum: &Defs,
    result: &mut QueryResult,
    db_helper: &mut DB,
    atoms: &[Atom],
    bindings: Bindings,
) -> Vec<Bindings> {
    if let Some(atom) = atoms.first() {
        eval_atom(strata, stratum, result, db_helper, atom, bindings.clone())
            .into_iter()
            .flat_map(|bindings| {
                eval_body(strata, stratum, result, db_helper, &atoms[1..], bindings)
            })
            .collect()
    } else {
        vec![bindings]
    }
}

fn eval_atom<DB: DBAux>(
    strata: &Strata,
    stratum: &Defs,
    result: &mut QueryResult,
    db_helper: &mut DB,
    atom: &Atom,
    bindings: Bindings,
) -> Vec<Bindings> {
    // find rule in strata
    let atom_spec = atom.functor_spec();
    let mut rule_opt = None;
    for stratum in strata.iter() {
        if let Some(rule) = stratum.get(&atom_spec) {
            rule_opt = Some(rule);
            break;
        }
    }

    let atom_bindings = if let Some(rule) = rule_opt {
        let mut rule_bindings = vec![];

        // new sub-query case
        if stratum.contains_key(&atom_spec) {
            let set = result.inputs.get_mut(&atom_spec).unwrap();

            let atom_ = unify::substitute_bindings(atom, &bindings);
            let bindings_ = unify::unify(&rule.head, &atom_).unwrap();
            let new = unify::filter_bound(&bindings_);

            if !set.contains(&new) {
                set.push(new);
                result.inputs_changed = true;
            }
            // let atom1 = unify::substitute_bindings(atom, &bindings);
            // let bindings1 = unify::unify(&atom1, &rule.head).unwrap();
            // if set.insert(unify::substitute_bindings(&rule.head, &bindings1)) {
            //     result.inputs_changed = true;
            // }
        }

        let answers = result.answers.get(&atom_spec).unwrap();

        rule_bindings.extend(
            answers
                .iter()
                .filter_map(|answer| unify::unify_with_bindings(atom, answer, bindings.clone())),
        );

        rule_bindings
    } else {
        eval_db_fact(atom, bindings.clone(), db_helper)
    };

    return if atom.functor_negated() {
        if atom_bindings.is_empty() {
            vec![bindings]
        } else {
            vec![]
        }
    } else {
        atom_bindings
    };
}

fn eval_db_fact<DB: DBAux>(atom: &Atom, bindings: Bindings, db_helper: &mut DB) -> Vec<Bindings> {
    let mut result_bindings = vec![];
    let search_term = unify::substitute_bindings(atom, &bindings);
    result_bindings.extend(
        db_helper
            .lookup(search_term)
            .filter_map(|(_, fact)| unify::unify_with_bindings(atom, &fact, bindings.clone())),
    );
    result_bindings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::analysis;
    use crate::lang::parse::parse_program;
    use crate::lang::parse::Parser;
    use crate::lang::parse::TypedStatement;
    use crate::lang::scan::Scanner;

    #[derive(Debug)]
    struct TestDBAux {
        defs: parse::Defs,
        facts: Vec<Atom>,
        query: Option<Atom>,
    }

    impl TestDBAux {
        fn from_string(program: &str) -> Result<Self, anyhow::Error> {
            let scanner = Scanner::new(program);
            let mut parser = Parser::new(scanner);
            let program = parse_program(&mut parser)?;
            let typed_program = analysis::typecheck(program)?;

            let mut facts = vec![];
            let mut query = None;

            for stmt in typed_program.statements {
                match stmt {
                    TypedStatement::Fact(atom) => facts.push(atom),
                    TypedStatement::Query(atom) => query = Some(atom),
                    _ => continue,
                }
            }

            Ok(TestDBAux {
                defs: typed_program.defs,
                facts,
                query,
            })
        }
    }

    impl DBAux for TestDBAux {
        fn get_rule(&self, name: &str, arity: u8) -> Option<Vec<parse::Literal>> {
            self.defs.get(&(name.to_string(), arity)).cloned()
        }

        fn lookup(&mut self, _search_term: Atom) -> Box<dyn Iterator<Item = (u32, Atom)>> {
            Box::new(
                self.facts
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(id, atom)| (u32::try_from(id).unwrap(), atom))
                    .collect::<Vec<_>>()
                    .into_iter(),
            )
        }
    }

    fn atom(name: &str) -> parse::Term {
        parse::Term::Atom(name.to_string())
    }

    fn var(name: &str) -> parse::Term {
        parse::Term::Var(name.to_string())
    }

    fn functor(name: &str, args: Vec<parse::Term>) -> parse::Term {
        parse::Term::Functor(name.to_string(), args, false)
    }

    #[test]
    fn rename_vars_test1() -> Result<(), anyhow::Error> {
        let db_helper = TestDBAux::from_string(
            "
            #! a(atom,atom).
            #! b(atom,atom).
            b(R,S,T) :- a(R,S), b(Z, T).
        ",
        )?;

        let got = analysis::rename_vars(db_helper.defs);
        let want = HashMap::from_iter(
            vec![(
                format!("{}/{}", "b", 3),
                Rule {
                    head: functor("b", vec![var("v1"), var("v2"), var("v3")]),
                    clauses: vec![vec![
                        functor("a", vec![var("v1"), var("v2")]),
                        functor("b", vec![var("v4"), var("v3")]),
                    ]],
                },
            )]
            .into_iter(),
        );
        assert_eq!(got, want);
        Ok(())
    }

    // #[test]
    // fn adorn_program_test1() -> Result<(), anyhow::Error> {
    //     let db_helper = TestDBAux::from_string("
    //         #! a(atom,atom).
    //         true.
    //         succeed :- true.
    //         b(A,B) :- a(A,C), a(C,B), succeed.
    //         b(a,Y)?
    //     ")?;

    //     let renamed_defs = rename_vars(db_helper.defs);
    //     let got = adorn_program(&db_helper.query.unwrap(), renamed_defs);
    //     let want = QueryProgram {
    //         adorned_query: functor("b_bf", vec![atom("a"), var("Y")]),
    //         adorned_defs:
    //             HashMap::from_iter(vec![
    //                 (format!("{}/{}", "b_bf", 2), Rule {
    //                     head: functor("b_bf", vec![var("v1_bf"), var("v2_bf")]),
    //                     clauses: vec![vec![
    //                         functor("a", vec![var("v1_bf"), var("v3_bf")]),
    //                         functor("a", vec![var("v3_bf"), var("v2_bf")]),
    //                         functor("succeed_", vec![]),
    //                     ]],
    //                 }),
    //                 (format!("{}/{}", "succeed_", 0), Rule {
    //                     head: functor("succeed_", vec![]),
    //                     clauses: vec![
    //                         vec![functor("true_", vec![])],
    //                     ],
    //                 }),
    //                 (format!("{}/{}", "true_", 0), Rule {
    //                     head: functor("true_", vec![]),
    //                     clauses: vec![vec![]],
    //                 }),
    //             ].into_iter()),
    //     };
    //     assert_eq!(got, want);
    //     Ok(())
    // }

    // #[test]
    // fn adorn_program_test2() -> Result<(), anyhow::Error> {
    //     let db_helper = TestDBAux::from_string("
    //         #! a(atom,atom).
    //         b(A,B) :- a(A,C), a(C,B), b(X,Y).
    //         b(a,Y)?
    //     ")?;

    //     let renamed_defs = rename_vars(db_helper.defs);
    //     let got = adorn_program(&db_helper.query.unwrap(), renamed_defs);
    //     let want = QueryProgram {
    //         adorned_query: functor("b_bf", vec![atom("a"), var("Y")]),
    //         adorned_defs:
    //             HashMap::from_iter(vec![
    //                 (format!("{}/{}", "b_ff", 2), Rule {
    //                     head: functor("b_ff", vec![var("v1_ff"), var("v2_ff")]),
    //                     clauses: vec![vec![
    //                         functor("a", vec![var("v1_ff"), var("v3_ff")]),
    //                         functor("a", vec![var("v3_ff"), var("v2_ff")]),
    //                         functor("b_ff", vec![var("v4_ff"), var("v5_ff")]),
    //                     ]],
    //                 }),
    //                 (format!("{}/{}", "b_bf", 2), Rule {
    //                     head: functor("b_bf", vec![var("v1_bf"), var("v2_bf")]),
    //                     clauses: vec![vec![
    //                         functor("a", vec![var("v1_bf"), var("v3_bf")]),
    //                         functor("a", vec![var("v3_bf"), var("v2_bf")]),
    //                         functor("b_ff", vec![var("v4_bf"), var("v5_bf")]),
    //                     ]],
    //                 }),
    //             ].into_iter()),
    //     };
    //     assert_eq!(got, want);
    //     Ok(())
    // }

    #[test]
    fn qsq_test1() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            #! bar(atom).

            link(a,b).
            link(b,c).
            link(c,c).
            link(c,d).

            true.
            succeed :- true.
            foo(X) :- bar(X).
            reachable(X,Y) :- link(X,Y), succeed.
            reachable(X,Y) :- link(X,Z), reachable(Z,Y).

            reachable(b,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                functor("reachable", vec![atom("b"), atom("c")]),
                functor("reachable", vec![atom("b"), atom("d")]),
            ])
        );
        Ok(())
    }

    #[test]
    fn qsq_test2() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            link(a,b).
            link(b,c).
            link(c,c).
            link(c,d).

            reachable(X,Y) :- link(X,Y).
            reachable(X,Y) :- link(X,Z), reachable(Z,Y).

            reachable(X,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                functor("reachable", vec![atom("a"), atom("b")]),
                functor("reachable", vec![atom("a"), atom("c")]),
                functor("reachable", vec![atom("a"), atom("d")]),
                functor("reachable", vec![atom("b"), atom("c")]),
                functor("reachable", vec![atom("b"), atom("d")]),
                functor("reachable", vec![atom("c"), atom("c")]),
                functor("reachable", vec![atom("c"), atom("d")]),
            ])
        );
        Ok(())
    }

    #[test]
    fn qsq_test3() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            link(a,b).
            link(b,c).
            link(c,c).
            link(c,d).

            reachable(X,Y) :- link(X,Y).
            reachable(X,Y) :- link(X,Z), reachable(Z,Y).

            path(X,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(answer.get_atoms(), HashSet::from([]));
        Ok(())
    }

    #[test]
    fn qsq_test4() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            link(a,b).
            link(b,c).
            link(c,c).
            link(c,d).

            link(X,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                functor("link", vec![atom("a"), atom("b")]),
                functor("link", vec![atom("b"), atom("c")]),
                functor("link", vec![atom("c"), atom("c")]),
                functor("link", vec![atom("c"), atom("d")]),
            ])
        );
        Ok(())
    }

    #[test]
    fn qsq_test5() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            link(x,y).
            link(y,z).

            node(X) :- link(X, Y).
            node(Y) :- link(X, Y).
            reachable(X, Y) :- link(X, Y).
            reachable(X, Y) :- link(X, Z), reachable(Z, Y).
            unreachable(X, Y) :- node(X), node(Y), not reachable(X, Y).

            unreachable(z,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                functor("unreachable", vec![atom("z"), atom("z")]),
                functor("unreachable", vec![atom("z"), atom("y")]),
                functor("unreachable", vec![atom("z"), atom("x")]),
            ])
        );
        Ok(())
    }

    #[test]
    fn qsq_test6() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            link(x,y).
            link(y,z).

            node(X) :- link(X, Y).
            node(Y) :- link(X, Y).
            reachable(X, Y) :- link(X, Y).
            reachable(X, Y) :- link(X, Z), reachable(Z, Y).
            indirect(X, Y) :- reachable(X, Y), not link(X, Y).

            indirect(X,Y)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([functor("indirect", vec![atom("x"), atom("z")]),])
        );
        Ok(())
    }
}
