#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::lang::analysis;
use crate::lang::unify;
use crate::lang::unify::Bindings;
use crate::lang::DBAux;
use crate::lang::QueryAnswer;
use crate::lang::{Term, Atom, Defs, Rule, Strata};

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

fn collect_rules<DB: DBAux>(
    query: &Atom,
    db_helper: &DB,
) -> HashMap<(String, u8), Rule> {
    let mut result = HashMap::new();

    let name = query.atom_name();
    let arity = query.atom_arity();
    let mut rule_added = false;

    if let Some(rule) = db_helper.get_rule(name, arity) {
        rule_added = result.insert((name.into(), arity), rule).is_none();
    }

    while rule_added {
        let mut candidates = vec![];

        for (_, rule) in result.iter() {
            for clause in rule.clauses.iter() {
                for atom in clause.iter() {
                    let name = atom.atom_name();
                    let arity = atom.atom_arity();

                    if let Some(rule) = db_helper.get_rule(name, arity) {
                        candidates.push(rule);
                    }
                }
            }
        }

        if {
            candidates.into_iter().any(|candidate| {
                let name = candidate.head.atom_name().into();
                let arity = candidate.head.atom_arity();

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

fn term_with_suffix(term: Term, suffix: &str) -> Term {
    match term {
        Term::Var(var) => Term::Var(format!("{}_{}", var, suffix)),
        Term::Atom(name, args, negated) => {
            Term::Atom(
                name,
                args.into_iter().map(|arg| term_with_suffix(arg, suffix)).collect(),
                negated,
            )
        }
        other => other,
    }
}

fn term_without_suffix(term: Term, suffix: &str) -> Term {
    match term {
        Term::Var(var) => {
            let mut trimmed = var.strip_suffix(suffix).unwrap_or(&var).to_string();
            if trimmed.ends_with('_') {
                trimmed.pop();
            }
            Term::Var(trimmed)
        }
        Term::Atom(name, args, negated) => {
            Term::Atom(
                name,
                args.into_iter().map(|arg| term_without_suffix(arg, suffix)).collect(),
                negated,
            )
        }
        other => other,
    }
}

fn fresh_var_suffix() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("__fresh{id}")
}

fn term_is_ground(term: &Term) -> bool {
    match term {
        Term::Var(_) => false,
        Term::Atom(_, args, _) => args.iter().all(term_is_ground),
        _ => true,
    }
}

fn term_contains_var(term: &Term, target: &str) -> bool {
    match term {
        Term::Var(var) => var == target,
        Term::Atom(_, args, _) => args.iter().any(|arg| term_contains_var(arg, target)),
        _ => false,
    }
}

fn atom_bound_flags(atom: &Atom) -> Option<Vec<char>> {
    let arity = atom.atom_arity() as usize;
    atom.atom_name()
        .rsplit_once('_')
        .and_then(|(_, suffix)| {
            if suffix.len() == arity && suffix.chars().all(|c| c == 'b' || c == 'f') {
                Some(suffix.chars().collect::<Vec<_>>())
            } else {
                None
            }
        })
}

fn atom_bound_args_ground(atom: &Atom) -> bool {
    if let Some(flags) = atom_bound_flags(atom) {
        atom.atom_args()
            .iter()
            .zip(flags.iter())
            .all(|(arg, flag)| *flag != 'b' || term_is_ground(arg))
    } else {
        true
    }
}

fn collect_term_vars(term: &Term, vars: &mut Vec<String>) {
    match term {
        Term::Var(var) => {
            if !vars.contains(var) {
                vars.push(var.clone());
            }
        }
        Term::Atom(_, args, _) => {
            for arg in args {
                collect_term_vars(arg, vars);
            }
        }
        _ => {}
    }
}

fn term_is_bound(term: &Term, bound_vars: &Vec<String>) -> bool {
    match term {
        Term::Var(var) => bound_vars.contains(var),
        Term::Atom(_, args, _) => args.iter().all(|arg| term_is_bound(arg, bound_vars)),
        _ => true,
    }
}

fn rewrite_query_atom(atom: &Atom, bound_flags: &Vec<bool>) -> Atom {
    let suffix = bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let name = format!("{}_{}", atom.atom_name(), suffix);
    let args = atom.atom_args().iter().cloned().collect::<Vec<_>>();
    let negated = atom.atom_negated();

    Term::Atom(name, args, negated)
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

    let name = format!("{}_{}", atom.atom_name(), name_suffix);
    let args = atom
        .atom_args()
        .iter()
        .cloned()
        .map(|atom| term_with_suffix(atom, &var_suffix))
        .collect::<Vec<_>>();
    let negated = atom.atom_negated();

    Term::Atom(name, args, negated)
}

fn rewrite_fact_atom(atom: &Atom, head_bound_flags: &Vec<bool>) -> Atom {
    let var_suffix = head_bound_flags
        .into_iter()
        .map(|bound| if *bound { "b" } else { "f" })
        .collect::<Vec<_>>()
        .join("");

    let name = format!("{}", atom.atom_name());
    let args = atom
        .atom_args()
        .iter()
        .cloned()
        .map(|atom| term_with_suffix(atom, &var_suffix))
        .collect::<Vec<_>>();
    let negated = atom.atom_negated();

    Term::Atom(name, args, negated)
}

fn adorn_program(query: &Atom, defs: Defs) -> (Atom, Defs) {
    let mut adorned_defs: Defs = HashMap::new();
    let mut adorned_rules = HashSet::new();

    // only rule atoms in `adorn_atoms` should be rules in defs
    let mut adorn_atoms = vec![];

    let query_name = query.atom_name().to_string();
    let query_arity = query.atom_arity();

    let query_bound_flags = query
        .atom_args()
        .iter()
        .map(|arg| {
            if let Term::Var(_) = arg {
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
        let atom_spec = format!("{}/{}", name, bound_flags.len());
        let rule = defs.get(&atom_spec).unwrap();

        let mut head_bounded_vars = vec![];
        for i in 0..bound_flags.len() {
            if bound_flags[i] {
                collect_term_vars(&rule.head.atom_args()[i], &mut head_bounded_vars);
            }
        }

        let head_ = rewrite_rule_atom(&rule.head, &bound_flags, &bound_flags);
        let mut clauses_ = Vec::with_capacity(rule.clauses.len());

        for clause in rule.clauses.iter() {
            let mut clause_ = Vec::with_capacity(clause.len());

            // starts with head bound vars for each rule clause
            let mut clause_bounded_vars = head_bounded_vars.clone();

            for atom in clause.iter() {
                let name = atom.atom_name();
                let arity = atom.atom_arity();
                let atom_spec = format!("{}/{}", name, arity);

                if defs.contains_key(&atom_spec) {
                    /* atom is a rule */
                    let mut atom_bound_flags = Vec::with_capacity(arity.into());

                    for i in 0..arity.into() {
                        atom_bound_flags.push(term_is_bound(&atom.atom_args()[i], &clause_bounded_vars));
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
                for arg in atom.atom_args() {
                    collect_term_vars(arg, &mut clause_bounded_vars);
                }
            }

            clauses_.push(clause_);
        }

        let adorned_rule_key = head_.atom_spec();
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
    let query_rules =
        collect_rules(&query, db_helper);

    if query_rules.is_empty() {
        /* try eval query as fact */
        let answer =
            eval_db_fact(&query, HashMap::new(), db_helper);
        QueryAnswer { query, answer }
    } else {
        /* implies query is a rule */
        let renamed_vars: Defs =
            analysis::rename_vars(query_rules);
        let (adorned_query, adorned_rules) =
            adorn_program(&query, renamed_vars);
        let adorned_strata =
            analysis::stratify_rules(adorned_rules).unwrap();
        let query_program = QueryProgram {
            query: adorned_query,
            strata: adorned_strata,
        };
        let mut query_result =
            initialize_query_result(&query_program);
        if std::env::var("DEBUG_STRATA").is_ok() {
            for (i, stratum) in query_program.strata.iter().enumerate() {
                let mut keys = stratum.keys().cloned().collect::<Vec<_>>();
                keys.sort();
                println!("stratum {i}: {keys:?}");
            }
        }
        eval_until_fixpoint(&query_program, &mut query_result, db_helper);
        if std::env::var("DEBUG_STRATA").is_ok() {
            for (key, val) in query_result.answers.iter() {
                println!("{key} -> {:?}", val);
            }
        }
        // println!("{query_program:?}");
        // println!("{query_result:?}");
        let answer =
            query_result
            .answers
            .get(&(query_program.query.atom_spec()))
            .unwrap()
            .iter()
            .filter_map(|atom| unify::unify(&query_program.query, atom))
            .collect::<Vec<Bindings>>();
        QueryAnswer { query, answer }
    }
}

fn initialize_query_result(program: &QueryProgram) -> QueryResult {
    if std::env::var("DEBUG_ITERS").is_ok() {
        eprintln!("initialize_query_result start");
    }
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
        let atom_spec = atom.atom_spec();

        for stratum in program.strata.iter() {
            if let Some(rule) = stratum.get(&atom_spec) {
                if let Some(bindings) = unify::unify(&rule.head, &atom) {
                    let filtered = unify::filter_bound(&bindings);
                    let set = inputs.get_mut(&atom_spec).unwrap();

                    if set.is_empty() && atom_bound_args_ground(&atom) {
                        set.push(filtered.clone());
                    }

                    for clause in rule.clauses.iter() {
                        for atom in clause.iter() {
                            if let Some(set) = inputs.get(&atom.atom_spec()) {
                                if set.is_empty() && atom_bound_args_ground(atom) {
                                    let atom = unify::substitute_bindings(atom, &bindings);
                                    if atom_bound_args_ground(&atom) {
                                        stack.push(atom);
                                    }
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
    let debug_iters = std::env::var("DEBUG_ITERS").is_ok();
    let mut iter = 0usize;
    if debug_iters {
        eprintln!("enter eval_until_fixpoint");
    }
    loop {
        let mut strata_changed = false;

        for stratum in program.strata.iter() {
            let mut inner_iters = 0usize;
            loop {
                result.inputs_changed = false;
                result.answers_changed = false;

                let (local_inputs_changed, local_answers_changed) =
                    eval_stratum(&program.strata, stratum, result, db_helper);

                if result.inputs_changed || result.answers_changed {
                    strata_changed = true;
                }

                if !local_inputs_changed && !local_answers_changed {
                    break;
                }

                inner_iters += 1;
                if debug_iters {
                    eprintln!(
                        "inner iter {} inputs_changed={} answers_changed={}",
                        inner_iters, result.inputs_changed, result.answers_changed
                    );
                }
                if inner_iters > 10_000 {
                    panic!("inner fixpoint did not converge");
                }
            }
        }

        iter += 1;
        if debug_iters {
            println!(
                "fixpoint iter {} inputs_changed={} answers_changed={}",
                iter, result.inputs_changed, result.answers_changed
            );
        }
        if iter > 10_000 {
            panic!("fixpoint did not converge after {iter} iterations");
        }

        if !strata_changed {
            break;
        }
    }
}

fn eval_stratum<DB: DBAux>(
    strata: &Strata,
    stratum: &Defs,
    result: &mut QueryResult,
    db_helper: &mut DB,
) -> (bool, bool) {
    let debug_iters = std::env::var("DEBUG_ITERS").is_ok();
    let mut iter = 0usize;
    let mut local_inputs_changed = false;
    let mut local_answers_changed = false;
    for (atom_spec, rule) in stratum.iter() {
        let before_inputs_len = result.inputs.get(atom_spec).map(|v| v.len()).unwrap_or(0);
        let before_answers_len = result.answers.get(atom_spec).map(|s| s.len()).unwrap_or(0);

        // Seed this predicate's answers with extensional database facts so
        // predicates that have both facts and rules (e.g., base cases) can
        // participate in fixpoint evaluation.
        {
            let head = &rule.head;
            let arity = head.atom_arity();
            let head_name = head.atom_name();
            let base_name = head_name
                .rsplit_once('_')
                .and_then(|(prefix, suffix)| {
                    if suffix.len() == usize::from(arity)
                        && suffix.chars().all(|c| c == 'b' || c == 'f')
                    {
                        Some(prefix.to_string())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| head_name.clone());

            // Query for all facts of this predicate using a wildcard atom so
            // we do not lose facts because of adornment or head shape.
            let fact_atom = Term::Atom(
                base_name.clone(),
                (0..arity).map(|i| Term::Var(format!("_{}", i))).collect(),
                head.atom_negated(),
            );

            if !atom_bound_args_ground(&fact_atom) {
                continue;
            }

            let fact_bindings = eval_db_fact(&fact_atom, HashMap::new(), db_helper);
            let fact_atoms: Vec<_> = fact_bindings
                .into_iter()
                .map(|bindings| unify::substitute_bindings(&fact_atom, &bindings))
                .filter_map(|fact| {
                    if let Term::Atom(_, args, negated) = fact {
                        Some(Term::Atom(head_name.clone(), args, negated))
                    } else {
                        None
                    }
                })
                .collect();

            let set = result.answers.get_mut(atom_spec).unwrap();
            for fact in fact_atoms {
                if set.insert(fact) {
                    result.answers_changed = true;
                }
            }
        }

        let mut derived_atoms = HashSet::new();

        let inputs =
            result
            .inputs
            .get(atom_spec)
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        for input_bindings in inputs {
            let outputs =
                rule
                .clauses
                .iter()
                .flat_map(|clause| {
                    eval_rule_body(
                        strata,
                        stratum,
                        result,
                        db_helper,
                        &clause[..],
                        input_bindings.clone(),
                    )
                })
                .collect::<Vec<_>>();

            for output_bindings in outputs {
                derived_atoms
                .insert(unify::substitute_bindings(&rule.head, &output_bindings));
            }
        }

        let set =
            result
            .answers
            .get_mut(atom_spec)
            .unwrap();

        for derived_atom in derived_atoms {
            if set.insert(derived_atom) {
                result.answers_changed = true;
            }
        }

        iter += 1;
        if debug_iters && iter > 10_000 {
            panic!("stratum {atom_spec} did not converge after {iter} iterations");
        }

        let after_inputs_len = result.inputs.get(atom_spec).map(|v| v.len()).unwrap_or(0);
        let after_answers_len = result.answers.get(atom_spec).map(|s| s.len()).unwrap_or(0);
        if after_inputs_len > before_inputs_len {
            local_inputs_changed = true;
        }
        if after_answers_len > before_answers_len {
            local_answers_changed = true;
        }
    }

    (local_inputs_changed, local_answers_changed)
}

fn eval_rule_body<DB: DBAux>(
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
                eval_rule_body(strata, stratum, result, db_helper, &atoms[1..], bindings)
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
    let atom_spec = atom.atom_spec();
    let mut added_input = false;
    let find_rule = {
        let mut rule_opt = None;
        for stratum in strata.iter() {
            if let Some(rule) = stratum.get(&atom_spec) {
                rule_opt = Some(rule);
                break;
            }
        }
        rule_opt
    };
    let atom_bindings = if let Some(rule) = find_rule {
        // new sub-query case
        if result.inputs.contains_key(&atom_spec) {
            let set =
                result
                .inputs
                .get_mut(&atom_spec)
                .unwrap();
            let atom_ =
                unify::substitute_bindings(atom, &bindings);
            let should_add_input = atom_bound_args_ground(&atom_);
            let fresh_suffix = fresh_var_suffix();
            let fresh_head =
                term_with_suffix(rule.head.clone(), &fresh_suffix);
            let bindings_ =
                unify::unify(&fresh_head, &atom_);

            // If the rule head cannot unify with this atom under current bindings,
            // there is nothing to propagate.
            if should_add_input && bindings_.is_some() {
                let bindings_ = bindings_.unwrap();
                let mut head_vars = vec![];
                collect_term_vars(&rule.head, &mut head_vars);

                let bindings_vec: Vec<(String, Term)> =
                    bindings_
                    .into_iter()
                    .map(|(var, term)| {
                        let mut var = var
                            .strip_suffix(&fresh_suffix)
                            .unwrap_or(&var)
                            .to_string();
                        if var.ends_with('_') {
                            var.pop();
                        }
                        (var, term_without_suffix(term, &fresh_suffix))
                    })
                    .filter(|(var, term)| !term_contains_var(term, var))
                    .collect();

                let bindings_clean: Bindings = bindings_vec
                    .into_iter()
                    .filter(|(var, _)| head_vars.contains(var))
                    .collect();

                if bindings_clean.is_empty() && !head_vars.is_empty() {
                    return vec![];
                }

                let new = unify::filter_bound(&bindings_clean);

                if !set.contains(&new) {
                    set.push(new);
                    result.inputs_changed = true;
                    added_input = true;
                }
            }
            // let atom1 = unify::substitute_bindings(atom, &bindings);
            // let bindings1 = unify::unify(&atom1, &rule.head).unwrap();
            // if set.insert(unify::substitute_bindings(&rule.head, &bindings1)) {
            //     result.inputs_changed = true;
            // }
        }

        let answers =
            result
            .answers
            .get(&atom_spec)
            .unwrap();

        let rule_bindings =
            answers
            .iter()
            .filter_map(|answer| {
                unify::unify_with_bindings(atom, answer, bindings.clone())
            })
            .collect::<Vec<_>>();

        rule_bindings
    } else {
        let fact_bindings =
            eval_db_fact(atom, bindings.clone(), db_helper);

        fact_bindings
    };

    return if atom.atom_negated() {
        if added_input {
            vec![]
        } else if atom_bindings.is_empty() {
            vec![bindings]
        } else {
            vec![]
        }
    } else {
        atom_bindings
    };
}

fn eval_db_fact<DB: DBAux>(atom: &Atom, bindings: Bindings, db_helper: &mut DB) -> Vec<Bindings> {
    let search_term =
        unify::substitute_bindings(atom, &bindings);

    db_helper
    .lookup(search_term)
    .filter_map(|(_, fact)| {
        unify::unify_with_bindings(atom, &fact, bindings.clone())
    })
    .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::analysis;
    use crate::lang::parse::parse_program;
    use crate::lang::parse::Parser;
    use crate::lang::parse::Statement;
    use crate::lang::parse::AssertionKind;
    use crate::lang::scan::Scanner;

    #[derive(Debug)]
    struct TestDBAux {
        defs: HashMap<(String, u8), Rule>,
        facts: Vec<Atom>,
        query: Option<Atom>,
    }

    impl TestDBAux {
        fn from_string(program: &str) -> Result<Self, anyhow::Error> {
            let scanner = Scanner::new(program);
            let mut parser = Parser::new(scanner);
            let program = parse_program(&mut parser)?;

            let mut defs: HashMap<(String, u8), Rule> = HashMap::new();
            let mut facts = vec![];
            let mut query = None;

            for stmt in program {
                match stmt {
                    Statement::Assertion(kind) => {
                        match kind {
                            AssertionKind::Fact { head} => {
                                facts.push(head);
                            },
                            AssertionKind::RuleClause { head, body } => {
                                let name = head.atom_name();
                                let arity = head.atom_arity();

                                defs
                                .entry((name.into(), arity))
                                .and_modify(|rule| {
                                    rule
                                    .clauses
                                    .push(body.clone());
                                })
                                .or_insert(Rule { head, clauses: vec![body]});
                            },
                        }
                    },
                    Statement::Query(term) => {
                        query = Some(term);
                    },
                    Statement::Retraction(kind) => {
                        unreachable!();
                    },
                }
            }

            Ok(TestDBAux {
                defs,
                facts,
                query,
            })
        }
    }

    impl DBAux for TestDBAux {
        fn get_rule(&self, name: &str, arity: u8) -> Option<Rule> {
            self.defs.get(&(name.into(), arity)).cloned()
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

    fn symbol(name: &str) -> Term {
        Term::Sym(name.to_string())
    }

    fn var(name: &str) -> Term {
        Term::Var(name.to_string())
    }

    fn atom(name: &str, args: Vec<Term>) -> Term {
        Term::Atom(name.to_string(), args, false)
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
                    head: atom("b", vec![var("v1"), var("v2"), var("v3")]),
                    clauses: vec![vec![
                        atom("a", vec![var("v1"), var("v2")]),
                        atom("b", vec![var("v4"), var("v3")]),
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
    //         adorned_query: atom("b_bf", vec![atom("a"), var("Y")]),
    //         adorned_defs:
    //             HashMap::from_iter(vec![
    //                 (format!("{}/{}", "b_bf", 2), Rule {
    //                     head: atom("b_bf", vec![var("v1_bf"), var("v2_bf")]),
    //                     clauses: vec![vec![
    //                         atom("a", vec![var("v1_bf"), var("v3_bf")]),
    //                         atom("a", vec![var("v3_bf"), var("v2_bf")]),
    //                         atom("succeed_", vec![]),
    //                     ]],
    //                 }),
    //                 (format!("{}/{}", "succeed_", 0), Rule {
    //                     head: atom("succeed_", vec![]),
    //                     clauses: vec![
    //                         vec![atom("true_", vec![])],
    //                     ],
    //                 }),
    //                 (format!("{}/{}", "true_", 0), Rule {
    //                     head: atom("true_", vec![]),
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
    //         adorned_query: atom("b_bf", vec![atom("a"), var("Y")]),
    //         adorned_defs:
    //             HashMap::from_iter(vec![
    //                 (format!("{}/{}", "b_ff", 2), Rule {
    //                     head: atom("b_ff", vec![var("v1_ff"), var("v2_ff")]),
    //                     clauses: vec![vec![
    //                         atom("a", vec![var("v1_ff"), var("v3_ff")]),
    //                         atom("a", vec![var("v3_ff"), var("v2_ff")]),
    //                         atom("b_ff", vec![var("v4_ff"), var("v5_ff")]),
    //                     ]],
    //                 }),
    //                 (format!("{}/{}", "b_bf", 2), Rule {
    //                     head: atom("b_bf", vec![var("v1_bf"), var("v2_bf")]),
    //                     clauses: vec![vec![
    //                         atom("a", vec![var("v1_bf"), var("v3_bf")]),
    //                         atom("a", vec![var("v3_bf"), var("v2_bf")]),
    //                         atom("b_ff", vec![var("v4_bf"), var("v5_bf")]),
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
                atom("reachable", vec![symbol("b"), symbol("c")]),
                atom("reachable", vec![symbol("b"), symbol("d")]),
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
                atom("reachable", vec![symbol("a"), symbol("b")]),
                atom("reachable", vec![symbol("a"), symbol("c")]),
                atom("reachable", vec![symbol("a"), symbol("d")]),
                atom("reachable", vec![symbol("b"), symbol("c")]),
                atom("reachable", vec![symbol("b"), symbol("d")]),
                atom("reachable", vec![symbol("c"), symbol("c")]),
                atom("reachable", vec![symbol("c"), symbol("d")]),
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
                atom("link", vec![symbol("a"), symbol("b")]),
                atom("link", vec![symbol("b"), symbol("c")]),
                atom("link", vec![symbol("c"), symbol("c")]),
                atom("link", vec![symbol("c"), symbol("d")]),
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
            node(X) :- link(Y, X).
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
                atom("unreachable", vec![symbol("z"), symbol("z")]),
                atom("unreachable", vec![symbol("z"), symbol("y")]),
                atom("unreachable", vec![symbol("z"), symbol("x")]),
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
            HashSet::from([atom("indirect", vec![symbol("x"), symbol("z")]),])
        );
        Ok(())
    }

    #[test]
    fn qsq_functor_test() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            person(alice, cons(apple, cons(pear, nil))).
            person(bob, cons(lemon, nil)).
            person(dave, nil).

            first(cons(H, T), H).

            my_first_fruit(Name, F) :-
              person(Name, Fs),
              first(Fs, F).

            my_first_fruit(Name, Fruit)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                atom("my_first_fruit", vec![symbol("alice"), symbol("apple")]),
                atom("my_first_fruit", vec![symbol("bob"), symbol("lemon")]),
            ])
        );
        Ok(())
    }

    #[test]
    fn qsq_functor_len_test() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            person(alice, cons(apple, cons(pear, nil))).
            person(bob, cons(lemon, nil)).
            person(dave, nil).

            len(nil, z).
            len(cons(H, T), s(N)) :- len(T, N).

            num_fruits(Name, Num) :-
              person(Name, Fs),
              len(Fs, Num).

            num_fruits(Name, Num)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                atom("num_fruits", vec![symbol("alice"), atom("s", vec![atom("s", vec![symbol("z")])])]),
                atom("num_fruits", vec![symbol("bob"), atom("s", vec![symbol("z")])]),
                atom("num_fruits", vec![symbol("dave"), symbol("z")]),
            ])
        );

        Ok(())
    }

    #[test]
    fn qsq_functor_len_alice_test() -> Result<(), anyhow::Error> {
        let mut db_helper = TestDBAux::from_string(
            "
            person(alice, cons(apple, cons(pear, nil))).
            person(bob, cons(lemon, nil)).
            person(dave, nil).

            len(nil, z).
            len(cons(H, T), s(N)) :- len(T, N).

            num_fruits(Name, Num) :-
              person(Name, Fs),
              len(Fs, Num).

            num_fruits(alice, Num)?
        ",
        )?;

        let answer = eval_query(db_helper.query.clone().unwrap(), &mut db_helper);
        assert_eq!(
            answer.get_atoms(),
            HashSet::from([
                atom("num_fruits", vec![symbol("alice"), atom("s", vec![atom("s", vec![symbol("z")])])]),
            ])
        );

        Ok(())
    }
}
