use std::collections::HashSet;

use crate::lang::Program;
use crate::lang::Term;
use crate::lang::Rule;
use crate::lang::Bindings;

use crate::eval::Eval;
use crate::eval::DbEngine;

pub struct Naive<'a> {
    query_head: &'a Term,
    rules: Vec<&'a Rule>,
    learned: HashSet<Term>,
    iterations: u64,
}

impl<'a> Eval<'a> for Naive<'a> {
    fn from_term<T: DbEngine>(
        query: &'a Term,
        program: Option<&'a Program>,
        db_engine: &mut T,
    ) -> Self {
        let mut rules = vec![];
        let mut learned = HashSet::new();
        Self::extend_from_program(&mut rules, &mut learned, program);
        Self::extend_from_edb(&mut learned, db_engine);
        Naive {
            query_head: &query,
            rules,
            learned,
            iterations: 0,
        }
    }

    fn from_rule<T: DbEngine>(
        query: &'a Rule,
        program: Option<&'a Program>,
        db_engine: &mut T,
    ) -> Self {
        let mut rules = vec![];
        let mut learned = HashSet::new();
        rules.push(query);
        Self::extend_from_program(&mut rules, &mut learned, program);
        Self::extend_from_edb(&mut learned, db_engine);
        Naive {
            query_head: &query.head,
            rules,
            learned,
            iterations: 0,
        }
    }

    fn execute(mut self) -> Vec<Term> {
        loop {
            let EvalResult { halt } = self.iterate();
            self.iterations += 1;
            if halt { break }
        }
        eprintln!("DEBUG: completed in {} iterations", self.iterations);
        self.result()
    }
}

struct EvalResult {
    halt: bool,
}

impl<'a> Naive<'a> {
    fn extend_from_program(
        rules: &mut Vec<&'a Rule>,
        learned: &mut HashSet<Term>,
        program: Option<&'a Program>,
    ) {
        program.into_iter().for_each(|program| {
            let program_rules =
                program
                .rules
                .iter();
            let program_facts =
                program
                .facts
                .iter()
                .map(|x| x.clone() );
            rules.extend(program_rules);
            learned.extend(program_facts);
        });
    }

    fn extend_from_edb<T: DbEngine>(
        learned: &mut HashSet<Term>,
        db_engine: &mut T,
    ) {
        learned.extend(db_engine.get_stored_terms());
    }

    fn iterate(&mut self) -> EvalResult {
        // evaluate all the rules, if this iteration produces
        // no new learned predicates, then halt
        let derived: Vec<Term> =
            self
            .rules
            .iter()
            .flat_map(|&rule| {
                eval_rule(rule, &self.learned)
                .into_iter()
                .map(|bindings| {
                    Term::subsitute_bindings(&rule.head, &bindings)
                })
                .collect::<Vec<Term>>()
            })
            .collect();

        let halt =
            derived
            .into_iter()
            .map(|predicate| {
                self.learned.insert(predicate) == false
            })
            .collect::<Vec<bool>>()
            .into_iter()
            .all(|b| b);

        EvalResult { halt }
    }

    fn result(&self) -> Vec<Term> {
        eval_predicate(&self.query_head, &self.learned, None)
        .into_iter()
        .map(|bindings| {
            Term::subsitute_bindings(&self.query_head, &bindings)
        })
        .collect()
    }
}

fn eval_rule<'a>(rule: &'a Rule, learned: &'a HashSet<Term>) -> Vec<Bindings<'a>>  {
    let predicates: Vec<&Term> = rule.body.iter().collect();
    eval_and(&predicates[..], learned)
}

fn eval_and<'a>(
    predicates: &[&'a Term],
    learned: &'a HashSet<Term>,
) -> Vec<Bindings<'a>> {
    eval_and_aux(predicates, learned, None)
}

fn eval_and_aux<'a>(
    predicates: &[&'a Term],
    learned: &'a HashSet<Term>,
    env: Option<Bindings<'a>>,
) -> Vec<Bindings<'a>> {
    match predicates.first() {
        Some(&predicate) => {
            eval_predicate(predicate, learned, env)
            .into_iter()
            .flat_map(|bindings| {
                eval_and_aux(&predicates[1..], learned, Some(bindings))
            })
            .collect()
        }
        None => {
            env.map_or(
                vec![],
                |bindings| vec![bindings],
            )
        }
    }
}

fn eval_predicate<'a>(
    predicate: &'a Term,
    learned: &'a HashSet<Term>,
    env: Option<Bindings<'a>>,
) -> Vec<Bindings<'a>> {
    learned
    .iter()
    .filter_map(move |term| {
        eval_step(
            predicate,
            term,
            env.clone(),
        )
    })
    .collect()
}

fn eval_step<'a>(
    predicate: &'a Term,
    base: &'a Term,
    env: Option<Bindings<'a>>,
) -> Option<Bindings<'a>> {
    match env {
        Some(bindings) => {
            Term::unify_with_bindings(predicate, base, bindings)
        },
        None => Term::unify(predicate, base),
    }
}
