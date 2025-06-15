use std::collections::HashSet;
use std::iter::IntoIterator;

use crate::lang::scan::Scanner;
use crate::lang::parse;
use crate::lang::parse::Parser;
use crate::lang::parse::Literal;
use crate::lang::parse::Predicate;
use crate::lang::unify::Bindings;
use crate::lang::unify::unify;
use crate::lang::unify::unify_with_bindings;
use crate::lang::unify::substitute_bindings;
use crate::QueryEvaluator;
use crate::storage::db;
use crate::errors;

pub struct NaiveEvaluator;

pub struct NaiveResultIter {
    iter: std::vec::IntoIter<Predicate>,
}

fn eval_query(
    query: Predicate,
    rules: Vec<Literal>,
    facts: Vec<Predicate>,
) -> NaiveResultIter {
    let mut learned= HashSet::from_iter(
        facts
        .into_iter()
    );

    // iterate until fixpoint: condition when no new facts are learnt
    let mut _k = 0;
    while !can_halt_iterations(&rules, &mut learned) {
        _k += 1;
    }

    // eprintln!("DEBUG: completed in {} iterations", k);

    NaiveResultIter {
        iter:
            eval_predicate(&query.clone(), &learned, None)
            .into_iter()
            .map(move |bindings| {
                substitute_bindings(&query, &bindings)
            })
            .collect::<Vec<_>>()
            .into_iter(),
    }
}

impl Default for NaiveResultIter {
    fn default() -> Self {
        NaiveResultIter {
            iter: Vec::new().into_iter(),
        }
    }
}

impl<'a> QueryEvaluator<'a> for NaiveEvaluator {
    type Iter = NaiveResultIter;

    fn evaluate(
        &self,
        query: Predicate,
        txn: &'a db::TransactionOp,
    ) -> Result<Self::Iter, errors::RecallError> {
        // collect all the rules from the database
        let mut rules = vec![];

        for rule_text in txn.rules() {
            let scanner = Scanner::new(&rule_text);
            let parser = Parser::new(scanner);
            let rule = parse::parse_rule(parser)?;
            rules.push(rule);
        }

        // collect all the facts from the database
        let mut facts = vec![];

        for predicate in txn.predicates() {
            for term in predicate.into_iter() {
                facts.push(term);
            }
        }

        Ok(eval_query(query, rules, facts))
    }
}

impl Iterator for NaiveResultIter {
    type Item = Predicate;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

fn can_halt_iterations<'a>(rules: &'a [Literal], learned: &'a mut HashSet<Predicate>) -> bool {
    let deduced: Vec<Predicate> =
        rules
        .iter()
        .flat_map(|rule| {
            eval_rule(rule, learned)
            .into_iter()
            .map(|bindings| {
                substitute_bindings(&rule.head, &bindings)
            })
            .collect::<Vec<Predicate>>()
        })
        .collect();

    let halt =
        deduced
        .into_iter()
        .map(|predicate| {
            learned.insert(predicate) == false
        })
        .collect::<Vec<bool>>()
        .into_iter()
        .all(|b| b);

    halt
}

fn eval_rule<'a>(
    rule: &'a Literal,
    learned: &'a HashSet<Predicate>,
) -> Vec<Bindings<'a>> {
    eval_and(&rule.body[..], learned, None)
}

fn eval_and<'a>(
    predicates: &'a[Predicate],
    learned: &'a HashSet<Predicate>,
    env: Option<Bindings<'a>>,
) -> Vec<Bindings<'a>> {
    match predicates.first() {
        Some(predicate) =>
            eval_predicate(predicate, learned, env)
            .into_iter()
            .flat_map(|bindings| {
                eval_and(&predicates[1..], learned, Some(bindings))
            })
            .collect(),
        None =>
            env
            .map_or(
                vec![],
                |bindings| vec![bindings],
            ),
    }
}

fn eval_predicate<'a>(
    predicate0: &'a Predicate,
    learned: &'a HashSet<Predicate>,
    env: Option<Bindings<'a>>,
) -> impl Iterator<Item = Bindings<'a>> {
    learned
    .iter()
    .filter_map(move |predicate| {
        match env.clone() {
            Some(bindings) =>
                unify_with_bindings(predicate0, predicate, bindings),
            None =>
                unify(predicate0, predicate),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::lang::scan::Scanner;
    use crate::lang::parse::parse_program;
    use crate::lang::parse::Parser;
    use crate::lang::parse::Statement;
    use crate::lang::parse::Term;

    #[test]
    fn it_evals_datalog() {
        let program = "
            link(a, b).
            link(b, c).
            link(c, c).
            link(c, d).
            same(X) :- link(X, X).
            link(X, Y)?
            same(X)?
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        let program = parse_program(&mut parser);
        let program = program.unwrap();
        let mut facts = vec![];
        let mut rules = vec![];
        let mut queries = vec![];
        for stmt in program.statements.into_iter() {
            match stmt {
                Statement::Query(stmt) => {
                    queries.push(stmt)
                },

                Statement::Asserted(stmt) => {
                    if stmt.body.len() > 0 {
                        rules.push(stmt);
                    } else {
                        facts.push(stmt.head);
                    }
                },

                _ => unreachable!(),
            }
        }
        assert_eq!(facts.len(), 4);
        assert_eq!(rules.len(), 1);
        assert_eq!(queries.len(), 2);
        let result =
            eval_query(queries[0].clone(), rules.clone(), facts.clone())
            .collect::<HashSet<Predicate>>();

        assert_eq!(result, HashSet::from_iter(vec![
            Term::Functor("link".to_string(), vec![Term::Atom("a".to_string()), Term::Atom("b".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("b".to_string()), Term::Atom("c".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("c".to_string()), Term::Atom("c".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("c".to_string()), Term::Atom("d".to_string())]),
        ].into_iter()));

        let result =
            eval_query(queries[1].clone(), rules, facts)
            .collect::<HashSet<Predicate>>();

        assert_eq!(result, HashSet::from_iter(vec![
            Term::Functor("same".to_string(), vec![Term::Atom("c".to_string())]),
        ].into_iter()));
    }
}
