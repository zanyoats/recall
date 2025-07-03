use std::collections::HashSet;
use std::iter::IntoIterator;

use crate::lang::scan::Scanner;
use crate::lang::parse;
use crate::lang::parse::Parser;
use crate::lang::parse::Term;
use crate::lang::parse::Literal;
use crate::lang::parse::FunctorTerm;
use crate::lang::unify::Bindings;
use crate::lang::unify;
use crate::eval::QueryEvaluator;
use crate::storage::db;
use crate::storage::tuple::ParameterType;
use crate::errors;

pub struct NaiveEvaluator;

pub struct NaiveResultIter {
    iter: std::vec::IntoIter<FunctorTerm>,
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

    fn eval_declarations(
        &self,
        txn: &'a db::TransactionOp,
        program: &parse::TypedProgram,
    ) -> Result<(), anyhow::Error> {
        // check on declarations used in non-head positions
        for ((name, arity), scm) in program.schemas.iter() {
            let key = (name.to_string(), *arity);
            if !program.facts.contains(&key) && !program.defs.contains_key(&key) {
                if !txn.exists(name, scm)? {
                    return Err(errors::RecallError::RuntimeError(format!(
                        "declaration not found in db: {}/{}",
                        name, arity,
                    )).into());
                }
            }
        }

        // declare facts
        for (name, arity) in program.facts.iter() {
            let scm =
                program
                .schemas
                .get(&(name.to_string(), *arity))
                .unwrap();

            txn.declare(name, scm)?;
        }

        // define rules
        for ((name, arity), rules) in program.defs.iter() {
            let rule_text =
                rules
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("");

            let scm =
                program
                .schemas
                .get(&(name.to_string(), *arity))
                .unwrap();

            txn.define(name, &rule_text, scm)?;
        }

        Ok(())
    }

    fn evaluate_assertion(
        &self,
        txn: &'a db::TransactionOp,
        term: &FunctorTerm,
    ) -> Result<(), anyhow::Error> {
        if let Term::Functor(name, args) = term {
            let input =
                args
                .into_iter()
                .map(|arg| arg.into())
                .collect::<Vec<ParameterType>>();
            let arity = input.len().try_into().unwrap();

            txn.assert(&name, arity, &input)?;

            Ok(())
        } else {
            unreachable!()
        }
    }

    fn evaluate_query(
        &self,
        txn: &'a db::TransactionOp,
        query: &FunctorTerm,
    ) -> Result<Self::Iter, anyhow::Error> {
        let (facts0, rules) = collect_and_parse_rules(txn)?;

        let mut facts =
            txn
            .query(txn.prefix_all())?
            .map(|(_, term)| term)
            .collect::<Vec<_>>();
        facts.extend(facts0.into_iter());

        Ok(eval_query(query, rules, facts))
    }

    fn evaluate_retraction(
        &self,
        txn: &'a db::TransactionOp,
        literal: &Literal,
    ) -> Result<(), anyhow::Error> {
        let name = literal.head.functor_name();
        let arity = literal.head.functor_arity();

        if let Some(is_rule) = txn.is_rule(name, arity)? {
            if is_rule {
                txn.forget(name, arity)?;
            } else {
                let tuples =
                    txn
                    .query(txn.prefix_all())?;

                for (id, term) in tuples {
                    if let Some(_) = unify::unify(&literal.head, &term) {
                        let name = term.functor_name();
                        let arity = term.functor_arity();
                        txn.retract(name, arity, id)?;
                    }
                }
            }
        } else {
            if crate::get_verbose() {
                eprintln!("WARN: {}/{} not found for literal '{}'", name, arity, literal);
            }
        }

        Ok(())
    }
}

fn collect_and_parse_rules(txn: &db::TransactionOp) -> Result<(Vec<FunctorTerm>, Vec<Literal>), anyhow::Error> {
    let rules =
        txn
        .rules()
        .collect::<Vec<_>>()
        .join("");

    let scanner = Scanner::new(&rules);
    let mut parser = Parser::new(scanner);
    let program = parse::parse_program(&mut parser)?;

    let mut facts = vec![];
    let mut rules = vec![];
    for stmt in program.statements {
        if let parse::Statement::Assertion(literal) = stmt {
            // a rule is like a fact if its arity is 0 and has empty body
            if literal.head.functor_arity() == 0 && literal.body.len() == 0 {
                facts.push(literal.head);
            } else {
                rules.push(literal)
            }
        }
    }

    Ok((facts, rules))
}

impl Iterator for NaiveResultIter {
    type Item = FunctorTerm;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

fn eval_query(
    query: &FunctorTerm,
    rules: Vec<Literal>,
    facts: Vec<FunctorTerm>,
) -> NaiveResultIter {
    let mut k = 0;

    if crate::get_verbose() {
        eprintln!("INFO: Generation {}", k);
    }
    let mut learned= HashSet::from_iter(
        facts
        .into_iter()
        .map(|fact| {
            if crate::get_verbose() {
                eprintln!("INFO: {} (new)", fact);
            }
            fact
        })
    );

    // iterate until fixpoint: condition when no new facts are learnt
    loop {
        k += 1;
        if can_halt_iterations(&rules, &mut learned, k) {
            break;
        }
    };

    if crate::get_verbose() {
        eprintln!("INFO: completed in {} generations", k);
    }

    NaiveResultIter {
        iter:
            eval_predicate(query, &learned, None)
            .into_iter()
            .map(move |bindings| {
                unify::substitute_bindings(query, &bindings)
            })
            .collect::<Vec<_>>()
            .into_iter(),
    }
}

fn can_halt_iterations<'a>(
    rules: &'a [Literal],
    learned: &'a mut HashSet<FunctorTerm>,
    k: i32,
) -> bool {
    if crate::get_verbose() {
        eprintln!("INFO: Generation {}", k);
    }

    let deduced: Vec<FunctorTerm> =
        rules
        .iter()
        .flat_map(|rule| {
            eval_rule(rule, learned)
            .into_iter()
            .map(|bindings| {
                unify::substitute_bindings(&rule.head, &bindings)
            })
            .collect::<Vec<FunctorTerm>>()
        })
        .collect();

    let halt =
        deduced
        .into_iter()
        .map(|predicate| {
            if learned.insert(predicate.clone()) {
                if crate::get_verbose() {
                    eprintln!("INFO: {} (new)", predicate);
                }
                false
            } else {
                if crate::get_verbose() {
                    eprintln!("INFO: {}", predicate);
                }
                true
            }
        })
        .collect::<Vec<bool>>()
        .into_iter()
        .all(|b| b);

    halt
}

fn eval_rule<'a>(
    rule: &'a Literal,
    learned: &'a HashSet<FunctorTerm>,
) -> Vec<Bindings<'a>> {
    eval_and(&rule.body[..], learned, None)
}

fn eval_and<'a>(
    predicates: &'a[FunctorTerm],
    learned: &'a HashSet<FunctorTerm>,
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
    predicate0: &'a FunctorTerm,
    learned: &'a HashSet<FunctorTerm>,
    env: Option<Bindings<'a>>,
) -> impl Iterator<Item = Bindings<'a>> {
    learned
    .iter()
    .filter_map(move |predicate| {
        match env.clone() {
            Some(bindings) =>
                unify::unify_with_bindings(predicate0, predicate, bindings),
            None =>
                unify::unify(predicate0, predicate),
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
        let mut facts = vec![];
        let mut rules = vec![];
        let mut queries = vec![];

        let program = "
            link(a, b).
            link(b, c).
            link(c, c).
            link(c, d).
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        let program = parse_program(&mut parser);
        let program = program.unwrap();

        for stmt in program.statements.into_iter() {
            match stmt {
                Statement::Assertion(stmt) => {
                    facts.push(stmt.head);
                },
                _ => unreachable!(),
            }
        }

        let program = "
            same(X) :- link(X, X).
            link(X, Y)?
            same(X)?
        ";
        let scanner = Scanner::new(program);
        let mut parser = Parser::new(scanner);
        let program = parse_program(&mut parser);
        let program = program.unwrap();

        for stmt in program.statements.into_iter() {
            match stmt {
                Statement::Query(stmt) => {
                    queries.push(stmt)
                },
                Statement::Assertion(stmt) => {
                    rules.push(stmt);
                },
                _ => unreachable!(),
            }
        }

        assert_eq!(facts.len(), 4);
        assert_eq!(rules.len(), 1);
        assert_eq!(queries.len(), 2);
        let result =
            eval_query(&queries[0], rules.clone(), facts.clone())
            .collect::<HashSet<FunctorTerm>>();

        assert_eq!(result, HashSet::from_iter(vec![
            Term::Functor("link".to_string(), vec![Term::Atom("a".to_string()), Term::Atom("b".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("b".to_string()), Term::Atom("c".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("c".to_string()), Term::Atom("c".to_string())]),
            Term::Functor("link".to_string(), vec![Term::Atom("c".to_string()), Term::Atom("d".to_string())]),
        ].into_iter()));

        let result =
            eval_query(&queries[1], rules, facts)
            .collect::<HashSet<FunctorTerm>>();

        assert_eq!(result, HashSet::from_iter(vec![
            Term::Functor("same".to_string(), vec![Term::Atom("c".to_string())]),
        ].into_iter()));
    }
}
