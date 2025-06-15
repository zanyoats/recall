use std::iter;

pub mod naive;

use crate::errors;
use crate::lang::scan::Scanner;
use crate::lang::parse::Program;
use crate::lang::parse;
use crate::lang::parse::Statement;
use crate::lang::parse::Term;
use crate::lang::parse::Literal;
use crate::lang::parse::Parser;
use crate::lang::unify;
use crate::storage::db;
use crate::storage::format;
use crate::storage::tuple::ParameterType;
use crate::QueryEvaluator;

pub fn iter_eval<'a, Engine: QueryEvaluator<'a>>(
    program: Program,
    txn: &'a db::TransactionOp,
    evaluator: Engine,
) -> Result<Engine::Iter, errors::RecallError> {
    for decl in program.declarations {
        eval_decl(txn, decl)?;
    }

    for stmt in program.statements {
        match stmt {
            Statement::Asserted(stmt) => {
                eval_asserted(txn, stmt)?;
            },
            Statement::Retracted(stmt) => {
                eval_retracted(txn, stmt)?;
            },
            Statement::Query(stmt) => {
                return eval_query(txn, &evaluator, stmt)
            },
        }
    }

    return Ok(Engine::Iter::default());
}

pub fn batch_eval<'a, Engine: QueryEvaluator<'a>>(
    program: Program,
    txn: &'a db::TransactionOp,
    evaluator: Engine,
) -> Result<Vec<parse::Predicate>, errors::RecallError> {
    for decl in program.declarations {
        eval_decl(txn, decl)?;
    }

    // eval datalog program
    let mut result: Vec<parse::Predicate> = vec![];

    for stmt in program.statements {
        match stmt {
            Statement::Asserted(stmt) => {
                eval_asserted(txn, stmt)?;
            },
            Statement::Retracted(stmt) => {
                eval_retracted(txn, stmt)?;
            },
            Statement::Query(stmt) => {
                result.extend(eval_query(txn, &evaluator, stmt)?);
            },
        }
    }

    Ok(result)
}

fn eval_decl(
    txn: &db::TransactionOp,
    decl: Term,
) -> Result<(), errors::RecallError> {
    let name = parse::Predicate::name(&decl);
    let mut tup_scm = vec![];
    let args = parse::Predicate::arguments(&decl);

    // validate declaration
    for arg in args {
        if let Term::Atom(s) = arg {
            tup_scm.push(match s.as_str() {
                "atom" => format::ATOM_TYPE,
                "string" => format::STRING_TYPE,
                "int" => format::INT_TYPE,
                _ => return Err(errors::RecallError::TypeError(format!(
                    "unrecognize parameter type for predicate '{}'", name,
                ))),
            })
        } else {
            return Err(errors::RecallError::TypeError(format!(
                "declaration parameters should consist of atom terms",
            )))
        }
    }

    // define predicate
    let _ = txn.define_predicate(name, tup_scm)?;

    Ok(())
}

fn eval_asserted(
    txn: &db::TransactionOp,
    stmt: Literal,
) -> Result<(), errors::RecallError> {
    if stmt.body.len() > 0 {
        txn.assert_rule(stmt.to_string())?;
    } else {
        let name = parse::Predicate::name(&stmt.head);
        let args = parse::Predicate::arguments(&stmt.head);
        let arity = args.len().try_into().unwrap();

        // ensure term args do not contain a functor or var term
        for arg in args {
            match arg {
                Term::Var(_) => return Err(errors::RecallError::TypeError(format!(
                    "ground facts cannot contain arguments with variables",
                ))),
                Term::Functor(_, _) => return Err(errors::RecallError::TypeError(format!(
                    "ground facts cannot contain arguments with functors",
                ))),
                _ => {}
            }
        }
        let predicate = txn.get_predicate(name, arity)?;
        let param_types: Vec<ParameterType> = stmt.head.into();
        predicate.assert(param_types)?;
    }

    Ok(())
}

fn eval_retracted(
    txn: &db::TransactionOp,
    stmt: Literal,
) -> Result<(), errors::RecallError> {
    if stmt.body.len() > 0 {
        for rule_text in txn.rules() {
            let scanner = Scanner::new(&rule_text);
            let parser = Parser::new(scanner);
            let rule = parse::parse_rule(parser)?;
            if rule.body.len() != stmt.body.len() {
                continue
            }
            let init =
                unify::unify(&rule.head, &stmt.head);
            let rule_bindings =
                iter::zip(
                    rule.body.iter(),
                    stmt.body.iter(),
                )
                .fold(init, |acc, (a, b)| {
                    if let Some(bindings) = acc {
                        unify::unify_with_bindings(a, b, bindings)
                    } else {
                        None
                    }
                });
            if rule_bindings.is_some() {
                txn.retract_rule(rule_text)?;
            }
        }
    } else {
        for predicate in txn.predicates() {
            let name = predicate.name.clone();
            let arity: i32 = predicate.tup_scm.len().try_into().unwrap();
            for term in predicate.into_iter() {
                if let Some(_) = unify::unify(&term, &stmt.head) {
                    let predicate = txn.get_predicate(&name, arity - 1)?;
                    let param_types: Vec<ParameterType> = term.into();
                    predicate.retract(param_types)?;
                }
            }
        }
    }

    Ok(())
}

fn eval_query<'a, Engine: QueryEvaluator<'a>>(
    txn: &'a db::TransactionOp,
    evaluator: &Engine,
    stmt: parse::Predicate,
) -> Result<Engine::Iter, errors::RecallError> {
    evaluator.evaluate(stmt, txn)
}
