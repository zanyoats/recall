use std::iter;

mod naive;

use crate::errors;
use crate::lang::parse::Program;
use crate::lang::parse::Literal;
use crate::lang::parse::Predicate;
use crate::lang::parse::Term;
use crate::lang::unify;
use crate::storage::db::DB;
use crate::storage::engine::btree::format;
use crate::storage::engine::btree::format::tuple::ParameterType;

// TODO: create a EvalIterator for handling infinite result sets
pub fn eval(program: Program, mut db: DB) -> Result<Vec<Predicate>, errors::RecallError> {
    use crate::lang::parse::Statement::*;

    // process declarations
    for decl in &program.declarations {
        let name = Predicate::name(decl);
        let args = Predicate::arguments(decl);
        let mut tup_scm = vec![];

        // validate declaration
        for arg in args {
            if let Term::Atom(s) = arg {
                tup_scm.push(match s.as_str() {
                    "atom" => format::ATOM_TYPE,
                    "string" => format::STRING_TYPE,
                    "bytes" => format::BYTES_TYPE,
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
        let _ = db.define_predicate(name, tup_scm)?;
    }

    // eval datalog program
    let mut result = vec![];
    let mut rules = vec![];

    for stmt in &program.statements {
        match stmt {
            Asserted(stmt) => {
                if stmt.body.len() > 0 {
                    rules.push(stmt);
                } else {
                    let name = Predicate::name(&stmt.head);
                    let args = Predicate::arguments(&stmt.head);
                    let arity = args.len().try_into().unwrap();
                    let predicate = db.get_predicate(name, arity)?;
                    let mut args1 = vec![];
                    for arg in args {
                        args1.push(match arg {
                            Term::Atom(s) => ParameterType::Atom(s.to_string()),
                            Term::Str(s) => ParameterType::String(s.to_string()),
                            Term::Integer(n) => ParameterType::Int(*n),
                            _ => return Err(errors::RecallError::TypeError(format!(
                                "ground facts cannot contain arguments with variables or functors",
                            ))),
                        });
                    }
                    predicate.assert(&args1)?;
                }
            },
            Retracted(target) => {
                println!("ignoring retracted expressions");
                // if target.body.len() > 0 {
                //     mem.rules =
                //         mem
                //         .rules
                //         .iter()
                //         .filter_map(|rule| {
                //             if rule.body.len() != target.body.len() {
                //                 return Some(rule.clone())
                //             }

                //             let init =
                //                 unify::unify(&rule.head, &target.head);
                //             let rule_bindings =
                //                 iter::zip(
                //                     rule.body.iter(),
                //                     target.body.iter(),
                //                 )
                //                 .fold(init, |acc, (a, b)| {
                //                     if let Some(bindings) = acc {
                //                         unify::unify_with_bindings(a, b, bindings)
                //                     } else {
                //                         None
                //                     }
                //                 });

                //             if rule_bindings.is_some() {
                //                 None
                //             } else {
                //                 Some(rule.clone())
                //             }
                //         })
                //         .collect();
                // } else {
                //     mem.facts =
                //         mem
                //         .facts
                //         .iter()
                //         .filter_map(|predicate| {
                //             if let Some(_) = unify::unify(&predicate, &target.head) {
                //                 None
                //             } else {
                //                 Some(predicate.clone())
                //             }
                //         })
                //         .collect();
                // }
            },
            Query(stmt) => {
                // collect all the facts from the database
                let mut facts = vec![];
                let predicates = db.get_predicates_object_btree()?;

                for key in predicates.key_into_iter()? {
                    let name = key[0].get_atom();
                    let arity = key[1].get_uint();
                    let predicate = db.get_predicate(name, arity)?;

                    for val in predicate.iter_owned() {
                        facts.push(
                            Predicate::new(
                                name.to_string(),
                                val
                                .into_iter()
                                .map(|param| {
                                    match param {
                                        ParameterType::Atom(s) => Term::Atom(s),
                                        ParameterType::Int(n) => Term::Integer(n),
                                        ParameterType::String(s) => Term::Str(s),
                                        _ => unreachable!(),
                                    }
                                })
                                .collect(),
                            )
                        );
                    }

                }

                // naive evaluation
                let facts =
                    facts
                    .iter()
                    .map(|predicate| predicate)
                    .collect::<Vec<&Predicate>>();
                result.extend(
                    naive::eval_query(&stmt, &facts[..], &rules[..])
                    .into_iter()
                );
            },
        }
    }

    db.save();

    Ok(result)
}
