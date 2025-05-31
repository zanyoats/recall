use std::iter;

mod naive;

use crate::lang::program::DatalogAST;
use crate::lang::program::Literal;
use crate::lang::program::Predicate;
use crate::lang::unify;

pub struct Memory {
    facts: Vec<Predicate>,
    rules: Vec<Literal>,
}

impl Memory {
    pub fn new() -> Self {
        Memory { facts: vec![], rules: vec![] }
    }

    fn assert(&mut self, literal: Literal) {
        if literal.body.len() > 0 {
            self.rules.push(literal);
        } else {
            self.facts.push(literal.head);
        }
    }
}

// TODO: create a EvalIterator for handling infinite result sets
pub fn eval(ast: DatalogAST, mem: &mut Memory) -> Vec<Predicate> {
    use crate::lang::program::Statement::*;

    let mut result = vec![];

    for stmt in ast.program {
        match stmt {
            Asserted(stmt) => mem.assert(stmt),
            Retracted(target) => {
                if target.body.len() > 0 {
                    mem.rules =
                        mem
                        .rules
                        .iter()
                        .filter_map(|rule| {
                            if rule.body.len() != target.body.len() {
                                return Some(rule.clone())
                            }

                            let init =
                                unify::unify(&rule.head, &target.head);
                            let rule_bindings =
                                iter::zip(
                                    rule.body.iter(),
                                    target.body.iter(),
                                )
                                .fold(init, |acc, (a, b)| {
                                    if let Some(bindings) = acc {
                                        unify::unify_with_bindings(a, b, bindings)
                                    } else {
                                        None
                                    }
                                });

                            if rule_bindings.is_some() {
                                None
                            } else {
                                Some(rule.clone())
                            }
                        })
                        .collect();
                } else {
                    mem.facts =
                        mem
                        .facts
                        .iter()
                        .filter_map(|predicate| {
                            if let Some(_) = unify::unify(&predicate, &target.head) {
                                None
                            } else {
                                Some(predicate.clone())
                            }
                        })
                        .collect();
                }
            },
            Query(stmt) => {
                let facts =
                    mem
                    .facts
                    .iter()
                    .map(|predicate| predicate)
                    .collect::<Vec<&Predicate>>();
                let rules =
                    mem
                    .rules
                    .iter()
                    .map(|literal| literal)
                    .collect::<Vec<&Literal>>();
                result.extend(
                    naive::eval_query(&stmt, &facts[..], &rules[..])
                    .into_iter()
                );
            },
        }
    }

    result
}
