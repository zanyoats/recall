use std::collections::HashSet;

use crate::lang::parse::Literal;
use crate::lang::parse::Predicate;
use crate::lang::unify::Bindings;
use crate::lang::unify::unify;
use crate::lang::unify::unify_with_bindings;
use crate::lang::unify::substitute_bindings;

pub fn eval_query<'a>(
    query: &'a Predicate,
    facts: &'a [&'a Predicate],
    rules: &'a [&'a Literal],
) -> Vec<Predicate> {
    let mut state = QueryState::new(query, facts, rules);
    let mut k = 0;
    // iterate until fixpoint: condition when no new facts are learnt
    while !can_halt_iterations(&mut state) {
        k += 1;
    }
    eprintln!("DEBUG: completed in {} iterations", k);
    get_result(&state)
}

struct QueryState<'a> {
    query: &'a Predicate,
    rules: &'a [&'a Literal],
    learned: HashSet<Predicate>,
}

impl<'a> QueryState<'a> {
    fn new(
        query: &'a Predicate,
        facts: &'a [&'a Predicate],
        rules: &'a [&'a Literal],
    ) -> Self {
        let learned= HashSet::from_iter(
            facts
            .into_iter()
            .map(|&fact| fact.clone())
        );
        QueryState {
            query,
            rules,
            learned,
        }
    }
}

fn get_result(state: &QueryState) -> Vec<Predicate> {
    eval_predicate(&state.query, &state.learned, None)
    .into_iter()
    .map(|bindings| {
        substitute_bindings(&state.query, &bindings)
    })
    .collect()
}

fn can_halt_iterations(state: &mut QueryState) -> bool {
    let deduced: Vec<Predicate> =
        state
        .rules
        .iter()
        .flat_map(|&rule| {
            eval_rule(rule, &state.learned)
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
            state.learned.insert(predicate) == false
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
    use super::*;
    use crate::lang::scan::Scanner;
    use crate::lang::parse::parse_program;
    use crate::lang::parse::Parser;
    use crate::lang::parse::Statement;

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
        for stmt in program.statements.iter() {
            if let Statement::Query(stmt) = stmt {
                queries.push(stmt);
            }

            if let Statement::Asserted(stmt) = stmt {
                if stmt.body.len() > 0 {
                    rules.push(stmt);
                } else {
                    facts.push(&stmt.head);
                }
            }
        }
        assert_eq!(facts.len(), 4);
        assert_eq!(rules.len(), 1);
        assert_eq!(queries.len(), 2);
        let result = eval_query(queries[0], &facts[..], &rules[..]);
        println!("{result:?}");
        let result = eval_query(queries[1], &facts[..], &rules[..]);
        println!("{result:?}");
    }
}
