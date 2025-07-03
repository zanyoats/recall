use std::collections::HashMap;
use std::collections::HashSet;

use crate::lang::parse::Term;
use crate::lang::parse::FunctorTerm;
use crate::lang::parse::Literal;
use crate::lang::parse::Statement;
use crate::lang::parse::Program;
use crate::lang::parse::TypedProgram;
use crate::lang::parse::TypedStatement;
use crate::lang::unify;
use crate::storage::format;
use crate::errors;

// TODO: make sure all atoms are <= 127 length
// see src/storage/sizedbuf.rs:write_atom_offset

fn convert_to_typed_term(term: &Term) -> Term {
    match term {
        Term::Atom(_) => Term::Atom("atom".to_string()),
        Term::Integer(_) => Term::Atom("int".to_string()),
        Term::Str(_) => Term::Atom("string".to_string()),
        Term::Var(s) => Term::Var(s.to_string()),
        Term::Functor(_, _) => unreachable!(),
    }
}

fn convert_to_typed_functor(functor: &FunctorTerm) -> FunctorTerm {
    Term::functor(
        functor.functor_name().to_string(),
        functor.functor_args().iter().map(convert_to_typed_term).collect(),
    )
}

fn convert_typed_to_scm(decl: &FunctorTerm) -> Vec<u8> {
    decl
    .functor_args()
    .iter()
    .map(|term| match term {
        Term::Atom(s) if s == "atom" => format::ATOM_TYPE,
        Term::Atom(s) if s == "string" => format::STRING_TYPE,
        Term::Atom(s) if s == "int" => format::INT_TYPE,
        _ => unreachable!(),
    })
    .collect()
}

pub fn typecheck(program: Program) -> Result<TypedProgram, anyhow::Error> {
    let mut defs = HashMap::new();
    let mut facts = HashSet::new();
    let mut computed_decls = HashMap::new();
    let mut statements = vec![];

    // Typed form of program only concerns assertions (rules and facts). Not
    // included are statements that are queries and retractions. These are not
    // typechecked.
    let mut typed_program= vec![];

    // preload with user provided declarations
    for user_decl in program.declarations {
        let name = user_decl.functor_name();
        let arity = user_decl.functor_arity();

        let valid_decl=
            user_decl
            .functor_args()
            .into_iter()
            .all(|term| {
                match term {
                    Term::Atom(s)
                        if s == "atom"
                        || s == "int"
                        || s == "string" => true,
                    _ => false,
                }
            });
        if !valid_decl {
            return Err(errors::RecallError::TypeError(format!(
                "declaration for predicate {}/{} does not contain valid type \
                specifiers: atom, int, or string",
                name, arity,
            )).into())
        }

        let key = (name.to_string(), arity);
        computed_decls
        .entry(key)
        .or_insert_with(|| user_decl.clone());

        // Ensure this user declaration, if restated, is consistent with the
        // prior declaration.
        let key = &(name.to_string(), arity);
        let computed_decl = &computed_decls[key];
        if unify::unify(computed_decl, &user_decl).is_none() {
            return Err(errors::RecallError::TypeError(format!(
                "inconsistent declarations for {}/{}",
                name, arity,
            )).into());
        }
    }

    // build statments (for later evaluation) and typed form of program
    for stmt in program.statements {
        match stmt {
            Statement::Query(functor) => {
                statements.push(TypedStatement::Query(functor));
            },

            Statement::Retraction(literal) => {
                statements.push(TypedStatement::Retraction(literal));
            },

            Statement::Assertion(literal) => {
                let name = literal.head.functor_name();
                let arity = literal.head.functor_arity();

                let typed_literal = Literal {
                    head: convert_to_typed_functor(&literal.head),
                    body:
                        literal
                        .body
                        .iter()
                        .map(convert_to_typed_functor)
                        .collect()
                };

                let key = (name.to_string(), arity);
                computed_decls
                .entry(key)
                .or_insert_with(|| typed_literal.head.clone());

                // add typed form of assertion literal
                typed_program.push(typed_literal);

                // If assertion is a rule add it to defs, otherwise add it to
                // one of the statements.
                let key = (name.to_string(), arity);
                if literal.body.len() > 0 || arity == 0 {
                    let existing_rules =
                        defs
                        .entry(key)
                        .or_insert(vec![]);

                    existing_rules.push(literal);
                } else {
                    facts.insert((name.to_string(), arity));
                    statements.push(TypedStatement::Fact(literal.head));
                }
            },
        }
    }

    // ensure rules use declared functors in bodies
    for typed_literal in typed_program.iter() {
        for functor in typed_literal.body.iter() {
            let name = functor.functor_name();
            let arity = functor.functor_arity();

            if !computed_decls.contains_key(&(name.clone(), arity)) {
                return Err(errors::RecallError::TypeError(format!(
                    "declaration for {}/{} not found",
                    name, arity,
                )).into());
            }
        }
    }

    // typecheck using typed form of program
    for typed_literal in typed_program {
        let name = typed_literal.head.functor_name();
        let arity = typed_literal.head.functor_arity();

        // compute the unified declaration for each assertion (rule or fact) by
        // first unifying with the head, then the body.
        let computed_decl = &computed_decls[&(name.clone(), arity)];
        let init = unify::unify(&typed_literal.head, computed_decl);
        let result =
            typed_literal
            .body
            .iter()
            .fold(init, |acc, term| {
                if let Some(bindings) = acc {
                    let name = term.functor_name().to_string();
                    let arity = term.functor_arity();
                    let computed_decl = &computed_decls[&(name, arity)];
                    unify::unify_with_bindings(term, computed_decl, bindings)
                } else {
                    None
                }
            });

        // If the unification succeeds, then update the computed decl with these
        // updated bindings.
        if let Some(bindings) = result {
            let updated = unify::substitute_bindings(&typed_literal.head, &bindings);
            let key = (name.to_string(), arity);
            computed_decls.insert(key, updated);
        } else {
            return Err(errors::RecallError::TypeError(format!(
                "could not determine type for {}/{}",
                name, arity,
            )).into());
        }
    }

    // ensure no variable unbound after unifying
    for ((name, arity), typed_functor) in computed_decls.iter() {
        for arg in typed_functor.functor_args() {
            match arg {
                Term::Var(s) => return Err(errors::RecallError::TypeError(format!(
                    "could not determine type for variable {} in {}/{}",
                    s, name, arity,
                )).into()),
                _ => continue,
            }
        }
    }

    // program has typechecked successfully, now we can return a typed program
    // for evaluation.

    if crate::get_verbose() {
        for ((name, arity), typed_functor) in computed_decls.iter() {
            eprintln!("INFO: {}/{} : {}", name, arity, typed_functor);
        }
    }

    // collect all the schemas for rules and facts (assertions)
    let schemas =
        HashMap::from_iter(
            computed_decls
            .into_iter()
            .map(|((name, arity), computed_decl)| {
                let scm = convert_typed_to_scm(&computed_decl);
                ((name, arity), scm)
            })
        );

    Ok(TypedProgram {
        defs,
        facts,
        schemas,
        statements,
    })
}

pub fn check_range_restriction_property(program: &Program) -> Result<(), anyhow::Error> {
    for stmt in program.statements.iter() {
        if let Statement::Assertion(literal) = stmt {
            let name = literal.head.functor_name();
            let head_vars = collect_variables(&literal.head);
            let body_vars =
                literal
                .body
                .iter()
                .flat_map(collect_variables)
                .collect::<Vec<&String>>();
            for head_var in head_vars {
                if !body_vars.contains(&head_var) {
                    return Err(errors::RecallError::TypeError(
                        format!("variable '{}' in head of '{}' not found in body", head_var, name)
                    ).into())
                }
            }
        }
    }

    Ok(())
}

fn collect_variables(term: &Term) -> Vec<&String> {
    match term {
        Term::Var(s) => vec![s],
        Term::Functor(_, terms) =>
            terms
            .iter()
            .flat_map(collect_variables)
            .collect(),
        _ => vec![],
    }
}
