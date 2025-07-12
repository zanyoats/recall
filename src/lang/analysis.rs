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

/// TODO: make sure all functor names are <= 127 length
/// see src/storage/sizedbuf.rs:write_atom_offset

////////////////////////////////////////////////////////////////////////////////
/// Typechecking
////////////////////////////////////////////////////////////////////////////////

fn convert_to_typed_term(term: &Term) -> Term {
    match term {
        Term::Atom(_) => Term::Atom("atom".to_string()),
        Term::Integer(_) => Term::Atom("int".to_string()),
        Term::Str(_) => Term::Atom("string".to_string()),
        Term::Var(s) => Term::Var(s.to_string()),
        Term::Functor(_, _, _) => unreachable!(),
    }
}

fn convert_to_typed_functor(functor: &FunctorTerm) -> FunctorTerm {
    Term::Functor(
        functor.functor_name().to_string(),
        functor.functor_args().iter().map(convert_to_typed_term).collect(),
        false,
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
                if literal.is_rule() {
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

////////////////////////////////////////////////////////////////////////////////
/// Ensure the range restriction property is not violated
///
/// Violates: foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Z).
///   -> variable 'Y' is not mentioned in the rule body.
/// Ok:       foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Y).
///   -> variables 'X', 'Y', and 'Z' are mentioned in rule body.
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
/// Stratify program
////////////////////////////////////////////////////////////////////////////////

pub type Strata = Vec<Vec<Literal>>;

// TODO: check no cycles contain a negated edge, here???
pub fn stratify_rules(rules: Vec<Literal>) -> Result<Strata, anyhow::Error> {
    use tools::*;

    // Need to group all the rules by functor spec (e.g. pair/2)
    let grouped_rules =
        rules
        .into_iter()
        .fold(HashMap::<String, Vec<Literal>>::new(), |mut acc, literal| {
            let rules =
                acc
                .entry(literal.head.functor_spec())
                .or_insert(vec![]);
            rules.push(literal);
            acc
        });

    let rules_list =
        grouped_rules
        .keys()
        .collect::<Vec<_>>();
    let n = rules_list.len();
    let rules_map: HashMap<&String, usize> = HashMap::from_iter(
        rules_list
        .iter()
        .enumerate()
        .map(|(i, functor_spec)| {
            (*functor_spec, i)
        })
    );
    let mut negated: Graph<bool> = vec![vec![]; n];

    // build precedence graph from rules
    let mut p = Precedence::new(n);

    for (head_functor_spec, rules) in grouped_rules.iter() {
        for rule in rules.iter() {
            // construct an edge from each rule in body to it rule head
            for body_functor in rule.body.iter() {
                let body_functor_spec = body_functor.functor_spec();

                if let Some(u) = rules_map.get(&body_functor_spec) {
                    let v = rules_map[head_functor_spec];
                    p.add(*u, v);
                    // label negated edges
                    negated[*u].push(body_functor.functor_negated());
                }
            }
        }
    }

    let cycles = collect_graph_cycles(&p.graph);

    // Detect if a cycle path contains at least one negative edge
    for cycle in cycles {
        let mut i = 0;
        let mut j = 1;
        while j < cycle.len() {
            let u = cycle[i];
            let v = cycle[j];
            let v_index =
                p
                .graph[u]
                .iter()
                .position(|t| *t == v)
                .unwrap();

            if negated[u][v_index] == true {
                return Err(errors::RecallError::RuntimeError(format!(
                    "The rules {} form a cycle that contain a negative (using \"not\") link. This is not allowed because the program cannot be stratified before evaluating.",
                    cycle
                    .iter()
                    .map(|u| rules_list[*u].to_string())
                    .collect::<Vec<_>>()
                    .join(" -> "),
                )).into())
            }

            i += 1;
            j += 1;
        }
    }

    // compute strata as strongly connected components
    let mut priority = vec![];
    let mut components = vec![];
    visit_graph(&p.graph, &mut priority);
    assign_graph(&p.transposed, priority, &mut components);

    Ok(
        components
        .into_iter()
        .map(|component| {
            component
            .into_iter()
            .flat_map(|i| {
                let functor_spec = rules_list[i];
                let rules_for_spec = grouped_rules[functor_spec].clone();
                rules_for_spec
            })
            .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
    )
}


/// helpers

fn collect_variables(term: &Term) -> Vec<&String> {
    match term {
        Term::Var(s) => vec![s],
        Term::Functor(_, terms, _) =>
            terms
            .iter()
            .flat_map(collect_variables)
            .collect(),
        _ => vec![],
    }
}

fn collect_graph_cycles(
    g: &tools::Graph<usize>,
) -> Vec<Vec<usize>> {
    fn visit_vertex(
        g: &tools::Graph<usize>,
        u: usize,
        visited: &mut Vec<bool>,
        parent: &mut Vec<i32>,
        cycles: &mut Vec<Vec<usize>>,
    ) {
        if !visited[u] {
            visited[u] = true;

            for v in g[u].iter() {
                // Collect self-cycles too
                if u == *v {
                    cycles.push(vec![u, u]);
                    continue
                }

                if visited[*v] {
                    let mut cycle = vec![*v, u];
                    let mut p = parent[u];

                    while p >= 0 {
                        cycle.push(p as usize);
                        if p as usize == *v {
                            break
                        }
                        p = parent[p as usize];
                    }
                    if p as usize == *v {
                        cycle.reverse();
                        cycles.push(cycle);
                    }
                } else {
                    parent[*v] = u as i32;
                    visit_vertex(g, *v, visited, parent, cycles);
                }
            }
        }
    }

    let n = g.len();
    let mut visited = vec![false; n];
    let mut parent = vec![-1; n];
    let mut cycles = vec![];

    for (u, _) in g.iter().enumerate() {
        visit_vertex(g, u, &mut visited, &mut parent, &mut cycles);
    }

    cycles
}

mod tools {
    use std::collections::HashSet;

    pub type Graph<T> = Vec<Vec<T>>;

    pub struct Precedence {
        edges: HashSet<(usize, usize)>,
        pub graph: Graph<usize>,
        pub transposed: Graph<usize>,
    }

    impl Precedence {
        pub fn new(n: usize) -> Self {
            Precedence {
                edges: HashSet::new(),
                graph: vec![Vec::new(); n],
                transposed: vec![Vec::new(); n],
            }
        }

        pub fn add(&mut self, u: usize, v: usize) {
            assert!(u < self.graph.len());
            assert!(v < self.graph.len());
            if self.edges.contains(&(u, v)) {
                return
            }
            self.edges.insert((u, v));
            self.graph[u].push(v);
            self.transposed[v].push(u);
        }
    }

    // Kosaraju algorithm to compute SCCs

    pub fn visit_graph(g: &Graph<usize>, priority: &mut Vec<usize>) {
        let n = g.len();
        let mut visited = vec![false; n];

        for (u, _) in g.iter().enumerate() {
            visit_vertex(g, u, &mut visited, priority);
        }

        priority.reverse();
    }

    pub fn visit_vertex(
        g: &Graph<usize>,
        u: usize,
        visited: &mut Vec<bool>,
        priority: &mut Vec<usize>,
    ) {
        if !visited[u] {
            visited[u] = true;

            for v in g[u].iter() {
                visit_vertex(g, *v, visited, priority);
            }

            priority.push(u);
        }
    }

    pub fn assign_graph(
        tg: &Graph<usize>,
        priority: Vec<usize>,
        components: &mut Vec<Vec<usize>>,
    ) {
        assert!(tg.len() == priority.len());
        let n = tg.len();
        let mut visited = vec![false; n];

        for u in priority {
            let mut component = vec![];
            assign_vertex(tg, u, &mut visited, &mut component);
            if !component.is_empty() {
                components.push(component);
            }
        }
    }

    pub fn assign_vertex(
        tg: &Graph<usize>,
        u: usize,
        visited: &mut Vec<bool>,
        component: &mut Vec<usize>,
    ) {
        if !visited[u] {
            visited[u] = true;
            component.push(u);

            for v in tg[u].iter() {
                assign_vertex(tg, *v, visited, component);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use super::*;

        #[test]
        fn it_can_get_prioties_from_visit_vertex() {
            let vertices = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
            let map: HashMap<char, usize> = HashMap::from_iter(
                vertices
                .iter()
                .enumerate()
                .map(|(i, ch)| {
                    (*ch, i)
                })
            );
            let n = map.len();
            let mut p = Precedence::new(n);
            for (u, v) in vec![
                ('a', 'b'), ('a', 'b'),
                ('b', 'c'), ('b', 'd'), ('b', 'e'),
                ('c', 'a'),
                ('e', 'f'), ('e', 'g'),
                ('g', 'f'),
                ('f', 'h'),
                ('h', 'g'),
            ] {
                p.add(map[&u], map[&v]);
            }
            let mut priority = vec![];
            let mut components = vec![];
            visit_graph(&p.graph, &mut priority);
            assign_graph(&p.transposed, priority, &mut components);
            let result =
                components
                .into_iter()
                .map(|component| {
                    component
                    .into_iter()
                    .map(|i| vertices[i])
                    .collect::<HashSet<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(result.len(), 4);
            assert!(result.contains(&HashSet::from(['a', 'b', 'c'])));
            assert!(result.contains(&HashSet::from(['d'])));
            assert!(result.contains(&HashSet::from(['e'])));
            assert!(result.contains(&HashSet::from(['f', 'h', 'g'])));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::scan::Scanner;
    use crate::lang::parse::Parser;
    use crate::lang::parse::Statement;
    use crate::lang::parse::parse_program;

    #[test]
    fn it_stratifies_programs() -> Result<(), anyhow::Error> {
        fn get_rules(program: &str) -> Result<Vec<Literal>, anyhow::Error> {
            let scanner = Scanner::new(program);
            let mut parser = Parser::new(scanner);
            let program = parse_program(&mut parser)?;
            Ok(
                program
                .statements
                .into_iter()
                .map(|stmt| match stmt {
                    Statement::Assertion(stmt) => stmt,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>()
            )
        }

        { // p-p cycle
            let program = "
                p :- p.
            ";
            let rules = get_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 1);
        }

        { // p-q cycle
            let program = "
                p :- q.
                q :- p.
            ";
            let rules = get_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 1);
        }

        { // a_b_c_cycle
            let program = "
                # stratum 0
                b :- a.
                c :- b.
                a :- c.
                # stratum 1
                d :- b.
                d :- c.
                # stratum 2
                e :- b.
                # stratum 3
                f :- e.
                # stratum 4
                g :- e.
            ";
            let rules = get_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 5);
        }

        { // no_cycle
            let program = "
                d. # dummy rule
                e :- d.
                f :- e.
                f :- g.
                g :- e.
            ";
            let rules = get_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 4);
        }

        { // p-q cycle with "not"
            let program = "
                p :- not q.
                q :- not p.
            ";
            let rules = get_rules(program)?;
            let err = stratify_rules(rules.clone()).unwrap_err();
            assert!(err.to_string().contains("cycle that contain a negative"));
        }

        Ok(())
    }
}
