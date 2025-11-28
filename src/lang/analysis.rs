use std::collections::HashMap;

use crate::errors;
use crate::lang;
use crate::lang::parse::Program;
use crate::lang::parse::Statement;
use crate::lang::parse::AssertionKind;
use crate::lang::{Defs, Strata, Atom, Term, Rule};

/// TODO: make sure all functor names are <= 127 length
/// see src/storage/sizedbuf.rs:write_atom_offset

////////////////////////////////////////////////////////////////////////////////
/// Ensure the range restriction property is not violated
///
/// Violates: foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Z).
///   -> variable 'Y' is not mentioned in the rule body.
/// Ok:       foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Y).
///   -> variables 'X', 'Y', and 'Z' are mentioned in rule body.
////////////////////////////////////////////////////////////////////////////////

pub fn check_range_restriction_property(program: &Program) -> Result<(), anyhow::Error> {
    for stmt in program.iter() {
        if let Statement::Assertion(kind) = stmt {
            if let AssertionKind::RuleClause { head, body } = kind {
                let name = head.atom_name();
                let arity = head.atom_arity();
                let head_vars = collect_variables(head);
                let body_vars = body
                    .iter()
                    .flat_map(collect_variables)
                    .collect::<Vec<&String>>();
                for head_var in head_vars {
                    if !body_vars.contains(&head_var) {
                        return Err(errors::RecallError::TypeError(format!(
                            "variable '{}' in head of {}/{} not found in body",
                            head_var, name, arity,
                        ))
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
/// Ensure negation safety condition
///
/// Violates: foo(X, Y, Z) :- bar(X), baz(Y), not quuz(Z).
///   -> variable 'Z' is not used in a positive position
/// Ok:       foo(X, Y, Z) :- bar(X), baz(Y), bax(Z), not quuz(Z).
///   -> variable 'Z' is used in a positive position with 'bax(Z)'
////////////////////////////////////////////////////////////////////////////////

pub fn check_negation_safety_condition(program: &Program) -> Result<(), anyhow::Error> {
    for stmt in program.iter() {
        if let Statement::Assertion(kind) = stmt {
            if let AssertionKind::RuleClause { head, body } = kind {
                let name = head.atom_name();
                let arity = head.atom_arity();

                let mut pos: Vec<&String> = vec![];
                let mut neg: Vec<&String> = vec![];
                for atom in body.iter() {
                    if atom.atom_negated() {
                        neg.extend(collect_variables(atom).iter());
                    } else {
                        pos.extend(collect_variables(atom).iter());
                    }
                }

                for neg_var in neg {
                    if !pos.contains(&neg_var) {
                        return Err(errors::RecallError::TypeError(format!(
                            "variable '{}' in head of {}/{} not found in body in a positive position",
                            neg_var, name, arity,
                        ))
                        .into());
                    }
                }
            }
        }
    }

    Ok(())
}

// Rename original var names to be `v#` which will never clash since user var
// names have to start with a uppercase letter or underscore.
pub fn rename_vars(defs: HashMap<(String, u8), Rule>) -> lang::Defs {
    let mut result: Defs = HashMap::new();

    let mut k = 0u32;

    for ((name, arity), rule) in defs {
        let atom_spec = format!("{}/{}", name, arity);

        let head_suffixes = (0..arity)
            .map(|_| {
                k = k.checked_add(1).unwrap();
                k
            })
            .collect::<Vec<_>>();

        let mut head_renames = HashMap::new();

        // work on head atoms
        let head = {
            let (name, args, negated) = rule.head.atom_parts();

            debug_assert_eq!(args.len(), arity.into());

            let mut args_ = Vec::with_capacity(args.len());

            for (i, atom) in args.into_iter().enumerate() {
                args_.push(match atom {
                    Term::Var(var) => {
                        let var_ = format!("v{}", head_suffixes[i]);
                        head_renames.insert(var, var_.clone());
                        Term::Var(var_)
                    },
                    _ => atom,
                });
            }

            Term::Atom(name, args_, negated)
        };

        let mut clauses: Vec<Vec<Atom>> = Vec::with_capacity(rule.clauses.len());

        // work on body atoms
        for clause in rule.clauses {
            let mut clause_renames = head_renames.clone();

            let mut clause_: Vec<Atom> = vec![];
            for atom in clause {
                let (name, args, negated) = atom.atom_parts();

                let mut args_ = Vec::with_capacity(args.len());

                for atom in args {
                    args_.push(match atom {
                        Term::Var(var) => {
                            if let Some(var_) = clause_renames.get(&var) {
                                Term::Var(var_.to_string())
                            } else {
                                k = k.checked_add(1).unwrap();
                                let var_ = format!("v{}", k);
                                clause_renames.insert(var, var_.clone());
                                Term::Var(var_)
                            }
                        }
                        _ => atom,
                    });
                }

                clause_.push(Term::Atom(name, args_, negated));
            }

            clauses.push(clause_);
        }

        result.insert(
            atom_spec,
            lang::Rule { head, clauses },
        );
    }

    result
}

////////////////////////////////////////////////////////////////////////////////
/// Stratify program
////////////////////////////////////////////////////////////////////////////////

pub fn stratify_rules(defs: Defs) -> Result<Strata, anyhow::Error> {
    use tools::*;

    let rules_list = defs.keys().collect::<Vec<_>>();
    let n = rules_list.len();
    let rules_map: HashMap<&String, usize> = HashMap::from_iter(
        rules_list
            .iter()
            .enumerate()
            .map(|(i, &functor_spec)| (functor_spec, i)),
    );

    // println!("rules list: {rules_list:?}");
    // println!("rules map: {rules_map:?}");

    // labelled negated edges
    let mut negated: Graph<bool> = vec![vec![]; n];

    // labelled aggregated edges
    // let mut aggregated: Graph<bool> = vec![vec![]; n];

    // build precedence graph from rules
    let mut p = Precedence::new(n);

    for (head_spec, rule) in defs.iter() {
        for clause in rule.clauses.iter() {
            // construct an edge from each rule in body to its rule head
            for atom in clause.iter() {
                let body_spec = atom.atom_spec();

                // exclude facts in body as they do not contribute to edges in the graph
                if let Some(&u) = rules_map.get(&body_spec) {
                    // println!("add edge for {body_spec} -> {head_spec}");
                    let v = rules_map[head_spec];

                    if p.add(u, v) {
                        // label negated edges
                        negated[u].push(atom.atom_negated());

                        // // label aggregated edges
                        // aggregated[u].push(rule.head.functor_aggregated());
                    } else {
                        let v_index = p.graph[u].iter().position(|t| *t == v).unwrap();
                        negated[u][v_index] = negated[u][v_index] || atom.atom_negated();
                    }
                }
            }
        }
    }

    // println!("graph: {:?}", p.graph);

    let cycles = collect_graph_cycles(&p.graph);

    // Detect if a cycle path contains at least:
    //   one negative edge
    //   one aggregated edge
    for cycle in cycles {
        let mut i = 0;
        let mut j = 1;
        while j < cycle.len() {
            let u = cycle[i];
            let v = cycle[j];
            let v_index = p.graph[u].iter().position(|t| *t == v).unwrap();

            if negated[u][v_index] == true {
                return Err(errors::RecallError::RuntimeError(format!(
                    "The rules {} form a cycle that contains a negative (using \"not\") link. This is not allowed because the program cannot be stratified.",
                    cycle
                    .iter()
                    .map(|u| rules_list[*u].to_string())
                    .collect::<Vec<_>>()
                    .join(" -> "),
                )).into());
            }

            // if aggregated[u][v_index] == true {
            //     return Err(errors::RecallError::RuntimeError(format!(
            //         "The rules {} form a cycle that contains a aggregated link. This is not allowed because the program cannot be stratified.",
            //         cycle
            //         .iter()
            //         .map(|u| rules_list[*u].to_string())
            //         .collect::<Vec<_>>()
            //         .join(" -> "),
            //     )).into());
            // }

            i += 1;
            j += 1;
        }
    }

    // compute strata as strongly connected components
    let mut priority = vec![];
    let mut components = vec![];
    visit_graph(&p.graph, &mut priority);
    assign_graph(&p.transposed, priority, &mut components);

    // println!("components {components:?}");
    Ok(components
        .into_iter()
        .map(|component| {
            component
                .into_iter()
                .map(|i| {
                    let functor_spec = rules_list[i];
                    let key = functor_spec.to_string();
                    let value = defs[functor_spec].clone();
                    (key, value)
                })
                .collect()
        })
        .collect::<Vec<_>>())
}

/// helpers

fn collect_variables(term: &Term) -> Vec<&String> {
    match term {
        Term::Var(s) => vec![s],
        Term::Atom(_, terms, _) => terms.iter().flat_map(collect_variables).collect(),
        _ => vec![],
    }
}

fn collect_graph_cycles(g: &tools::Graph<usize>) -> Vec<Vec<usize>> {
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
                    continue;
                }

                if visited[*v] {
                    let mut cycle = vec![*v, u];
                    let mut p = parent[u];

                    while p >= 0 {
                        cycle.push(p as usize);
                        if p as usize == *v {
                            break;
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

        pub fn add(&mut self, u: usize, v: usize) -> bool {
            assert!(u < self.graph.len());
            assert!(v < self.graph.len());
            if self.edges.insert((u, v)) {
                self.edges.insert((u, v));
                self.graph[u].push(v);
                self.transposed[v].push(u);
                true
            } else {
                false
            }
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

    pub fn assign_graph(tg: &Graph<usize>, priority: Vec<usize>, components: &mut Vec<Vec<usize>>) {
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
        use super::*;
        use std::collections::HashMap;

        #[test]
        fn it_can_get_prioties_from_visit_vertex() {
            let vertices = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
            let map: HashMap<char, usize> =
                HashMap::from_iter(vertices.iter().enumerate().map(|(i, ch)| (*ch, i)));
            let n = map.len();
            let mut p = Precedence::new(n);
            for (u, v) in vec![
                ('a', 'b'),
                ('a', 'b'),
                ('b', 'c'),
                ('b', 'd'),
                ('b', 'e'),
                ('c', 'a'),
                ('e', 'f'),
                ('e', 'g'),
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
            let result = components
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
    use crate::lang::parse;
    use crate::lang::scan;

    fn collect_rules(program: &str) -> Result<Defs, anyhow::Error> {
        let mut defs: HashMap<(String, u8), Rule> = HashMap::new();
        let scanner = scan::Scanner::new(program);
        let mut parser = parse::Parser::new(scanner);
        let program = parse::parse_program(&mut parser)?;
        for stmt in program {
            if let Statement::Assertion(kind) = stmt {
                if let AssertionKind::RuleClause { head, body } = kind {
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
                }
            }
        }

        let rules = rename_vars(defs);
        Ok(rules)
    }

    #[test]
    fn it_checks_range_restriction_property() -> Result<(), anyhow::Error> {
        {
            // valid
            let program = "
                foo.
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            check_range_restriction_property(&program)?;
        }

        {
            // valid
            let program = "
                foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Y).
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            check_range_restriction_property(&program)?;
        }

        {
            // invalid
            let program = "
                foo(X, Y, Z) :- bar(X), baz(Z, X), quux(Z).
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            let err = check_range_restriction_property(&program).unwrap_err();
            assert!(err
                .to_string()
                .contains("variable 'Y' in head of foo/3 not found in body"));
        }

        Ok(())
    }

    #[test]
    fn it_checks_negation_safety_condition() -> Result<(), anyhow::Error> {
        {
            // valid
            let program = "
                foo.
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            check_negation_safety_condition(&program)?;
        }

        {
            // valid
            let program = "
                foo(X, Y, Z) :- bar(X), baz(Y), bax(Z), not quuz(Z).
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            check_negation_safety_condition(&program)?;
        }

        {
            // invalid
            let program = "
                foo(X, Y, Z) :- bar(X), baz(Y), not quuz(Z).
            ";
            let scanner = scan::Scanner::new(program);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser)?;
            let err = check_negation_safety_condition(&program).unwrap_err();
            assert!(err.to_string().contains(
                "variable 'Z' in head of foo/3 not found in body in a positive position"
            ));
        }

        Ok(())
    }

    #[test]
    fn it_stratifies_programs() -> Result<(), anyhow::Error> {
        {
            // p-p cycle
            let program = "
                p :- p.
            ";
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 1);
        }

        {
            // p-q cycle
            let program = "
                p :- q.
                q :- p.
            ";
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 1);
        }

        {
            // a_b_c_cycle
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
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 5);
        }

        {
            // no_cycle
            let program = "
                d. # dummy rule
                e :- d.
                f :- e.
                f :- g.
                g :- e.
            ";
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 4);
        }

        {
            // valid usage of "not"
            let program = "
                a. # dummy rule
                b :- a.
                c :- not b.
            ";
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 3);
        }

        {
            // p-q cycle with "not"
            let program = "
                p :- not q.
                q :- not p.
            ";
            let rules = collect_rules(program)?;
            let err = stratify_rules(rules.clone()).unwrap_err();
            assert!(err.to_string().contains("cycle that contains a negative"));
        }

        {
            // handles double negative bug
            let program = "
                a :- c.
                b :- not a.
                b :- not a.
                c :- a.
            ";
            let rules = collect_rules(program)?;
            let strata = stratify_rules(rules.clone())?;
            assert_eq!(strata.len(), 2);
        }

        Ok(())
    }
}
