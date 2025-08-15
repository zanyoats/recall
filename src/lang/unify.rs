use std::collections::HashMap;

use crate::lang::parse::Term;

pub type Bindings = HashMap<String, Term>;
pub type BindingPair = (String, Term);
pub type BindingPairs = Vec<BindingPair>;

pub fn from_binding_pairs(pairs: BindingPairs) -> Bindings {
    let mut bindings = HashMap::new();

    for (x, term) in pairs {
        insert_binding(&x, &term, &mut bindings);
    }

    bindings
}

pub fn select_bindings(
    bindings: &Bindings,
    by_vars: &Vec<&str>,
) -> BindingPairs {
    by_vars
    .into_iter()
    .filter_map(|&x| {
        if let Some(term) = lookup(x, bindings) {
            Some((x.to_string(), term))
        } else {
            None
        }
    })
    .collect()
}

pub fn unify(t0: &Term, t1: &Term) -> Option<Bindings> {
    unify_with_bindings(t0, t1, HashMap::new())
}

pub fn unify_with_bindings(
    t0: &Term,
    t1: &Term,
    mut bindings: Bindings,
) -> Option<Bindings> {
    if unify_terms(t0, t1, &mut bindings) {
        Some(bindings)
    } else {
        None
    }
}

pub fn substitute_bindings(term: &Term, bindings: &Bindings) -> Term {
    use Term::*;

    match term {
        Var(val) =>
            lookup(val, bindings)
            .map_or_else(|| Var(val.clone()), |term| {
                substitute_bindings(&term, bindings)
            }),
        Functor(name, terms, negated) => {
            let terms: Vec<Term> =
                terms
                .into_iter()
                .map(|term| {
                    substitute_bindings(term, bindings)
                })
                .collect();
            Functor(name.clone(), terms, *negated)
        }
        _ => term.clone(),
    }
}

fn lookup(x: &str, bindings: &Bindings) -> Option<Term> {
    bindings
    .get(x)
    .cloned()
}

fn unify_terms(t0: &Term, t1: &Term, bindings: &mut Bindings) -> bool {
    use Term::*;

    match (t0, t1) {
        (Atom(x), Atom(y)) =>
            x == y,
        (Str(x), Str(y)) =>
            x == y,
        (Int(x), Int(y)) =>
            x == y,
        (AggVar(func1, _), AggVar(func2, _)) =>
            func1 == func2,
        (Var(x), _) =>
            unify_var(x, t1, bindings),
        (_, Var(y)) =>
            unify_var(y, t0, bindings),
        (Functor(name0, terms0, _), Functor(name1, terms1, _)) => {
            name0 == name1 &&
            terms0.len() == terms1.len() &&
            terms0.iter().zip(terms1.iter()).all(|(t0, t1)| {
                unify_terms(t0, t1, bindings)
            })
        }
        _ => false,
    }
}

fn unify_var(
    x: &str,
    term: &Term,
    bindings: &mut Bindings
) -> bool {
    match lookup(x, bindings) {
        Some(bound) => unify_terms(&bound, term, bindings),
        None => {
            match term {
                Term::Var(y) if x == y => true,
                Term::Var(y) => {
                    match lookup(y, bindings) {
                        Some(bound) => unify_var(x, &bound, bindings),
                        None => insert_binding(x, term, bindings),
                    }
                }
                _ => {
                    if occurs_check(x, term, bindings) {
                        false
                    } else {
                        insert_binding(x, term, bindings)
                    }
                }
            }
        }
    }
}

fn insert_binding(
    x: &str,
    term: &Term,
    bindings: &mut Bindings
) -> bool {
    bindings.insert(x.to_string(), term.clone());
    true
}

fn occurs_check(
    x: &str,
    term: &Term,
    bindings: &mut Bindings
) -> bool {
    match term {
        Term::Var(y) if x == y => true,
        Term::Var(y) => {
            match lookup(y, bindings) {
                Some(term) => occurs_check(x, &term, bindings),
                None => false,
            }
        }
        Term::Functor(_, terms, _) => {
            terms.iter().any(|term| occurs_check(x, term, bindings))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use Term::*;

    #[test]
    fn it_unifies_simple_terms() {
        {
            let mut bindings = HashMap::new();
            let t1 = Atom("foo".to_string());
            let t2 = Atom("foo".to_string());
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Var("X".to_string());
            let t2 = Var("Y".to_string());
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert_eq!(bindings.len(), 1);
            let term = bindings.get("X").unwrap();
            assert!(matches!(term, Var(val) if val == "Y"));
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Var("X".to_string());
            let t2 = Atom("foo".to_string());
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert_eq!(bindings.len(), 1);
            let term = bindings.get("X").unwrap();
            assert!(matches!(term, Atom(val) if val == "foo"));
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Int(42);
            let t2 = Int(42);
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }

        {
            let mut bindings = HashMap::new();
            let t1 = AggVar("count".to_string(), "X".to_string());
            let t2 = AggVar("count".to_string(), "Y".to_string());
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Functor(
                "foo".to_string(),
                vec![Var("X".to_string())],
                false,
            );
            let t2 = Functor(
                "foo".to_string(),
                vec![Var("X".to_string())],
                false,
            );
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }
    }

    #[test]
    fn it_fails_to_unify_terms() {
        {
            let mut bindings = HashMap::new();
            let t1 = Int(42);
            let t2 = Int(69);
            assert!(!unify_terms(&t1, &t2, &mut bindings));
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Atom("foo".to_string());
            let t2 = Atom("bar".to_string());
            assert!(!unify_terms(&t1, &t2, &mut bindings));
        }

        {
            let mut bindings = HashMap::new();
            let t1 = AggVar("min".to_string(), "X".to_string());
            let t2 = AggVar("max".to_string(), "Y".to_string());
            assert!(!unify_terms(&t1, &t2, &mut bindings));
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Functor(
                "foo".to_string(),
                vec![Atom("foo".to_string())],
                false,
            );
            let t2 = Functor(
                "foo".to_string(),
                vec![Atom("bar".to_string())],
                false,
            );
            assert!(!unify_terms(&t1, &t2, &mut bindings));
        }
    }

    #[test]
    fn it_unifies_complex_terms() {
        let t0 = Functor(
            "foo".to_string(),
            vec![
                Var("X".to_string()),
                Var("Y".to_string()),
                Var("Z".to_string()),
            ],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Functor(
                    "bar".to_string(),
                    vec![
                        Var("Z".to_string()),
                    ],
                    false,
                ),
                Atom("foo".to_string()),
            ],
            false,
        );
        let mut bindings = HashMap::new();
        assert!(unify_terms(&t0, &t1, &mut bindings));
        assert_eq!(bindings.len(), 3);
        let x_binding = bindings.get("X").unwrap();
        let y_binding = bindings.get("Y").unwrap();
        let z_binding = bindings.get("Z").unwrap();
        assert!(matches!(x_binding, Var(val) if val == "Y"));
        assert!(matches!(
            y_binding,
            Functor(name, args, _)
                if name == "bar"
                && args.len() == 1
                && matches!(&args[0], Var(val) if val == "Z")
        ));
        assert!(matches!(z_binding, Atom(val) if val == "foo"));
    }

    #[test]
    fn it_fails_to_unify_complex_terms() {
        let t0 = Functor(
            "foo".to_string(),
            vec![Atom("apple".to_string()), Var("X".to_string())],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![Var("X".to_string()), Atom("lemon".to_string())],
            false,
        );
        let mut bindings = HashMap::new();
        assert!(!unify_terms(&t0, &t1, &mut bindings));
    }

    #[test]
    fn it_fails_to_unify_when_occurs_check() {
        let t0 = Functor(
            "foo".to_string(),
            vec![
                Functor(
                    "bar".to_string(),
                    vec![Var("X".to_string())],
                    false,
                ),
            ],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![Var("X".to_string())],
            false,
        );
        let mut bindings = HashMap::new();
        assert!(!unify_terms(&t0, &t1, &mut bindings));
    }

    #[test]
    fn it_unifies_mutual_variables() {
        let t0 = Functor(
            "foo".to_string(),
            vec![
                Var("X".to_string()),
                Var("Y".to_string()),
            ],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("X".to_string()),
            ],
            false,
        );
        let mut bindings = HashMap::new();
        assert!(unify_terms(&t0, &t1, &mut bindings));
        assert_eq!(bindings.len(), 1);
        let term = bindings.get("X").unwrap();
        assert!(matches!(term, Var(val) if val == "Y"));
    }

    #[test]
    fn it_unifies_recursive_variables() {
        let t0 = Functor(
            "foo".to_string(),
            vec![
                Var("X".to_string()),
                Var("Y".to_string()),
                Var("Z".to_string()),
                Var("X".to_string()),
            ],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("Z".to_string()),
                Atom("foo".to_string()),
                Var("R".to_string()),
            ],
            false,
        );
        let mut bindings = HashMap::new();
        assert!(unify_terms(&t0, &t1, &mut bindings));
        assert_eq!(bindings.len(), 4);
        let x_binding = bindings.get("X").unwrap();
        let y_binding = bindings.get("Y").unwrap();
        let z_binding = bindings.get("Z").unwrap();
        let r_binding = bindings.get("R").unwrap();
        assert!(matches!(x_binding, Var(val) if val == "Y"));
        assert!(matches!(y_binding, Var(val) if val == "Z"));
        assert!(matches!(z_binding, Atom(val) if val == "foo"));
        assert!(matches!(r_binding, Atom(val) if val == "foo"));
    }

    #[test]
    fn it_substitutes_bindings() {
        let t0 = Functor(
            "foo".to_string(),
            vec![
                Var("X".to_string()),
                Var("Y".to_string()),
                Var("Z".to_string()),
                Int(42),
                Var("U".to_string()),
                Var("V".to_string()),
            ],
            false,
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("Z".to_string()),
                Atom("foo".to_string()),
                Int(42),
                Var("V".to_string()),
                Var("U".to_string()),
            ],
            false,
        );
        let mut bindings = unify(&t0, &t1).unwrap();
        let term = substitute_bindings(&t0, &mut bindings);
        assert!(matches!(
            term,
            Functor(name, args, _)
                if name == "foo"
                && args.len() == 6
                && matches!(&args[0], Atom(val) if val == "foo")
                && matches!(&args[1], Atom(val) if val == "foo")
                && matches!(&args[2], Atom(val) if val == "foo")
                && matches!(&args[3], Int(val) if *val == 42)
                && matches!(&args[4], Var(val) if val == "V")
                && matches!(&args[5], Var(val) if val == "V")
        ));
    }
}
