use std::collections::HashMap;

use crate::lang::parse::Term;

pub type Bindings<'a> = HashMap<&'a str, &'a Term>;

pub fn unify<'a>(t0: &'a Term, t1: &'a Term) -> Option<Bindings<'a>> {
    unify_with_bindings(t0, t1, HashMap::new())
}

pub fn unify_with_bindings<'a>(
    t0: &'a Term,
    t1: &'a Term,
    mut bindings: Bindings<'a>,
) -> Option<Bindings<'a>> {
    if unify_terms(t0, t1, &mut bindings) {
        Some(bindings)
    } else {
        None
    }
}

pub fn substitute_bindings<'a>(term: &'a Term, bindings: &Bindings<'a>) -> Term {
    use Term::*;

    match term {
        Var(val) =>
            lookup(val, bindings)
            .map_or_else(|| Var(val.clone()), |term| {
                substitute_bindings(&term, bindings)
            }),
        Functor(name, terms) => {
            let terms: Vec<Term> =
                terms
                .into_iter()
                .map(|term| {
                    substitute_bindings(term, bindings)
                })
                .collect();
            Functor(name.clone(), terms)
        }
        _ => term.clone(),
    }
}

fn lookup<'a>(var_name: &str, bindings: &Bindings<'a>) -> Option<&'a Term> {
    bindings
    .get(var_name)
    .map(|&bound| bound)
}

fn unify_terms<'a>(t0: &'a Term, t1: &'a Term, bindings: &mut Bindings<'a>) -> bool {
    use Term::*;

    match (t0, t1) {
        (Atom(x), Atom(y)) =>
            x == y,
        (Str(x), Str(y)) =>
            x == y,
        (Integer(x), Integer(y)) =>
            x == y,
        (Var(x), _) =>
            unify_var(x, t1, bindings),
        (_, Var(y)) =>
            unify_var(y, t0, bindings),
        (Functor(name0, terms0), Functor(name1, terms1)) => {
            name0 == name1 &&
            terms0.len() == terms1.len() &&
            terms0.iter().zip(terms1.iter()).all(|(t0, t1)| {
                unify_terms(t0, t1, bindings)
            })
        }
        _ => false,
    }
}

fn unify_var<'a>(
    x: &'a str,
    term: &'a Term,
    bindings: &mut Bindings<'a>
) -> bool {
    match bindings.get(x) {
        Some(&bound) => unify_terms(bound, term, bindings),
        None => {
            match term {
                Term::Var(y) if x == y => true,
                Term::Var(y) => {
                    match bindings.get(y.as_str()) {
                        Some(&bound) => unify_var(x, bound, bindings),
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

fn insert_binding<'a>(
    x: &'a str,
    term: &'a Term,
    bindings: &mut Bindings<'a>
) -> bool {
    bindings.insert(x, term);
    true
}

fn occurs_check<'a>(
    x: &'a str,
    term: &'a Term,
    bindings: &mut Bindings<'a>
) -> bool {
    match term {
        Term::Var(y) if x == y => true,
        Term::Var(y) => {
            match bindings.get(y.as_str()) {
                Some(term) => occurs_check(x, term, bindings),
                None => false,
            }
        }
        Term::Functor(_, terms) => {
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
            let t1 = Integer(42);
            let t2 = Integer(42);
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }

        {
            let mut bindings = HashMap::new();
            let t1 = Functor(
                "foo".to_string(),
                vec![Var("X".to_string())],
            );
            let t2 = Functor(
                "foo".to_string(),
                vec![Var("X".to_string())],
            );
            assert!(unify_terms(&t1, &t2, &mut bindings));
            assert!(bindings.is_empty());
        }
    }

    #[test]
    fn it_fails_to_unify_terms() {
        {
            let mut bindings = HashMap::new();
            let t1 = Integer(42);
            let t2 = Integer(69);
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
            let t1 = Functor(
                "foo".to_string(),
                vec![Atom("foo".to_string())],
            );
            let t2 = Functor(
                "foo".to_string(),
                vec![Atom("bar".to_string())],
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
                ),
                Atom("foo".to_string()),
            ],
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
            Functor(name, args)
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
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![Var("X".to_string()), Atom("lemon".to_string())],
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
                ),
            ],
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![Var("X".to_string())],
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
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("X".to_string()),
            ],
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
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("Z".to_string()),
                Atom("foo".to_string()),
                Var("R".to_string()),
            ],
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
                Integer(42),
                Var("U".to_string()),
                Var("V".to_string()),
            ],
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![
                Var("Y".to_string()),
                Var("Z".to_string()),
                Atom("foo".to_string()),
                Integer(42),
                Var("V".to_string()),
                Var("U".to_string()),
            ],
        );
        let mut bindings = unify(&t0, &t1).unwrap();
        let term = substitute_bindings(&t0, &mut bindings);
        assert!(matches!(
            term,
            Functor(name, args)
                if name == "foo"
                && args.len() == 6
                && matches!(&args[0], Atom(val) if val == "foo")
                && matches!(&args[1], Atom(val) if val == "foo")
                && matches!(&args[2], Atom(val) if val == "foo")
                && matches!(&args[3], Integer(val) if *val == 42)
                && matches!(&args[4], Var(val) if val == "V")
                && matches!(&args[5], Var(val) if val == "V")
        ));
    }
}
