use std::collections::HashMap;

use crate::lang::program::Term;

pub type Bindings<'a> = HashMap<&'a str, &'a Term>;

pub fn unify_terms<'a>(t0: &'a Term, t1: &'a Term, bindings: &mut Bindings<'a>) -> bool {
    match (t0, t1) {
        (Term::Symbol(x), Term::Symbol(y)) =>
            x == y,
        (Term::Number(x), Term::Number(y)) =>
            x == y,
        (Term::String(x), Term::String(y)) =>
            x == y,
        (Term::Var(x), _) =>
            unify_var(x, t1, bindings),
        (_, Term::Var(y)) =>
            unify_var(y, t0, bindings),
        (Term::Functor(name0, terms0), Term::Functor(name1, terms1)) => {
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

    macro_rules! assert_bindings {
        ($bindings:expr, $list:expr) => {
            assert_eq!(
                $bindings,
                HashMap::from_iter($list),
            );
        };
    }

    #[test]
    fn it_unifies_simple_terms() {
        let y_var = Var("Y".to_string());
        let foo_ident = Symbol("foo".to_string());
        let test_cases = vec![
            (
                Symbol("foo".to_string()),
                Symbol("foo".to_string()),
                vec![],
            ),
            (
                Var("X".to_string()),
                Var("Y".to_string()),
                vec![("X", &y_var)],
            ),
            (
                Var("X".to_string()),
                Symbol("foo".to_string()),
                vec![("X", &foo_ident)],
            ),
            (Number("42".to_string()), Number("42".to_string()), vec![]),
            (
                Functor(
                    "foo".to_string(),
                    vec![Var("X".to_string())],
                ),
                Functor(
                    "foo".to_string(),
                    vec![Var("X".to_string())],
                ),
                vec![],
            ),
        ];
        for (t0, t1, expected_bindings) in test_cases {
            let mut bindings = HashMap::new();
            assert!(unify_terms(&t0, &t1, &mut bindings));
            assert_bindings!(bindings, expected_bindings);
        }
    }

    #[test]
    fn it_fails_to_unify_terms() {
        let test_cases = [
            (Symbol("foo".to_string()), Symbol("bar".to_string())),
            (Number("42".to_string()), Number("69".to_string())),
            (
                Functor(
                    "foo".to_string(),
                    vec![Symbol("foo".to_string())],
                ),
                Functor(
                    "foo".to_string(),
                    vec![Symbol("bar".to_string())],
                ),
            ),
        ];
        for (t0, t1) in test_cases {
            let mut bindings = HashMap::new();
            assert!(!unify_terms(&t0, &t1, &mut bindings));
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
                Symbol("foo".to_string()),
            ],
        );
        let mut bindings = HashMap::new();
        assert!(unify_terms(&t0, &t1, &mut bindings));
        assert_bindings!(bindings, [
            ("X", &Var("Y".to_string())),
            ("Y", &Functor(
                "bar".to_string(),
                vec![Var("Z".to_string())],
            )),
            ("Z", &Symbol("foo".to_string())),
        ]);
    }

    #[test]
    fn it_fails_to_unify_complex_terms() {
        let t0 = Functor(
            "foo".to_string(),
            vec![Symbol("apple".to_string()), Var("X".to_string())],
        );
        let t1 = Functor(
            "foo".to_string(),
            vec![Var("X".to_string()), Symbol("lemon".to_string())],
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
        assert_bindings!(bindings, [
            ("X", &Var("Y".to_string())),
        ]);
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
                Symbol("foo".to_string()),
                Var("R".to_string()),
            ],
        );
        let mut bindings = HashMap::new();
        assert!(unify_terms(&t0, &t1, &mut bindings));
        assert_bindings!(bindings, [
            ("X", &Var("Y".to_string())),
            ("Y", &Var("Z".to_string())),
            ("Z", &Symbol("foo".to_string())),
            ("R", &Symbol("foo".to_string())),
        ]);
    }
}
