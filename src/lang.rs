pub mod analysis;
pub mod parse;
pub mod scan;
pub mod unify;

use std::collections::HashMap;
use std::collections::HashSet;

// TODO: rename Functor, FunctorTerm, and Term to Atom elsewheres
pub type Atom = parse::FunctorTerm;
pub type Defs = HashMap<String, Rule>;
pub type Strata = Vec<Defs>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Rule {
    pub head: Atom,
    pub clauses: Vec<Vec<Atom>>,
}

pub trait DBAux {
    fn get_rule(&self, name: &str, arity: u8) -> Option<Vec<parse::Literal>>;
    fn lookup(&mut self, search_term: Atom) -> Box<dyn Iterator<Item = (u32, Atom)>>;
}

#[derive(Debug)]
pub struct QueryAnswer {
    pub query: Atom,
    pub answer: Vec<unify::Bindings>,
}

impl QueryAnswer {
    pub fn get_atoms(&self) -> HashSet<Atom> {
        self.answer
            .iter()
            .map(|bindings| unify::substitute_bindings(&self.query, bindings))
            .collect()
    }
}
