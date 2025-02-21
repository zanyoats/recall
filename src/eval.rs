pub mod naive;

use crate::lang::Program;
use crate::lang::Term;
use crate::lang::Rule;

pub trait Eval<'a> {
    fn from_term<T: DbEngine>(
        query: &'a Term,
        program: Option<&'a Program>,
        db_engine: &mut T,
    ) -> Self;
    fn from_rule<T: DbEngine>(
        query: &'a Rule,
        program: Option<&'a Program>,
        db_engine: &mut T,
    ) -> Self;
    fn execute(self) -> Vec<Term>;
}

pub trait DbEngine {
    fn get_stored_terms(&mut self) -> Vec<Term>;
}
