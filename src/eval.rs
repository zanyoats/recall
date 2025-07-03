pub mod naive;

use crate::lang::parse::TypedStatement;
use crate::lang::parse::TypedProgram;
use crate::lang::parse::FunctorTerm;
use crate::lang::parse::Literal;
use crate::storage::db;

pub trait QueryEvaluator<'txn> {
    type Iter: Iterator<Item = FunctorTerm> + Default;

    fn eval_declarations(
        &self,
        txn: &'txn db::TransactionOp,
        program: &TypedProgram,
    ) -> Result<(), anyhow::Error>;

    fn evaluate_assertion(
        &self,
        txn: &'txn db::TransactionOp,
        term: &FunctorTerm,
    ) -> Result<(), anyhow::Error>;

    fn evaluate_query(
        &self,
        txn: &'txn db::TransactionOp,
        query: &FunctorTerm,
    ) -> Result<Self::Iter, anyhow::Error>;

    fn evaluate_retraction(
        &self,
        txn: &'txn db::TransactionOp,
        literal: &Literal,
    ) -> Result<(), anyhow::Error>;
}

pub fn iter_eval<'program, 'txn, Engine: QueryEvaluator<'txn>>(
    program: &TypedProgram,
    txn: &'txn db::TransactionOp,
    evaluator: Engine,
) -> Result<Engine::Iter, anyhow::Error> {
    evaluator.eval_declarations(txn, program)?;

    for stmt in program.statements.iter() {
        match stmt {
            TypedStatement::Fact(term) => {
                evaluator.evaluate_assertion(txn, term)?;
            },
            TypedStatement::Query(term) => {
                return evaluator.evaluate_query(txn, term);
            },
            TypedStatement::Retraction(literal) => {
                evaluator.evaluate_retraction(txn, literal)?;
            }
        }
    }

    return Ok(Engine::Iter::default());
}

pub fn batch_eval<'program, 'txn, Engine: QueryEvaluator<'txn>>(
    program: &TypedProgram,
    txn: &'txn db::TransactionOp,
    evaluator: Engine,
) -> Result<Vec<FunctorTerm>, anyhow::Error> {
    evaluator.eval_declarations(txn, program)?;

    let mut result: Vec<FunctorTerm> = vec![];

    for stmt in program.statements.iter() {
        match stmt {
            TypedStatement::Fact(term) => {
                evaluator.evaluate_assertion(txn, term)?;
            },
            TypedStatement::Query(term) => {
                result.extend(evaluator.evaluate_query(txn, term)?);
            },
            TypedStatement::Retraction(literal) => {
                evaluator.evaluate_retraction(txn, literal)?;
            }
        }
    }

    Ok(result)
}
