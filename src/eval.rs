pub mod qsq;

use std::collections::HashMap;

use crate::storage::format::ValDecodable;
use crate::lang::scan;
use crate::lang::parse;
use crate::lang::parse::Program;
use crate::lang::parse::Statement;
use crate::lang::parse::AssertionKind;
use crate::lang::Term;
use crate::lang::Rule;
use crate::lang::unify;
use crate::lang::DBAux;
use crate::lang::QueryAnswer;
use crate::storage::db;

// struct Trie {
//     root: TrieNode,
// }

// impl Trie {
//     fn empty() -> Self {
//         Trie { root: TrieNode { children: HashMap::new() } }
//     }

//     fn add_path(&mut self, path: &Vec<u8>) {
//         let mut cur = &mut self.root;
//         for &pos in path {
//             cur =
//                 cur
//                 .children
//                 .entry(pos)
//                 .or_insert_with(TrieNode::new);
//         }
//     }

//     fn collect_leaf_paths(&self) -> Vec<Vec<u8>> {
//         let mut paths = vec![];
//         let mut path = vec![];
//         Self::dfs(&self.root, &mut path, &mut paths);
//         paths
//     }

//     fn dfs(node: &TrieNode, path: &mut Vec<u8>, paths: &mut Vec<Vec<u8>>) {
//         if node.children.is_empty() {
//             paths.push(path.clone());
//             return
//         }
//         for (&pos, child) in node.children.iter() {
//             path.push(pos);
//             Self::dfs(child, path, paths);
//             path.pop();
//         }
//     }
// }

// struct TrieNode {
//     children: HashMap<u8, TrieNode>,
// }

// impl TrieNode {
//     fn new() -> Self { Self { children: HashMap::new() } }
// }

struct TxnDBAux<'txn> {
    txn: &'txn db::TransactionOp<'txn>,
}

impl<'txn> DBAux for TxnDBAux<'txn> {
    fn get_rule(&self, name: &str, arity: u8) -> Option<Rule> {
        let rule_text = self.txn.rule(name, arity).unwrap();

        rule_text
        .map(|text| {
            let scanner = scan::Scanner::new(&text);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser).unwrap();
            let mut head = None;
            let mut clauses = vec![];

            for stmt in program {
                if let Statement::Assertion(kind) = stmt {
                    if let AssertionKind::RuleClause { head: head0, body } = kind {
                        head = Some(head0);
                        clauses.push(body);
                    }
                }
            }

            Rule { head: head.unwrap(), clauses }
        })
    }

    fn lookup(&mut self, search_term: Term) -> Box<dyn Iterator<Item = (u32, Term)>> {
        let name = search_term.atom_name();
        let arity = search_term.atom_arity();
        let prefix = self.txn.get_prefix(name, arity).unwrap(); // todo handle errors

        let result = if let Some(prefix) = prefix {
            self
            .txn
            .query(prefix)
            .unwrap()
            .map(|(key, val)| {
                let (tuple_id, _) = key.decode_u32();
                let term = Term::decode(&val).unwrap();
                (tuple_id, term)
            })
            .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        Box::new(result.into_iter())
    }
}

pub fn batch_eval(
    txn: &db::TransactionOp,
    program: Program,
) -> Result<Vec<QueryAnswer>, anyhow::Error> {
    define_rules(txn, &program)?;

    let mut result = vec![];

    for stmt in program {
        match stmt {
            Statement::Assertion(kind) => {
                match kind {
                    AssertionKind::Fact { head: term } => {
                        evaluate_assertion(txn, &term)?;
                    }
                    AssertionKind::RuleClause { .. } => {
                        // rule definitions are already handled in `define_rules`
                        continue;
                    }
                }
            },
            Statement::Query(term) => {
                let mut txn_db_aux = TxnDBAux { txn };
                result.push(qsq::eval_query(term, &mut txn_db_aux));
            },
            Statement::Retraction(kind) => {
                evaluate_retraction(txn, &kind)?;
            },
        }
    }

    Ok(result)
}

fn define_rules(
    txn: &db::TransactionOp,
    program: &Program,
) -> Result<(), anyhow::Error> {
    let mut clauses = HashMap::new();

    for stmt in program {
        if let Statement::Assertion(kind) = stmt {
            if let AssertionKind::RuleClause { head, body: _ } = kind {
                let name = head.atom_name();
                let arity = head.atom_arity();
                let set =
                    clauses
                    .entry((name, arity))
                    .or_insert(vec![]);
                set.push(kind);
            }
        }
    }

    for ((name, arity), rules) in clauses {
        let rule_text =
            rules
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("");

        txn.define(name, arity, &rule_text)?;
    }

    Ok(())
}

fn evaluate_assertion(
    txn: &db::TransactionOp,
    term: &Term,
) -> Result<(), anyhow::Error> {
    if let Term::Atom(name, args, _) = term {
        let arity = args.len().try_into().unwrap();
        let data = Term::encode(term)?;
        txn.assert(name, arity, data)?;
        Ok(())
    } else {
        unreachable!()
    }
}

fn evaluate_retraction(
    txn: &db::TransactionOp,
    kind: &AssertionKind,
) -> Result<(), anyhow::Error> {
    match kind {
        AssertionKind::Fact { head } => {
            let name = head.atom_name();
            let arity = head.atom_arity();
            let prefix = txn.get_prefix(name, arity)?;
            if let Some(prefix) = prefix {
                let tuples_iter = txn.query(prefix)?;

                for (key, val) in tuples_iter {
                    let (tuple_id, _) = key.decode_u32();
                    let term = Term::decode(&val)?;
                    if let Some(_) = unify::unify(&head, &term) {
                        txn.retract(name, arity, tuple_id)?;
                    }
                }
            }
        },
        AssertionKind::RuleClause { head, body: _ } => {
            let name = head.atom_name();
            let arity = head.atom_arity();
            txn.forget(name, arity)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use crate::lang::parse::parse_program;
    use crate::lang::parse::Parser;
    use crate::lang::scan::Scanner;

    #[test]
    fn batch_eval_handles_zero_arity_rule_assertion() -> Result<(), anyhow::Error> {
        let program_text = "
            true.
            succeed :- true.
            succeed?
        ";

        let scanner = Scanner::new(program_text);
        let mut parser = Parser::new(scanner);
        let program = parse_program(&mut parser)?;

        let db = db::DB::new_temp()?;
        let txn = db.begin_transaction();
        let answers = batch_eval(&txn, program)?;
        txn.commit()?;

        let atoms = answers
            .into_iter()
            .flat_map(|answer| answer.get_atoms().into_iter())
            .collect::<HashSet<_>>();

        assert_eq!(
            atoms,
            HashSet::from([Term::Atom("succeed".into(), vec![], false)])
        );

        Ok(())
    }

    #[test]
    fn batch_eval_handles_functor_len() -> Result<(), anyhow::Error> {
        let program_text = {
            let mut program = include_str!("../tests/functors.pl").to_string();
            program.push_str("num_fruits(Name, Num)?");
            program
        };

        let scanner = Scanner::new(&program_text);
        let mut parser = Parser::new(scanner);
        let program = parse_program(&mut parser)?;

        let db = db::DB::new_temp()?;
        let txn = db.begin_transaction();
        let answers = batch_eval(&txn, program)?;
        txn.commit()?;

        let atoms = answers
            .into_iter()
            .flat_map(|answer| answer.get_atoms().into_iter())
            .collect::<HashSet<_>>();

        assert_eq!(
            atoms,
            HashSet::from([
                Term::Atom(
                    "num_fruits".into(),
                    vec![
                        Term::Sym("alice".into()),
                        Term::Atom(
                            "s".into(),
                            vec![Term::Atom("s".into(), vec![Term::Sym("z".into())], false)],
                            false
                        )
                    ],
                    false
                ),
                Term::Atom(
                    "num_fruits".into(),
                    vec![Term::Sym("bob".into()), Term::Atom("s".into(), vec![Term::Sym("z".into())], false)],
                    false
                ),
                Term::Atom("num_fruits".into(), vec![Term::Sym("dave".into()), Term::Sym("z".into())], false),
            ])
        );

        Ok(())
    }
}
