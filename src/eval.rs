pub mod qsq;

use std::collections::HashMap;
use std::iter;

use crate::storage::format::TaggedVal;
use crate::storage::format::ValDecodable;
use crate::storage::format::ValEncodable;
use crate::DeclRoot;
use crate::ArgType;
use crate::lang::scan;
use crate::lang::parse;
use crate::lang::parse::TypedStatement;
use crate::lang::parse::TypedProgram;
use crate::lang::Atom;
use crate::lang::parse::Literal;
use crate::lang::parse::Term;
use crate::lang::unify;
use crate::lang::DBAux;
use crate::lang::QueryAnswer;
use crate::storage::db;
use crate::storage::format;
use crate::errors;

struct Trie {
    root: TrieNode,
}

impl Trie {
    fn empty() -> Self {
        Trie { root: TrieNode { children: HashMap::new() } }
    }

    fn add_path(&mut self, path: &Vec<u8>) {
        let mut cur = &mut self.root;
        for &pos in path {
            cur =
                cur
                .children
                .entry(pos)
                .or_insert_with(TrieNode::new);
        }
    }

    fn collect_leaf_paths(&self) -> Vec<Vec<u8>> {
        let mut paths = vec![];
        let mut path = vec![];
        Self::dfs(&self.root, &mut path, &mut paths);
        paths
    }

    fn dfs(node: &TrieNode, path: &mut Vec<u8>, paths: &mut Vec<Vec<u8>>) {
        if node.children.is_empty() {
            paths.push(path.clone());
            return
        }
        for (&pos, child) in node.children.iter() {
            path.push(pos);
            Self::dfs(child, path, paths);
            path.pop();
        }
    }
}

struct TrieNode {
    children: HashMap<u8, TrieNode>,
}

impl TrieNode {
    fn new() -> Self { Self { children: HashMap::new() } }
}

struct TxnDBAux<'txn> {
    txn: &'txn db::TransactionOp<'txn>,
    query_cache: HashMap<Atom, Vec<(u32, Atom)>>,
}

impl<'txn> TxnDBAux<'txn> {
    fn lookup_atom(atom: &Atom) -> Atom {
        match atom {
            Term::Var(_) =>
                Term::Var("_".into()),
            Term::Functor(name, args, negated) =>
                Term::Functor(
                    name.into(),
                    args
                    .iter()
                    .map(|arg| TxnDBAux::lookup_atom(arg))
                    .collect(),
                    *negated
                ),
            _ => atom.clone(),
        }
    }

    fn value_positions(atom: &Atom) -> Vec<u8> {
        if let Term::Functor(_, args, _) = atom {
            args
            .iter()
            .enumerate()
            .filter_map(|(i, arg)| match arg {
                Term::Atom(_) | Term::Int(_) | Term::Str(_) => Some(u8::try_from(i).unwrap()),
                Term::Var(_) => None,
                _ => unreachable!(),
            })
            .collect()
        } else {
            unreachable!()
        }
    }
}

impl<'txn> DBAux for TxnDBAux<'txn> {
    fn get_rule(&self, name: &str, arity: u8) -> Option<Vec<Literal>> {
        let rule_text = self.txn.rule(name, arity).unwrap();

        rule_text
        .map(|text| {
            let scanner = scan::Scanner::new(&text);
            let mut parser = parse::Parser::new(scanner);
            let program = parse::parse_program(&mut parser).unwrap();
            let mut rules = vec![];

            for stmt in program.statements {
                if let parse::Statement::Assertion(literal) = stmt {
                    rules.push(literal)
                }
            }

            rules
        })
    }

    fn lookup(&mut self, search_term: Atom) -> Box<dyn Iterator<Item = (u32, Atom)>> {
        let lookup_atom = Self::lookup_atom(&search_term);
        let name = lookup_atom.functor_name();
        let arity = lookup_atom.functor_arity();
        let prefix =
            self
            .txn
            .get_prefix(name, arity)
            .unwrap() // todo handle errors
            .unwrap(); // assumes a fact atom is always found
        let atom_spec =
            self
            .txn
            .get_scm(prefix.clone())
            .unwrap();
        let value_positions = Self::value_positions(&lookup_atom);
        let mut positions_prefixes = iter::successors(
            (!value_positions.is_empty()).then(|| &value_positions[..]),
            |&prefix| {
                (prefix.len() > 1).then(|| &prefix[..prefix.len()-1])
            },
        );
        let found_index =
            positions_prefixes
            .find_map(|positions_prefix| {
                atom_spec
                .indexes
                .iter()
                .find(|&index| index.starts_with(positions_prefix))
                .map(|index| (index.as_slice(), positions_prefix))
            });
        let result =
            self
            .query_cache
            .entry(lookup_atom.clone())
            .or_insert_with(|| {

                if crate::get_verbose() {
                    eprintln!("INFO: index lookup {lookup_atom} index {found_index:?}");
                }

                if let Some((index, positions_prefix)) = found_index {
                    let mut index_prefix = vec![];
                    index_prefix.extend_from_slice(&prefix);
                    for &pos in index {
                        pos.encode_append(&mut index_prefix);
                    }
                    let scm1 =
                        positions_prefix
                        .iter()
                        .map(|&pos| {
                            let ty = &atom_spec.schema[pos as usize];
                            *ty
                        })
                        .collect::<Vec<_>>();
                    let tagged_vals = {
                        let atom_args = lookup_atom.functor_args().as_slice();

                        positions_prefix
                        .iter()
                        .map(|&pos| {
                            let arg = &atom_args[pos as usize];
                            TaggedVal::from(arg)
                        })
                        .collect::<Vec<_>>()
                    };
                    let encoded =
                        TaggedVal::encode_from_scm(
                            &tagged_vals,
                            &scm1,
                            0..tagged_vals.len().try_into().unwrap(),
                        ).unwrap();
                    index_prefix.extend(encoded.into_iter());

                    self
                    .txn
                    .index(index_prefix)
                    .unwrap()
                    .map(|raw_tuple_id| {
                        let mut tuple_prefix = prefix.clone();
                        tuple_prefix.extend_from_slice(&raw_tuple_id);
                        let raw_val =
                            self
                            .txn
                            .lookup(tuple_prefix)
                            .unwrap()
                            .unwrap();
                        let (tuple_id, _) = raw_tuple_id.decode_u32();
                        let tagged_vals =
                            TaggedVal::decode_from_scm(
                                &raw_val,
                                &atom_spec.schema,
                                0..atom_spec.schema.len().try_into().unwrap(),
                            );
                        let tuple_atom = Term::Functor(
                            name.into(),
                            tagged_vals.into_iter().map(Term::from).collect(),
                            false,
                        );

                        (tuple_id, tuple_atom)

                    })
                    .collect::<Vec<_>>()
                } else {
                    self
                    .txn
                    .query(prefix)
                    .unwrap()
                    .map(|(raw_key, raw_val)| {
                        let (tuple_id, _) = raw_key.decode_u32();
                        let tagged_vals =
                            TaggedVal::decode_from_scm(
                                &raw_val,
                                &atom_spec.schema,
                                0..atom_spec.schema.len().try_into().unwrap(),
                            );
                        let tuple_atom = Term::Functor(
                            name.into(),
                            tagged_vals.into_iter().map(Term::from).collect(),
                            false,
                        );

                        (tuple_id, tuple_atom)
                    })
                    .collect()
                }
            });

        Box::new(result.clone().into_iter())
    }
}

pub fn batch_eval(
    program: TypedProgram,
    txn: &db::TransactionOp,
) -> Result<Vec<QueryAnswer>, anyhow::Error> {
    check_declarations(txn, &program)?;

    let mut result = vec![];

    for stmt in program.statements {
        match stmt {
            TypedStatement::Fact(term) => {
                evaluate_assertion(txn, &term)?;
            },
            TypedStatement::Query(term) => {
                let mut txn_db_aux = TxnDBAux { txn, query_cache: HashMap::new() };
                result.push(qsq::eval_query(term, &mut txn_db_aux));
            },
            TypedStatement::Retraction(literal) => {
                evaluate_retraction(txn, &literal)?;
            }
        }
    }

    Ok(result)
}

pub fn eval_declarations(
    txn: &db::TransactionOp,
    root: DeclRoot,
) -> Result<(), anyhow::Error> {
    for (name, decl) in root.declare {
        if decl.args.len() < 1 {
            return Err(errors::RecallError::RuntimeError(format!(
                "declaration for '{}' must have at least one argument",
                name,
            )).into());
        }

        let mut schema = vec![];
        let mut labels = vec![];

        for arg in decl.args {
            schema.push(
                match arg.into_iter().nth(0) {
                    Some((label, ty)) => {
                        labels.push(label);
                        match ty {
                            ArgType::Atom => format::ATOM_TYPE,
                            ArgType::Int => format::INT_TYPE,
                            ArgType::Str => format::STRING_TYPE,
                        }
                    },
                    None => {
                        return Err(errors::RecallError::RuntimeError(format!(
                            "label and type for arg missing for declaration '{}'",
                            name,
                        )).into());
                    },
                }
            );
        }

        let indexes: Result<Vec<Vec<u8>>, anyhow::Error> =
            decl.indexes.map_or(Ok(vec![]), |decl_indexes| {
                let mut indexes = vec![];

                for arg_labels in decl_indexes {
                    let mut index = vec![];

                    for arg_label in arg_labels {
                        let pos =
                            labels
                            .iter()
                            .position(|label| {
                                &arg_label == label
                            });
                        if let Some(pos) = pos {
                            index.push(u8::try_from(pos).unwrap());
                        } else {
                            return Err(errors::RecallError::RuntimeError(format!(
                                "argument label '{}' not found for declaration '{}'",
                                arg_label, name,
                            )).into());
                        }
                    }

                    indexes.push(index);
                }

                // filter the indexes to select longest prefix

                let mut trie = Trie::empty();
                for index in indexes.iter() {
                    trie.add_path(index);
                }

                let indexes = trie.collect_leaf_paths();

                if crate::get_verbose() {
                    for index in indexes.iter() {
                        eprintln!("INFO: adding index {:?}", index)
                    }
                }

                Ok(indexes)
            });

        txn.declare(&name, schema, labels, indexes?)?;
    }

    Ok(())
}

fn check_declarations(
    txn: &db::TransactionOp,
    program: &TypedProgram,
) -> Result<(), anyhow::Error> {
    // check on declarations used in non-head positions
    for ((name, arity), scm) in program.schemas.iter() {
        let key = (name.to_string(), *arity);
        if !program.facts.contains(&key) && !program.defs.contains_key(&key) {
            if !txn.exists_with_same_schema(name, scm)? {
                return Err(errors::RecallError::RuntimeError(format!(
                    "declaration not found in db: {}/{}",
                    name, arity,
                )).into());
            }
        }
    }

    // check facts used
    for (name, arity) in program.facts.iter() {
        let scm =
            program
            .schemas
            .get(&(name.to_string(), *arity))
            .unwrap();

        if !txn.exists_with_same_schema(name, scm)? {
            return Err(errors::RecallError::RuntimeError(format!(
                "declaration not found in db: {}/{}",
                name, arity,
            )).into());
        }
    }

    // define rules
    for ((name, arity), rules) in program.defs.iter() {
        let rule_text =
            rules
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("");

        let schema =
            program
            .schemas
            .get(&(name.to_string(), *arity))
            .unwrap();

        txn.define(name, &rule_text, schema.to_vec())?;
    }

    Ok(())
}

fn evaluate_assertion(
    txn: &db::TransactionOp,
    term: &Atom,
) -> Result<(), anyhow::Error> {
    if let Term::Functor(name, args, _) = term {
        let input =
            args
            .into_iter()
            .map(|arg| arg.into())
            .collect::<Vec<TaggedVal>>();
        let arity = input.len().try_into().unwrap();

        txn.assert(name, arity, &input)?;

        Ok(())
    } else {
        unreachable!()
    }
}

fn evaluate_retraction(
    txn: &db::TransactionOp,
    literal: &Literal,
) -> Result<(), anyhow::Error> {
    let name = literal.head.functor_name();
    let arity = literal.head.functor_arity();

    if let Some(is_rule) = txn.is_rule(name, arity)? {
        if is_rule {
            txn.forget(name, arity)?;
        } else {
            let prefix = txn.get_prefix(name, arity)?.unwrap();

            // TODO: use index here if possible
            let raw_tuples =
                txn
                .query(prefix.clone())?;

            for (raw_key, raw_val) in raw_tuples {
                let (tuple_id, _) = raw_key.decode_u32();
                let atom_spec = txn.get_scm(prefix.clone())?;
                let tagged_vals =
                    TaggedVal::decode_from_scm(
                        &raw_val,
                        &atom_spec.schema,
                        0..atom_spec.schema.len().try_into().unwrap(),
                    );
                let atom = Term::Functor(
                    name.into(),
                    tagged_vals.into_iter().map(Term::from).collect(),
                    false,
                );

                if let Some(_) = unify::unify(&literal.head, &atom) {
                    txn.retract(name, arity, tuple_id)?;
                }
            }
        }
    } else {
        if crate::get_verbose() {
            eprintln!("WARN: {}/{} not found for literal '{}'", name, arity, literal);
        }
    }

    Ok(())
}
