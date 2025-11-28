use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use std::process;

use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::IteratorMode;
use rocksdb::Options;
use rocksdb::TransactionDB;
use rocksdb::TransactionDBOptions;
use rocksdb::DBIteratorWithThreadMode;
use rocksdb::PrefixRange;
use rocksdb::TransactionOptions;
use rocksdb::ReadOptions;
use rocksdb::WriteOptions;
use rocksdb::SingleThreaded;

use crate::storage::format;
use crate::storage::format::ValEncodable;
use crate::storage::format::ValDecodable;
use crate::errors;

pub struct DB {
    db: Option<TransactionDB<SingleThreaded>>,
    persist: bool,
    path: PathBuf,
}

pub struct TransactionOp<'a> {
    decl: &'a ColumnFamily,
    rvlk: &'a ColumnFamily,
    def: &'a ColumnFamily,
    tup: &'a ColumnFamily,
    meta: &'a ColumnFamily,
    idx: &'a ColumnFamily,
    tx: rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
}

pub struct TupleIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

pub struct IndexIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

pub struct RulesIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

type Query = Vec<u8>;

impl DB {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self, anyhow::Error> {
        DB::setup_db(file_path.as_ref(), true)
    }

    pub fn new_temp() -> Result<Self, anyhow::Error> {
        let temp_path = DB::temp_path()?;
        DB::setup_db(&temp_path, false)
    }

    fn temp_path() -> Result<PathBuf, anyhow::Error> {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let pid = process::id();
        let unique_name = format!("recall_scratch_{}_{}", pid, timestamp);
        path.push(unique_name);
        fs::create_dir(&path)?;
        Ok(path)
    }

    fn setup_db(path: &Path, persist: bool) -> Result<Self, anyhow::Error> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(500);
        db_opts.increase_parallelism(4);

        let cf_names = vec![
            "default",
            format::DECL,
            format::RVLK,
            format::DEF,
            format::TUP,
            format::IDX,
            format::META,
            format::IDX,
        ];
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let tx_db_opts = TransactionDBOptions::default();
        let db: TransactionDB<SingleThreaded> = TransactionDB::open_cf_descriptors(
            &db_opts,
            &tx_db_opts,
            path,
            cf_descriptors,
        )?;

        // initialize the two sequences: prefix codes & tuple ids
        // if not set already
        let meta = db.cf_handle(format::META).unwrap();

        if db.get_cf(meta, &[format::NEXT_PREFIX_SEQ])?.is_none() {
            let mut buf = vec![];
            0u16.encode_append(&mut buf);
            db.put_cf(meta, &[format::NEXT_PREFIX_SEQ], buf)?;
        }

        if db.get_cf(meta, &[format::NEXT_TUP_SEQ])?.is_none() {
            let mut buf = vec![];
            0u32.encode_append(&mut buf);
            db.put_cf(meta, &[format::NEXT_TUP_SEQ], buf)?;
        }

        Ok(Self { db: Some(db), persist, path: path.to_path_buf() })
    }

    pub fn begin_transaction(&self) -> TransactionOp {
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);
        let write_opts = WriteOptions::default();
        let db = self.db.as_ref().unwrap();
        let tx = db.transaction_opt(&write_opts, &txn_opts);
        let decl = db.cf_handle(format::DECL).unwrap();
        let rvlk = db.cf_handle(format::RVLK).unwrap();
        let def = db.cf_handle(format::DEF).unwrap();
        let tup = db.cf_handle(format::TUP).unwrap();
        let meta = db.cf_handle(format::META).unwrap();
        let idx = db.cf_handle(format::IDX).unwrap();
        TransactionOp { decl, rvlk, def, tup, meta, idx, tx }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if !self.persist {
            // Take and drop the DB to ensure RocksDB file handles are closed
            if let Some(db) = self.db.take() {
                drop(db);
            }

            if let Err(e) = fs::remove_dir_all(&self.path) {
                eprintln!("Warning: failed to delete temp RocksDB directory {:?}: {}", self.path, e);
            }
        }
    }
}

fn build_predicate_key(name: &str, arity: u8) -> Vec<u8> {
    let mut buf = vec![];
    name.to_string().encode_append(&mut buf);
    arity.encode_append(&mut buf);
    buf
}

impl<'a> TransactionOp<'a> {
    pub fn get_prefix(&self, name: &str, arity: u8) -> Result<Option<Vec<u8>>, anyhow::Error> {
        Ok({
            let key = build_predicate_key(name, arity);
            self
            .tx
            .get_cf(self.decl, key)?
        })
    }

    fn with_prefix(
        &self,
        name: &str,
        arity: u8,
        is_rule: bool,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let key = build_predicate_key(&name, arity);

        if let Some(prefix) = self.tx.get_cf(self.decl, &key)? {
            let (prefix_code, _) = prefix.as_slice().decode_u16();

            if is_rule {
                if prefix_code < format::FIRST_RULE_SEQ {
                    return Err(errors::RecallError::RuntimeError(format!(
                        "cannot add rule since predicate {}/{} is a fact",
                        name, arity,
                    )).into())
                }
            } else {
                if prefix_code >= format::FIRST_RULE_SEQ {
                    return Err(errors::RecallError::RuntimeError(format!(
                        "cannot add fact since predicate {}/{} is a rule",
                        name, arity,
                    )).into())
                }
            }

            Ok(prefix)
        } else {
            let current =
                self
                .tx
                .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let (current_code, _) = current.as_slice().decode_u16();
            let prefix =
                if is_rule {
                    let prefix_code =
                        current_code
                        .checked_add(format::FIRST_RULE_SEQ)
                        .unwrap();

                    let mut prefix = vec![];
                    prefix_code.encode_append(&mut prefix);
                    prefix
                } else {
                    self
                    .tx
                    .put_cf(self.decl, &key, &current)?;

                    current
                };

            self
            .tx
            .put_cf(self.decl, &key, &prefix)?;

            self
            .tx
            .put_cf(self.rvlk, &prefix, name.as_bytes())?;

            // update prefix
            {
                let mut buf = vec![];
                let next_code = current_code.checked_add(1).unwrap();
                next_code.encode_append(&mut buf);

                self
                .tx
                .put_cf(self.meta, &[format::NEXT_PREFIX_SEQ], buf)?;
            }

            Ok(prefix)
        }
    }

    pub fn define(
        &self,
        name: &str,
        arity: u8,
        text: &str,
    ) -> Result<(), anyhow::Error> {
        let prefix =
            self
            .with_prefix(name, arity, true)?;

        self
        .tx
        .put_cf(self.def, &prefix, text.as_bytes())?;

        Ok(())
    }

    pub fn assert(
        &self,
        name: &str,
        arity: u8,
        data: Vec<u8>,
    ) -> Result<u32, anyhow::Error> {
        let prefix =
            self
            .with_prefix(name, arity, false)?;

        // get the current tuple seq
        let id =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_TUP_SEQ])?
            .unwrap();
        let (tup_id, _) = id.as_slice().decode_u32();

        // insert tuple
        {
            let mut tup_prefix = vec![];
            tup_prefix.extend_from_slice(&prefix);
            tup_id.encode_append(&mut tup_prefix);

            self
            .tx
            .put_cf(self.tup, &tup_prefix, data)?;

        }

        // update id
        {
            let mut buf = vec![];
            let next_id = tup_id.checked_add(1).unwrap();
            next_id.encode_append(&mut buf);

            self
            .tx
            .put_cf(self.meta, &[format::NEXT_TUP_SEQ], buf)?;
        }

        Ok(tup_id)
    }

    pub fn retract(&self, name: &str, arity: u8, tuple_id: u32) -> Result<(), anyhow::Error> {
        if let Some(mut prefix) = self.get_prefix(name, arity)? {
            tuple_id.encode_append(&mut prefix);

            self
            .tx
            .delete_cf(self.tup, prefix)?;
        }

        Ok(())
    }

    pub fn rule(&self, name: &str, arity: u8) -> Result<Option<String>, anyhow::Error> {
        if let Some(prefix) = self.get_prefix(name, arity)? {
            if let Some(rules) = self.tx.get_cf(self.def, prefix)? {
                let text = String::from_utf8(rules).unwrap();
                return Ok(Some(text))
            }
        }

        Ok(None)
    }

    pub fn query(
        &'a self,
        query: Query,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>, anyhow::Error> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_range(PrefixRange(query));

        let iter = self.tx.iterator_cf_opt(
            self.tup,
            read_opts,
            IteratorMode::Start,
        );

        Ok(Box::new(TupleIterator {
            iter,
        }))
    }

    pub fn lookup(&'a self, query: Query) -> Result<Option<Vec<u8>>, anyhow::Error> {
        self
        .tx
        .get_cf(self.tup, query)
        .map_err(|e| e.into())
    }

    pub fn index(
        &'a self,
        query: Query,
    ) -> Result<Box<dyn Iterator<Item = Vec<u8>> + 'a>, anyhow::Error> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_range(PrefixRange(query));

        let iter = self.tx.iterator_cf_opt(
            self.idx,
            read_opts,
            IteratorMode::Start,
        );

        Ok(Box::new(IndexIterator {
            iter,
        }))
    }

    pub fn rules(&'a self) -> RulesIterator<'a> {
        let iter = self.tx.iterator_cf(
            self.def,
            IteratorMode::Start,
        );

        RulesIterator {
            iter,
        }
    }

    // eventually would like to do:
    // forget(link(a, b)).  forget single fact
    // forget(link(a, X)).  forget facts match unification
    // forget(link/2).      forget link predicate (remove declaration + data)
    // forget(path2/3).     forget rule

    // atm only works on rules
    pub fn forget(&self, name: &str, arity: u8) -> Result<(), anyhow::Error> {
        let key = build_predicate_key(name, arity);

        // remove rule from: decl
        // gc will eventually purge from other column families
        self
        .tx
        .delete_cf(self.decl, key)?;

        Ok(())
    }

    pub fn gc(&self) -> Result<(), anyhow::Error> {
        todo!()
    }

    pub fn info(&self) -> Result<(), anyhow::Error> {
        println!("meta:");
        let prefix =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
            .unwrap();
        let (prefix_code, _) = prefix.decode_u16();

        let id =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_TUP_SEQ])?
            .unwrap();
        let (tuple_id, _) = id.decode_u32();
        println!("\tprefix code {}", prefix_code);
        println!("\ttuple id    {}", tuple_id);

        println!();

        println!("declarations:");
        let iter = self.tx.iterator_cf(
            self.decl,
            IteratorMode::Start,
        );
        for result in iter {
            let (key, prefix) = result?;
            let (name, rest) = key.decode_string();
            let (arity, _) = rest.decode_u8();
            let (prefix_code, _) = prefix.decode_u16();
            println!("\t{}/{} ({})", name, arity, prefix_code);
        }

        Ok(())
    }

    pub fn commit(self) -> Result<(), anyhow::Error> {
        self.tx.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<(), anyhow::Error> {
        self.tx.rollback()?;
        Ok(())
    }
}

impl<'a> Iterator for TupleIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    // TODO: this may need to be Result<...> as Item
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (tuple_key, tuple_val) = result.unwrap();
            let (_, rest) = tuple_key.decode_u16();
            Some((rest.to_vec(), tuple_val.to_vec()))
        } else {
            None
        }
    }
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (_, tuple_val) = result.unwrap();
            Some(tuple_val.to_vec())
        } else {
            None
        }
    }
}

impl<'a> Iterator for RulesIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (_, rules) = result.unwrap();
            let text =
                String::from_utf8(rules.to_vec())
                .unwrap();
            Some(text)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::Term;
    use std::collections::HashSet;

    impl DB {
        fn new_temp_for_testing(test_name: &str) -> Result<Self, anyhow::Error> {
            let mut path = std::env::temp_dir();
            path.push(test_name);
            fs::create_dir(&path)?;
            DB::setup_db(&path, false)
        }
    }

    fn into_atom<'a>() -> impl Fn((Vec<u8>, Vec<u8>)) -> (u32, Term) + use<'a> {
        let inner = |(raw_key, raw_val): (Vec<u8>, Vec<u8>)| {
            let (tuple_id, _) = raw_key.decode_u32();
            let term = Term::decode(&raw_val).unwrap();
            (tuple_id, term)
        };

        inner
    }

    #[test]
    fn it_can_define_rules() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_define_rules")?;
        let tx = db.begin_transaction();

        let name = "foo";
        let arity = 2;
        let rule_text = "foo(X, Y) :- bar(X), baz(Y).";

        // define initial rule
        {
            tx.define(name, arity, rule_text)?;

            // DECL was set?
            let prefix = tx.get_prefix(name, arity)?.unwrap();
            assert_eq!(format::FIRST_RULE_SEQ.to_be_bytes(), &prefix[..]);

            // DEF was set?
            let rule_text1 = tx.rule(name, arity)?.unwrap();
            assert_eq!(rule_text1, rule_text);

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            assert_eq!(&[0, 1], &next_prefix[..]);

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([rule_text.into()]), rules);
        }

        // define subsequent rule
        {
            let name = "succeed";
            let arity = 0;
            let rule_text1 = "succeed :- true.";

            tx.define(name, arity, rule_text1)?;

            // DECL was set?
            let prefix = tx.get_prefix(name, arity)?.unwrap();
            assert_eq!((format::FIRST_RULE_SEQ + 1).to_be_bytes(), &prefix[..]);

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            assert_eq!(&[0, 2], &next_prefix[..]);

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([
                rule_text.into(),
                rule_text1.into(),
            ]), rules);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_assert_and_query() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_assert_and_query")?;
        let tx = db.begin_transaction();

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let term = Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(i)],
                    false
                );
                let data = Term::encode(&term)?;

                assert_eq!(tx.assert("foo", 2, data)?, i as u32);
            }
        }

        // insert a fact for bar
        {
            let term = Term::Atom(
                "bar".into(),
                vec![Term::Sym("apple".into()), Term::Int(k)],
                false
            );
            let data = Term::encode(&term)?;

            assert_eq!(tx.assert("bar", 2, data)?, k as u32);
        }

        // query all foo(X, Y)?
        {
            let prefix = tx.get_prefix("foo", 2)?.unwrap();
            let result =
                tx
                .query(prefix)?
                .map(into_atom())
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(2)],
                    false,
                )),
            ]), result);
        }

        // query all bar(X, Y)?
        {
            let prefix = tx.get_prefix("bar", 2)?.unwrap();
            let result =
                tx
                .query(prefix)?
                .map(into_atom())
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (3, Term::Atom(
                    "bar".into(),
                    vec![Term::Sym("apple".into()), Term::Int(k)],
                    false,
                )),
            ]), result);
        }

        // query on prefix that doesn't exist
        {
            let result =
                tx
                .query(b"hello".to_vec())?
                .collect::<Vec<_>>();

            assert_eq!(result.len(), 0);
        }

        // query all tuples
        {
            let result =
                tx
                .query(b"".to_vec())?
                .collect::<HashSet<_>>();

            assert_eq!(result.len(), (k + 1) as usize);
        }


        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_retract() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_retract")?;
        let tx = db.begin_transaction();

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let term = Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(i)],
                    false
                );
                let data = Term::encode(&term)?;

                assert_eq!(tx.assert("foo", 2, data)?, i as u32);
            }
        }

        // retract some facts
        {
            tx.retract("foo", 2, 1)?;
            tx.retract("foo", 2, 100)?;
            tx.retract("foo", 2 + 1, 1)?;
        }

        // query all foo(X, Y)? again
        {
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom())
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(0)],
                    false,
                )),
                (2, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(2)],
                    false,
                )),
            ]), result);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_forgot() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_forgot")?;
        let tx = db.begin_transaction();

        let term = Term::Atom(
            "bar".into(),
            vec![Term::Sym("apple".into()), Term::Int(0)],
            false
        );
        let data = Term::encode(&term)?;

        assert_eq!(tx.assert("bar", 2, data)?, 0);

        tx.forget("bar", 2)?;

        // DECL was unset?
        let prefix = tx.get_prefix("bar", 2)?;
        assert_eq!(prefix.is_none(), true);

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_abort_tx() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_abort_tx").unwrap();

        // set intial db state
        {
            let tx = db.begin_transaction();

            for i in 0..3 {
                let term = Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(i)],
                    false
                );
                let data = Term::encode(&term)?;

                assert_eq!(tx.assert("foo", 2, data)?, i as u32);
            }

            // query all foo(X, Y)?
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom())
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(2)],
                    false,
                )),
            ]));

            tx.commit()?;
        }

        // change state, but rollback
        {
            let tx = db.begin_transaction();

            tx.retract("foo", 2, 0)?;
            tx.retract("foo", 2, 1)?;
            tx.retract("foo", 2, 2)?;

            // query all foo(X, Y)?
            let result =
                tx
                .query(b"".to_vec())?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::new(), result);

            tx.rollback()?;
        }

        // confirm initial state intact
        {
            let tx = db.begin_transaction();

            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom())
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Atom(
                    "foo".into(),
                    vec![Term::Sym("apple".into()), Term::Int(2)],
                    false,
                )),
            ]));

            tx.commit()?;
        }

        Ok(())
    }
}
