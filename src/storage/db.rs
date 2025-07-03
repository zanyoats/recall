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

use crate::lang::parse::Term;
use crate::lang::parse::FunctorTerm;
use crate::storage::format;
use crate::storage::tuple::Tuple;
use crate::storage::tuple::ParameterType;
use crate::errors;

pub struct DB {
    db: Option<TransactionDB<SingleThreaded>>,
    persist: bool,
    path: PathBuf,
}

pub struct TransactionOp<'a> {
    decl: &'a ColumnFamily,
    rvlk: &'a ColumnFamily,
    scm: &'a ColumnFamily,
    def: &'a ColumnFamily,
    tup: &'a ColumnFamily,
    meta: &'a ColumnFamily,
    tx: rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
}

pub struct TupleIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
    rvlk: &'a ColumnFamily,
    scm: &'a ColumnFamily,
    tx: &'a rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
}

pub struct RulesIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

type Query = Tuple;

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
            format::SCM,
            format::TUP,
            format::IDX,
            format::META,
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
            db.put_cf(meta, &[format::NEXT_PREFIX_SEQ], Tuple::encode(
                &[ParameterType::U16(0)], &[format::U16_TYPE],
            )?.get())?;
        }

        if db.get_cf(meta, &[format::NEXT_TUP_SEQ])?.is_none() {
            db.put_cf(meta, &[format::NEXT_TUP_SEQ], Tuple::encode(
                &[ParameterType::U32(0)], &[format::U32_TYPE],
            )?.get())?;
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
        let scm = db.cf_handle(format::SCM).unwrap();
        let def = db.cf_handle(format::DEF).unwrap();
        let tup = db.cf_handle(format::TUP).unwrap();
        let meta = db.cf_handle(format::META).unwrap();
        TransactionOp { decl, rvlk, scm, def, tup, meta, tx }
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

fn predicate_key_tuple(name: &str, arity: u8) -> Result<Tuple, anyhow::Error> {
    let key_scm = &[format::ATOM_TYPE, format::U8_TYPE];
    Tuple::encode(
        &[
            ParameterType::Atom(name.to_string()),
            ParameterType::U8(arity),
        ],
        key_scm,
    )
}

impl<'a> TransactionOp<'a> {
    pub fn is_rule(&self, name: &str, arity: u8) -> Result<Option<bool>, anyhow::Error> {
        Ok({
            let key = predicate_key_tuple(name, arity)?;
            self
            .tx
            .get_cf(self.decl, key.get())?
            .map(|raw_val| {
                let tuple: Tuple = raw_val.into();
                let prefix =
                    tuple
                    .decode(&[format::U16_TYPE])[0]
                    .get_u16();
                prefix >= format::FIRST_RULE_SEQ
            })
        })
    }

    pub fn exists(&self, name: &str, schema: &[u8]) -> Result<bool, anyhow::Error> {
        let arity = schema.len().try_into().unwrap();
        let key = predicate_key_tuple(name, arity)?;

        if let Some(raw_val) = self.tx.get_cf(self.decl, key.get())? {
            let tuple: Tuple = raw_val.into();
            let scm = self.tx.get_cf(self.scm, tuple.get())?.unwrap();
            Ok(scm == schema)
        } else {
            Ok(false)
        }
    }

    pub fn declare(&self, name: &str, schema: &[u8]) -> Result<(), anyhow::Error> {
        let arity = schema.len().try_into().unwrap();

         // a zero arity predicate should be represented as a rule
        assert!(arity > 0);

        let key = predicate_key_tuple(name, arity)?;

        // ensure existing declaration matches (if one exists)
        if let Some(raw_val) = self.tx.get_cf(self.decl, key.get())? {
            let tuple: Tuple = raw_val.into();
            let prefix =
                tuple
                .decode(&[format::U16_TYPE])[0]
                .get_u16();

            // ensure existing declaration is a fact
            if prefix >= format::FIRST_RULE_SEQ {
                return Err(errors::RecallError::RuntimeError(format!(
                    "cannot add fact since predicate {}/{} is a rule",
                    name, arity,
                )).into())
            }

            // ensure the schemas match
            let scm = self.tx.get_cf(self.scm, tuple.get())?.unwrap();
            if scm != schema {
                return Err(errors::RecallError::RuntimeError(format!(
                    "fact {}/{} delcared does not match re-declaration",
                    name, arity,
                )).into())
            }

            return Ok(())
        }

        // no prior declatarion, create a new one for this fact

        // get the current prefix
        let prefix_tuple: Tuple =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
            .unwrap()
            .into();

        // add pair to DECL
        self.tx.put_cf(self.decl, key.get(), prefix_tuple.get())?;

        // add pair to RVLK
        self.tx.put_cf(self.rvlk, prefix_tuple.get(), name.as_bytes())?;

        // add pair to SCM
        self.tx.put_cf(self.scm, prefix_tuple.get(), schema)?;

        // update prefix
        let seq_num =
            prefix_tuple
            .decode(&[format::U16_TYPE])[0]
            .get_u16();

        self.tx.put_cf(self.meta, &[format::NEXT_PREFIX_SEQ], Tuple::encode(
            &[ParameterType::U16(
                seq_num
                .checked_add(1)
                .unwrap()
            )],
            &[format::U16_TYPE],
        )?.get())?;

        Ok(())
    }

    pub fn define(
        &self,
        name: &str,
        text: &str,
        schema: &[u8],
    ) -> Result<(), anyhow::Error> {
        let arity = schema.len().try_into().unwrap();
        let key = predicate_key_tuple(name, arity)?;

        // ensure existing declaration matches (if one exists)
        if let Some(raw_val) = self.tx.get_cf(self.decl, key.get())? {
            let tuple: Tuple = raw_val.into();
            let prefix =
                tuple
                .decode(&[format::U16_TYPE])[0]
                .get_u16();

            // ensure existing declaration is a rule
            if prefix < format::FIRST_RULE_SEQ {
                return Err(errors::RecallError::RuntimeError(format!(
                    "cannot add rule since predicate {}/{} is a fact",
                    name, arity,
                )).into())
            }

            // ensure the schemas match
            let scm = self.tx.get_cf(self.scm, tuple.get())?.unwrap();
            if scm != schema {
                return Err(errors::RecallError::RuntimeError(format!(
                    "rule {}/{} delcared does not match re-declaration",
                    name, arity,
                )).into())
            }

            // update existing rule
            self.tx.put_cf(self.def, tuple.get(), text.as_bytes())?;

            return Ok(())
        }

        // no prior declaration, create a new one for this rule

        // get the current prefix
        let seq_num = {
            let prefix_tuple: Tuple =
                self
                .tx
                .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap()
                .into();

            prefix_tuple
            .decode(&[format::U16_TYPE])[0]
            .get_u16()
        };
        let prefix_tuple = Tuple::encode(
            &[ParameterType::U16(
                seq_num
                .checked_add(format::FIRST_RULE_SEQ)
                .unwrap()
            )],
            &[format::U16_TYPE],
        )?;

        // add pair to DECL
        self.tx.put_cf(self.decl, key.get(), prefix_tuple.get())?;

        // add pair to RVLK
        self.tx.put_cf(self.rvlk, prefix_tuple.get(), name.as_bytes())?;

        // add pair to SCM
        self.tx.put_cf(self.scm, prefix_tuple.get(), schema)?;

        // add pair to DEF
        self.tx.put_cf(self.def, prefix_tuple.get(), text.as_bytes())?;

        // update to the next prefix code
        self.tx.put_cf(self.meta, &[format::NEXT_PREFIX_SEQ], Tuple::encode(
            &[ParameterType::U16(
                seq_num
                .checked_add(1)
                .unwrap()
            )],
            &[format::U16_TYPE],
        )?.get())?;

        Ok(())
    }

    pub fn assert(
        &self,
        name: &str,
        arity: u8,
        input: &[ParameterType],
    ) -> Result<u32, anyhow::Error> {
        let key = predicate_key_tuple(name, arity)?;

        // get predicate's prefix
        let mut prefix =
            self
            .tx
            .get_cf(self.decl, key.get())?
            .unwrap();

        // get schema assoc. with prefix
        let scm =
            self
            .tx
            .get_cf(self.scm, &prefix)?
            .unwrap();

        // get the current tuple seq
        let id_tuple: Tuple =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_TUP_SEQ])?
            .unwrap()
            .into();
        let id =
            id_tuple
            .decode(&[format::U32_TYPE])[0]
            .get_u32();

        // extend prefix with id
        prefix.extend_from_slice(id_tuple.get());

        // insert tuple
        let tup = Tuple::encode(input, &scm)?;
        self.tx.put_cf(self.tup, prefix, tup.get())?;

        // update to the next prefix code
        let tup = Tuple::encode(
            &[ParameterType::U32(
                id
                .checked_add(1)
                .unwrap()
            )],
            &[format::U32_TYPE],
        )?;
        self.tx.put_cf(self.meta, &[format::NEXT_TUP_SEQ], tup.get())?;

        Ok(id)
    }

    pub fn retract(&self, name: &str, arity: u8, id: u32) -> Result<(), anyhow::Error> {
        let key = predicate_key_tuple(name, arity)?;

        // get predicate's prefix
        let prefix =
            self
            .tx
            .get_cf(self.decl, key.get())?;

        if let Some(mut prefix) = prefix {
            let id_tuple = Tuple::encode(
                &[ParameterType::U32(id)], &[format::U32_TYPE]
            )?;

            // extend prefix with id
            prefix.extend_from_slice(id_tuple.get());

            // delete tuple
            self.tx.delete_cf(self.tup, prefix)?;

            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn prefix_all(&self) -> Option<Query> {
        Some(b"".to_vec().into())
    }

    pub fn prefix(
        &self,
        name: &str,
        arity: u8,
        with_id: Option<u32>,
    ) -> Result<Option<Query>, anyhow::Error> {
        let key = predicate_key_tuple(name, arity)?;

        // get predicate's prefix
        let prefix =
            self
            .tx
            .get_cf(self.decl, key.get())?;

        // get predicate's prefix
        if let Some(mut prefix) = prefix {
            if let Some(id) = with_id {
                let id_tuple = Tuple::encode(
                    &[ParameterType::U32(id)], &[format::U32_TYPE]
                )?;

                // extend prefix with id
                prefix.extend_from_slice(id_tuple.get());
            }

            Ok(Some(prefix.into()))
        } else {
            Ok(None)
        }
    }

    pub fn query(
        &'a self,
        query: Option<Query>,
    ) -> Result<Box<dyn Iterator<Item = (u32, FunctorTerm)> + 'a>, anyhow::Error> {
        if let Some(tuple) = query {
            let mut read_opts = ReadOptions::default();
            read_opts.set_iterate_range(PrefixRange(tuple.get()));

            let iter = self.tx.iterator_cf_opt(
                self.tup,
                read_opts,
                IteratorMode::Start,
            );

            Ok(Box::new(TupleIterator { iter, rvlk: self.rvlk, scm: self.scm, tx: &self.tx }))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
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
        let key = predicate_key_tuple(name, arity)?;

        // remove rule from: decl
        // gc will eventually purge from other column families
        self
        .tx
        .delete_cf(self.decl, key.get())?;

        Ok(())
    }

    pub fn gc(&self) -> Result<(), anyhow::Error> {
        todo!()
    }

    pub fn info(&self) -> Result<(), anyhow::Error> {
        println!("meta:");
        let next_prefix_tuple: Tuple =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
            .unwrap()
            .into();
        let next_prefix_seq =
            next_prefix_tuple
            .decode(&[format::U16_TYPE])[0]
            .get_u16();

        let next_tup_tuple: Tuple =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_TUP_SEQ])?
            .unwrap()
            .into();
        let next_tup_seq =
            next_tup_tuple
            .decode(&[format::U32_TYPE])[0]
            .get_u32();
        println!("\tnext prefix seq {}", next_prefix_seq);
        println!("\tnext tup seq    {}", next_tup_seq);

        println!();

        println!("declarations:");
        let iter = self.tx.iterator_cf(
            self.decl,
            IteratorMode::Start,
        );
        for result in iter {
            let (raw_key, raw_val) = result?;
            let key_tuple: Tuple = raw_key.into();
            let val_tuple: Tuple = raw_val.into();
            let key =
                key_tuple
                .decode(&[format::ATOM_TYPE, format::U8_TYPE]);
            let val =
                val_tuple
                .decode(&[format::U16_TYPE]);
            let name = key[0].get_atom();
            let arity = key[1].get_u8();
            let prefix = val[0].get_u16();

            println!("\t{}/{} ({})", name, arity, prefix);
        }

        println!();

        println!("tuples:");
        let iter = self.query(self.prefix_all())?;
        for (id, functor) in iter {
            println!("\t {} ({})", functor, id);
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
    type Item = (u32, FunctorTerm);

    // TODO: this may need to be Result<(u32, FunctorTerm)> as Item
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (raw_key, raw_val) = result.unwrap();
            let key_tuple: Tuple = raw_key.into();
            let val_tuple: Tuple = raw_val.into();
            let key =
                key_tuple
                .decode(&[format::U16_TYPE, format::U32_TYPE]);
            let id =
                key[1]
                .get_u32();
            let prefix =
                Tuple::encode(&key[0..1], &[format::U16_TYPE])
                .unwrap();

            // get predicate name
            let raw_val =
                self
                .tx
                .get_cf(self.rvlk, prefix.get())
                .unwrap().unwrap();
            let name =
                String::from_utf8(raw_val)
                .expect("invalid predicate name");

            // get predicate scm
            let scm =
                self
                .tx
                .get_cf(self.scm, prefix.get())
                .unwrap().unwrap();

            // get term
            let val = val_tuple.decode(&scm);
            let functor = Term::functor(
                name,
                val.into_iter().map(Term::from).collect(),
            );

            Some((id, functor))
        } else {
            None
        }
    }
}

impl<'a> Iterator for RulesIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (_, raw_val) = result.unwrap();
            let rule_text =
                String::from_utf8(raw_val.to_vec())
                .expect("invalid rule text");
            Some(rule_text)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    impl DB {
        fn new_temp_for_testing(test_name: &str) -> Result<Self, anyhow::Error> {
            let mut path = std::env::temp_dir();
            path.push(test_name);
            fs::create_dir(&path)?;
            DB::setup_db(&path, false)
        }
    }

    #[test]
    fn it_can_declare_predicates() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_declare_predicates").unwrap();
        let tx = db.begin_transaction();

        let foo_predicate = (
            "foo",
            &[format::ATOM_TYPE, format::INT_TYPE, format::STRING_TYPE],
            3,
        );

        // declare initial predicate
        {
            tx.declare(foo_predicate.0, foo_predicate.1)?;

            // DECL was set?
            let key = predicate_key_tuple(foo_predicate.0, foo_predicate.2)?;
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key.get())?
                .unwrap();
            let prefix_tuple: Tuple = prefix.into();
            assert_eq!(&[0, 0], prefix_tuple.get());

            // RVLK was set?
            let raw_name =
                tx
                .tx
                .get_cf(tx.rvlk, prefix_tuple.get())?
                .unwrap();
            assert_eq!(
                foo_predicate.0,
                String::from_utf8(raw_name).unwrap(),
            );

            // SCM was set?
            let tup_scm =
                tx
                .tx
                .get_cf(tx.scm, prefix_tuple.get())?
                .unwrap();
            assert_eq!(tup_scm, foo_predicate.1);

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let next_prefix_tuple: Tuple = next_prefix.into();
            assert_eq!(&[0, 1], next_prefix_tuple.get());
        }

        // re-declare with same scm
        {
            tx.declare(foo_predicate.0, foo_predicate.1).unwrap();
        }

        // re-declare with different scm
        {
            let tup_scm_different =
                foo_predicate
                .1
                .clone()
                .into_iter()
                .rev()
                .collect::<Vec<u8>>();
            let err =
                tx
                .declare(foo_predicate.0, &tup_scm_different)
                .unwrap_err();
            assert!(err.to_string().contains("does not match"));
        }

        // declare using existing name defined as a rule
        {
            let key = predicate_key_tuple("bar", foo_predicate.2)?;
            let prefix_tuple = Tuple::encode(
                &[ParameterType::U16(format::FIRST_RULE_SEQ)],
                &[format::U16_TYPE],
            )?;

            // simulate have a rule named "bar/3"
            tx
            .tx
            .put_cf(tx.decl, key.get(), prefix_tuple.get())?;

            let err =
                tx
                .declare("bar", foo_predicate.1)
                .unwrap_err();
            assert!(err.to_string().contains("predicate bar/3 is a rule"));
        }

        // ensure seq has not advanced from subsequent declarations
        {
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let next_prefix_tuple: Tuple = next_prefix.into();
            assert_eq!(&[0, 1], next_prefix_tuple.get());
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_define_rules() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_define_rules").unwrap();
        let tx = db.begin_transaction();

        let foo_predicate = (
            "foo",
            &[format::ATOM_TYPE, format::ATOM_TYPE],
            2,
            "foo(X, Y) :- bar(X), baz(Y).",
        );

        // define initial rule
        {
            tx.define(
                foo_predicate.0,
                foo_predicate.3,
                foo_predicate.1,
            )?;

            // DECL was set?
            let key = predicate_key_tuple(foo_predicate.0, foo_predicate.2)?;
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key.get())?
                .unwrap();
            let prefix_tuple: Tuple = prefix.into();
            assert_eq!(format::FIRST_RULE_SEQ.to_be_bytes(), prefix_tuple.get());

            // RVLK was set?
            let raw_name =
                tx
                .tx
                .get_cf(tx.rvlk, prefix_tuple.get())?
                .unwrap();
            assert_eq!(
                foo_predicate.0,
                String::from_utf8(raw_name).unwrap(),
            );

            // SCM was set?
            let rule_scm =
                tx
                .tx
                .get_cf(tx.scm, prefix_tuple.get())?
                .unwrap();
            assert_eq!(rule_scm, foo_predicate.1);

            // DEF was set?
            let raw_def =
                tx
                .tx
                .get_cf(tx.def, prefix_tuple.get())?
                .unwrap();
            assert_eq!(
                foo_predicate.3,
                String::from_utf8(raw_def).unwrap(),
            );

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let next_prefix_tuple: Tuple = next_prefix.into();
            assert_eq!(&[0, 1], next_prefix_tuple.get());

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([foo_predicate.3.to_string()]), rules);
        }

        // define subsequent rule
        {
            tx.define("succeed", "succeed :- true.", b"")?;

            // prefix (for rule) set correctly?
            let key = predicate_key_tuple("succeed", 0)?;
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key.get())?
                .unwrap();
            let prefix_tuple: Tuple = prefix.into();
            assert_eq!((format::FIRST_RULE_SEQ + 1).to_be_bytes(), prefix_tuple.get());

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let next_prefix_tuple: Tuple = next_prefix.into();
            assert_eq!(&[0, 2], next_prefix_tuple.get());

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([
                foo_predicate.3.to_string(),
                "succeed :- true.".to_string(),
            ]), rules);
        }

        // overwrite rule with re-definition
        {
            let new_rule = "foo(X, Y) :- baz(X, Y).";
            tx.define(foo_predicate.0, new_rule, foo_predicate.1)?;

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([
                new_rule.to_string(),
                "succeed :- true.".to_string(),
            ]), rules);
        }

        // re-define with different scm
        {
            let rule_scm1 = &[format::STRING_TYPE, format::STRING_TYPE];
            let err =
                tx
                .define(foo_predicate.0, foo_predicate.3, rule_scm1)
                .unwrap_err();
            assert!(err.to_string().contains("does not match"));
        }

        // declare using existing name defined as a fact
        {
            let key = predicate_key_tuple("bar", foo_predicate.2)?;
            let prefix_tuple = Tuple::encode(
                &[ParameterType::U16(0)],
                &[format::U16_TYPE],
            )?;

            // simulate have a fact named "bar/2"
            tx
            .tx
            .put_cf(tx.decl, key.get(), prefix_tuple.get())?;

            let err =
                tx
                .define("bar", foo_predicate.3, foo_predicate.1)
                .unwrap_err();
            assert!(err.to_string().contains("predicate bar/2 is a fact"));
        }

        // ensure seq has not advanced from subsequent declarations
        {
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            let next_prefix_tuple: Tuple = next_prefix.into();
            assert_eq!(&[0, 2], next_prefix_tuple.get());
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_assert_and_query() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_assert_and_query").unwrap();
        let tx = db.begin_transaction();

        let foo_predicate = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );
        let bar_predicate = (
            "bar",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare facts used
        {
            tx.declare(foo_predicate.0, foo_predicate.2)?;
            tx.declare(bar_predicate.0, bar_predicate.2)?;
        }

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let tuple = &[
                    ParameterType::Atom("apple".to_string()),
                    ParameterType::Int(i as i32),
                ];

                assert_eq!(tx.assert(
                    foo_predicate.0,
                    foo_predicate.1,
                    tuple,
                )?, i);
            }
        }

        // insert a fact for bar
        {
            let tuple = &[
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ];
            assert_eq!(tx.assert(
                bar_predicate.0,
                bar_predicate.1,
                tuple,
            )?, k);
        }

        // query all foo(X, Y)?
        {
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
            ]), result);
        }

        // query by specific id
        {
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, Some(1),
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
            ]), result);
        }


        // query on prefix that doesn't exist
        {
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1 + 1, None
                )?)?
                .collect::<Vec<_>>();

            assert_eq!(result.len(), 0);
        }

        // query all tuples
        {
            let result =
                tx
                .query(tx.prefix_all())?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
                (3, Term::Functor(
                    "bar".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(42)]
                )),
            ]), result);
        }


        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_retract() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_retract").unwrap();
        let tx = db.begin_transaction();

        let foo_predicate = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare fact
        {
            tx.declare(foo_predicate.0, foo_predicate.2)?;
        }

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let tuple = &[
                    ParameterType::Atom("apple".to_string()),
                    ParameterType::Int(i as i32),
                ];

                assert_eq!(tx.assert(
                    foo_predicate.0,
                    foo_predicate.1,
                    tuple,
                )?, i);
            }
        }

        // query all foo(X, Y)?
        {
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
            ]), result);
        }

        // retract some facts
        {
            tx.retract(foo_predicate.0, foo_predicate.1, 1)?;
            tx.retract(foo_predicate.0, foo_predicate.1, 100)?;
            tx.retract(foo_predicate.0, foo_predicate.1 + 1, 1)?;
        }

        // query all foo(X, Y)? again
        {
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
            ]), result);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_forgot() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_forgot").unwrap();
        let tx = db.begin_transaction();

        let foo_predicate = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare fact
        {
            tx.declare(foo_predicate.0, foo_predicate.2)?;
        }

        {
            // DECL was set?
            let key = predicate_key_tuple(foo_predicate.0, foo_predicate.1)?;
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key.get())?
                .unwrap();
            let prefix_tuple: Tuple = prefix.into();
            assert_eq!(&[0, 0], prefix_tuple.get());
        }

        // forgot fact
        {
            tx.forget(foo_predicate.0, foo_predicate.1)?;
        }

        {
            // DECL was unset?
            let key = predicate_key_tuple(foo_predicate.0, foo_predicate.1)?;
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key.get())?;
            assert_eq!(prefix, None);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_abort_tx() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_abort_tx").unwrap();

        let foo_predicate = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // set intial db state
        {
            let tx = db.begin_transaction();
            tx.declare(foo_predicate.0, foo_predicate.2)?;

            for i in 0..3 {
                let tuple = &[
                    ParameterType::Atom("apple".to_string()),
                    ParameterType::Int(i as i32),
                ];
                assert_eq!(tx.assert(
                    foo_predicate.0, foo_predicate.1, tuple,
                )?, i);
            }

            // query all foo(X, Y)?
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
            ]));

            tx.commit()?;
        }

        // change state, but rollback
        {
            let tx = db.begin_transaction();

            tx.retract(foo_predicate.0, foo_predicate.1, 0)?;
            tx.retract(foo_predicate.0, foo_predicate.1, 1)?;
            tx.retract(foo_predicate.0, foo_predicate.1, 2)?;

            // query all foo(X, Y)?
            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::new(), result);

            tx.rollback()?;
        }

        // confirm initial state intact
        {
            let tx = db.begin_transaction();

            let result =
                tx
                .query(tx.prefix(
                    foo_predicate.0, foo_predicate.1, None
                )?)?
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(0)]
                )),
                (1, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(1)]
                )),
                (2, Term::Functor(
                    "foo".to_string(),
                    vec![Term::Atom("apple".to_string()), Term::Integer(2)]
                )),
            ]));

            tx.commit()?;
        }

        Ok(())
    }
}
