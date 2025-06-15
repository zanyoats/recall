use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use std::process;

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
    db: &'a TransactionDB<SingleThreaded>,
    pub tx: rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
}

pub struct Predicate<'a> {
    db: &'a TransactionDB<SingleThreaded>,
    tx: &'a rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
    pub name: String,
    pub tup_scm: Vec<u8>,
}

impl<'a> IntoIterator for Predicate<'a> {
    type Item = Term;

    type IntoIter = PredicateIntoIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PredicateIntoIterator::new(self)
    }
}

pub struct PredicateIntoIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
    pub tup_scm: Vec<u8>,
}

pub struct PredicatesIterator<'a> {
    db: &'a TransactionDB<SingleThreaded>,
    tx: &'a rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

pub struct RulesIterator<'a> {
    iter: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, TransactionDB>>,
}

impl DB {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self, errors::RecallError> {
        DB::setup_db(file_path.as_ref(), true)
    }

    pub fn new_temp() -> Result<Self, errors::RecallError> {
        let temp_path = DB::temp_path()?;
        DB::setup_db(&temp_path, false)
    }

    fn temp_path() -> Result<PathBuf, errors::RecallError> {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let pid = process::id();
        let unique_name = format!("recall_{}_{}", pid, timestamp);
        path.push(unique_name);
        fs::create_dir(&path)?;
        Ok(path)
    }

    fn setup_db(path: &Path, persist: bool) -> Result<Self, errors::RecallError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(500);
        db_opts.increase_parallelism(4);

        let cf_names = vec!["default", "schema", "data", "rules"];
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

        Ok(Self { db: Some(db), persist, path: path.to_path_buf() })
    }

    pub fn begin_transaction(&self) -> TransactionOp {
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);
        let write_opts = WriteOptions::default();
        let db = self.db.as_ref().unwrap();
        let tx = db.transaction_opt(&write_opts, &txn_opts);
        TransactionOp { db, tx }
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

impl<'a> TransactionOp<'a> {
    pub fn assert_rule(&self, rule_text: String) -> Result<(), errors::RecallError> {
        let rules_cf = self.db.cf_handle("rules").unwrap();
        self.tx.put_cf(rules_cf, rule_text.as_bytes(), b"")?;
        Ok(())
    }

    pub fn retract_rule(&self, rule_text: String) -> Result<(), errors::RecallError> {
        let rules_cf = self.db.cf_handle("rules").unwrap();
        self.tx.delete_cf(rules_cf, rule_text.as_bytes())?;
        Ok(())
    }

    pub fn define_predicate(&self, name: &str, mut tup_scm: Vec<u8>) -> Result<Predicate, errors::RecallError> {
        let key_scm = vec![format::ATOM_TYPE, format::INT_TYPE];
        let arity = tup_scm.len().try_into().unwrap();

        let mut tup_scm0 = vec![format::ATOM_TYPE];
        tup_scm0.append(&mut tup_scm);

        let schema_cf = self.db.cf_handle("schema").unwrap();
        let key = Tuple::encode(
            &[ParameterType::Atom(name.to_string()), ParameterType::Int(arity)],
            &key_scm,
        )?;

        if let Some(existing_tup_scm) = self.tx.get_cf(schema_cf, key.get())? {
            if existing_tup_scm == tup_scm0 {
                Ok(Predicate { db: self.db, tx: &self.tx, name: name.to_string(), tup_scm: tup_scm0 })
            } else {
                Err(errors::RecallError::TypeError(format!(
                    "predicate type defined does not typecheck with re-declaration",
                )))
            }
        } else {
            self.tx.put_cf(schema_cf, key.get(), &tup_scm0)?;

            Ok(Predicate { db: self.db, tx: &self.tx, name: name.to_string(), tup_scm: tup_scm0 })
        }
    }

    pub fn get_predicate(&self, name: &str, arity: i32) -> Result<Predicate, errors::RecallError> {
        let key_scm = vec![format::ATOM_TYPE, format::INT_TYPE];

        let schema_cf = self.db.cf_handle("schema").unwrap();
        let key = Tuple::encode(
            &[ParameterType::Atom(name.to_string()), ParameterType::Int(arity)],
            &key_scm,
        )?;

        if let Some(tup_scm) = self.tx.get_cf(schema_cf, key.get())? {
            Ok(Predicate { db: self.db, tx: &self.tx, name: name.to_string(), tup_scm })
        } else {
            Err(errors::RecallError::PredicateNotFound(name.to_string(), arity))
        }
    }

    pub fn predicates(&'a self) -> PredicatesIterator<'a> {
        PredicatesIterator::new(self.db, &self.tx)
    }

    pub fn rules(&'a self) -> RulesIterator<'a> {
        RulesIterator::new(self.db, &self.tx)
    }

    pub fn commit(self) -> Result<(), errors::RecallError> {
        self.tx.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<(), errors::RecallError> {
        self.tx.rollback()?;
        Ok(())
    }
}

impl<'a> Predicate<'a> {
    pub fn assert(&self, tup: Vec<ParameterType>) -> Result<(), errors::RecallError> {
        let data_cf = self.db.cf_handle("data").unwrap();
        ParameterType::typecheck(&tup, &self.tup_scm)?;
        let key = Tuple::encode(&tup, &self.tup_scm)?;
        self.db.put_cf(data_cf, key.get(), b"")?;
        Ok(())
    }

    pub fn retract(&self, tup: Vec<ParameterType>) -> Result<(), errors::RecallError> {
        let data_cf = self.db.cf_handle("data").unwrap();
        ParameterType::typecheck(&tup, &self.tup_scm)?;
        let key = Tuple::encode(&tup, &self.tup_scm)?;
        self.db.delete_cf(data_cf, key.get())?;
        Ok(())
    }
}

impl<'a> PredicatesIterator<'a> {
    fn new(
        db: &'a TransactionDB<SingleThreaded>,
        tx: &'a rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
    ) -> Self {
        let schema_cf = db.cf_handle("schema").unwrap();
        let iter = tx.iterator_cf(schema_cf, IteratorMode::Start);

        PredicatesIterator { db, tx, iter }
    }
}

impl<'a> RulesIterator<'a> {
    fn new(
        db: &'a TransactionDB<SingleThreaded>,
        tx: &'a rocksdb::Transaction<'a, TransactionDB<SingleThreaded>>,
    ) -> Self {
        let rules_cf = db.cf_handle("rules").unwrap();
        let iter = tx.iterator_cf(rules_cf, IteratorMode::Start);

        RulesIterator { iter }
    }
}

impl<'a> Iterator for PredicatesIterator<'a> {
    type Item = Predicate<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (raw_key, tup_scm) = result.unwrap();
            let tup: Tuple = raw_key.into();
            let key_scm = vec![format::ATOM_TYPE, format::INT_TYPE];
            let key = tup.decode(&key_scm);
            Some(Predicate {
                db: self.db,
                tx: self.tx,
                name: key[0].get_atom().to_string(),
                tup_scm: tup_scm.into_vec(),
            })
        } else {
            None
        }
    }
}

impl<'a> Iterator for RulesIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (raw_key, _) = result.unwrap();
            let rule_text = String::from_utf8(raw_key.to_vec())
                .expect("invalid rule text");
            Some(rule_text)
        } else {
            None
        }
    }
}

impl<'a> PredicateIntoIterator<'a> {
    fn new(predicate: Predicate<'a>) -> Self {
        let prefix = Tuple::encode(
            &[ParameterType::Atom(predicate.name)],
            &[format::ATOM_TYPE],
        ).unwrap();

        let data_cf = predicate.db.cf_handle("data").unwrap();

        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_range(PrefixRange(prefix.get()));

        let iter = predicate.tx.iterator_cf_opt(
            data_cf,
            read_opts,
            IteratorMode::Start,
        );
        PredicateIntoIterator { iter, tup_scm: predicate.tup_scm }
    }
}

impl<'a> Iterator for PredicateIntoIterator<'a> {
    type Item = Term;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.iter.next() {
            let (raw_key, _) = result.unwrap();
            let tup: Tuple = raw_key.into();
            let key = tup.decode(&self.tup_scm);
            Some(key.into())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::collections::HashSet;
    use crate::errors::GenericError;

    struct Fixture {
        temp_path: std::path::PathBuf,
    }

    impl Fixture {
        fn new(file_name: &str) -> Self {
            let mut temp_path = env::temp_dir();
            temp_path.push(file_name);
            Fixture {
                temp_path,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_file(&self.temp_path).unwrap_or(());
        }
    }

    #[test]
    fn it_can_define_predicates() -> Result<(), GenericError> {
        let fixture = Fixture::new("it_can_define_predicates.db");
        let db =
            DB::new(&fixture.temp_path)
            .unwrap();
        let predicate_name = "foo";
        let predicate_schema = vec![format::ATOM_TYPE, format::INT_TYPE];
        let trx = db.begin_transaction();
        {
            let _ = trx.define_predicate(predicate_name, predicate_schema.clone())?;
            let predicate = trx.get_predicate(predicate_name, predicate_schema.len().try_into().unwrap())?;
            assert_eq!(predicate.name, "foo");
            assert_eq!(predicate.tup_scm, vec![format::ATOM_TYPE, format::ATOM_TYPE, format::INT_TYPE]);
        }
        trx.commit()?;
        Ok(())
    }

    #[test]
    fn it_can_assert_on_predicate() -> Result<(), GenericError> {
        let fixture = Fixture::new("it_can_assert_on_predicate.db");
        let db =
            DB::new(&fixture.temp_path)
            .unwrap();

        let trx = db.begin_transaction();
        {
            let predicate = trx.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE])?;
            predicate.assert(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ])?;
            predicate.assert(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ])?;
            predicate.assert(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("lemon".to_string()),
                ParameterType::Int(1337),
            ])?;

            let predicate = trx.define_predicate("bar", vec![format::STRING_TYPE, format::INT_TYPE])?;
            predicate.assert(vec![
                ParameterType::Atom("bar".to_string()),
                ParameterType::Str("cake".to_string()),
                ParameterType::Int(2001),
            ])?;
            predicate.assert(vec![
                ParameterType::Atom("bar".to_string()),
                ParameterType::Str("crepe".to_string()),
                ParameterType::Int(99),
            ])?;
        }

        assert_eq!(trx.predicates().collect::<Vec<Predicate<'_>>>().len(), 2);

        let got: HashSet<Term>=
            trx
            .predicates()
            .flat_map(|predicate| predicate.into_iter().collect::<Vec<Term>>())
            .collect();

        let want: HashSet<Term> =
            HashSet::from_iter(vec![
                Term::Functor("bar".to_string(), vec![Term::Str("cake".to_string()), Term::Integer(2001)]),
                Term::Functor("bar".to_string(), vec![Term::Str("crepe".to_string()), Term::Integer(99)]),
                Term::Functor("foo".to_string(), vec![Term::Atom("apple".to_string()), Term::Integer(42)]),
                Term::Functor("foo".to_string(), vec![Term::Atom("lemon".to_string()), Term::Integer(1337)]),
            ].into_iter());

        assert_eq!(got, want);

        trx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_retract_on_predicate() -> Result<(), GenericError> {
        let fixture = Fixture::new("it_can_retract_on_predicate.db");
        let db =
            DB::new(&fixture.temp_path)
            .unwrap();
        let trx = db.begin_transaction();
        {
            let predicate = trx.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE])?;

            predicate.assert(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ])?;
            predicate.assert(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("lemon".to_string()),
                ParameterType::Int(1337),
            ])?;

            let got =
                predicate
                .into_iter()
                .collect::<Vec<Term>>();

            assert_eq!(got, vec![
                Term::Functor("foo".to_string(), vec![Term::Atom("apple".to_string()), Term::Integer(42)]),
                Term::Functor("foo".to_string(), vec![Term::Atom("lemon".to_string()), Term::Integer(1337)]),
            ]);
        }

        {
            let predicate = trx.get_predicate("foo", 2)?;

            predicate.retract(vec![
                ParameterType::Atom("foo".to_string()),
                ParameterType::Atom("lemon".to_string()),
                ParameterType::Int(1337),
            ])?;

            let got =
                predicate
                .into_iter()
                .collect::<Vec<Term>>();

            assert_eq!(got, vec![
                Term::Functor("foo".to_string(), vec![Term::Atom("apple".to_string()), Term::Integer(42)]),
            ]);
        }

        trx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_handle_duplicate_definitions() -> Result<(), GenericError> {
        let fixture = Fixture::new("it_can_handle_duplicate_definitions.db");
        let db =
            DB::new(&fixture.temp_path)
            .unwrap();
        let trx = db.begin_transaction();
        assert!(trx.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE]).is_ok());
        assert!(trx.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE]).is_ok());
        assert!(trx.define_predicate("foo", vec![format::ATOM_TYPE, format::STRING_TYPE]).is_err());
        trx.commit()?;
        Ok(())
    }
}
