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
use crate::storage::format::AtomSpec;
use crate::storage::format::ValEncodable;
use crate::storage::format::ValDecodable;
use crate::storage::format::TaggedVal;
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
            format::SCM,
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
        let scm = db.cf_handle(format::SCM).unwrap();
        let def = db.cf_handle(format::DEF).unwrap();
        let tup = db.cf_handle(format::TUP).unwrap();
        let meta = db.cf_handle(format::META).unwrap();
        let idx = db.cf_handle(format::IDX).unwrap();
        TransactionOp { decl, rvlk, scm, def, tup, meta, idx, tx }
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

    pub fn get_scm(&self, prefix: Vec<u8>) -> Result<AtomSpec, anyhow::Error> {
        let raw_spec = self.tx.get_cf(self.scm, prefix)?.unwrap();
        let atom_spec = AtomSpec::decode(&raw_spec)?;
        Ok(atom_spec)
    }

    pub fn is_rule(&self, name: &str, arity: u8) -> Result<Option<bool>, anyhow::Error> {
        Ok(
            self
            .get_prefix(name, arity)?
            .map(|prefix| {
                let (prefix_code, _) = prefix.as_slice().decode_u16();
                prefix_code >= format::FIRST_RULE_SEQ
            })
        )
    }

    pub fn exists_with_same_schema(&self, name: &str, schema: &[u8]) -> Result<bool, anyhow::Error> {
        let arity = schema.len().try_into().unwrap();

        if let Some(prefix) = self.get_prefix(name, arity)? {
            let raw_spec =
                self
                .tx
                .get_cf(self.scm, prefix)?
                .unwrap();
            let atom_spec = AtomSpec::decode(&raw_spec)?;
            Ok(schema == atom_spec.schema)
        } else {
            Ok(false)
        }
    }

    pub fn declare(
        &self,
        name: &str,
        schema: Vec<u8>,
        labels: Vec<String>,
        indexes: Vec<Vec<u8>>,
    ) -> Result<(), anyhow::Error> {
        let arity = schema.len().try_into().unwrap();

         // a zero arity predicate should be represented as a rule
        assert!(arity > 0);

        let key = build_predicate_key(&name, arity);

        // ensure re-declaration is valid
        if let Some(prefix) = self.tx.get_cf(self.decl, &key)? {
            let (prefix_code, _) = prefix.as_slice().decode_u16();

            // existing declaration is a fact?
            if prefix_code >= format::FIRST_RULE_SEQ {
                return Err(errors::RecallError::RuntimeError(format!(
                    "cannot add fact since predicate {}/{} is a rule",
                    name, arity,
                )).into())
            }

            // schemas match?
            let raw_spec = self.tx.get_cf(self.scm, prefix)?.unwrap();
            let atom_spec = AtomSpec::decode(&raw_spec)?;

            if schema != atom_spec.schema {
                return Err(errors::RecallError::RuntimeError(format!(
                    "fact {}/{} delcared does not match re-declaration",
                    name, arity,
                )).into())
            }

            return Ok(())
        }

        // no prior declaration, create a new one for this fact

        // get the current prefix
        let prefix =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
            .unwrap();

        // add pair to DECL
        self.tx.put_cf(self.decl, &key, &prefix)?;

        // add pair to RVLK
        self.tx.put_cf(self.rvlk, &prefix, name.as_bytes())?;

        // add pair to SCM
        let atom_spec = AtomSpec {
            schema,
            labels,
            indexes,
        };
        let encoded = AtomSpec::encode(&atom_spec)?;
        self.tx.put_cf(self.scm, &prefix, encoded)?;

        // update prefix
        {
            let (prefix_code, _) = prefix.as_slice().decode_u16();
            let mut buf = vec![];
            let next_prefix_code = prefix_code.checked_add(1).unwrap();
            next_prefix_code.encode_append(&mut buf);
            self.tx.put_cf(self.meta, &[format::NEXT_PREFIX_SEQ], buf)?;
        }

        Ok(())
    }

    pub fn define(
        &self,
        name: &str,
        text: &str,
        schema: Vec<u8>,
    ) -> Result<(), anyhow::Error> {
        let arity = schema.len().try_into().unwrap();
        let key = build_predicate_key(name, arity);

        // ensure re-declaration is valid
        if let Some(prefix) = self.tx.get_cf(self.decl, &key)? {
            let (prefix_code, _) = prefix.as_slice().decode_u16();

            // declaration is a rule?
            if prefix_code < format::FIRST_RULE_SEQ {
                return Err(errors::RecallError::RuntimeError(format!(
                    "cannot add rule since predicate {}/{} is a fact",
                    name, arity,
                )).into())
            }

            // schemas match?
            let raw_spec = self.tx.get_cf(self.scm, &prefix)?.unwrap();
            let atom_spec = AtomSpec::decode(&raw_spec)?;

            if schema != atom_spec.schema {
                return Err(errors::RecallError::RuntimeError(format!(
                    "rule {}/{} delcared does not match re-declaration",
                    name, arity,
                )).into())
            }

            // update existing rule
            self.tx.put_cf(self.def, &prefix, text.as_bytes())?;

            return Ok(())
        }

        // no prior declaration, create a new one for this rule

        // get the current prefix
        let prefix =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_PREFIX_SEQ])?
            .unwrap();
        let (prefix_code0, _) = prefix.as_slice().decode_u16();
        let prefix_code = prefix_code0.checked_add(format::FIRST_RULE_SEQ).unwrap();
        let mut prefix = vec![];
        prefix_code.encode_append(&mut prefix);

        // add pair to DECL
        self.tx.put_cf(self.decl, &key, &prefix)?;

        // add pair to RVLK
        self.tx.put_cf(self.rvlk, &prefix, name.as_bytes())?;

        // add pair to SCM
        let atom_spec = AtomSpec {
            schema,
            labels: vec![],
            indexes: vec![],
        };
        let encoded = AtomSpec::encode(&atom_spec)?;
        self.tx.put_cf(self.scm, &prefix, encoded)?;

        // add pair to DEF
        self.tx.put_cf(self.def, &prefix, text.as_bytes())?;

        // update prefix
        {
            let mut buf = vec![];
            let next_prefix_code = prefix_code0.checked_add(1).unwrap();
            next_prefix_code.encode_append(&mut buf);
            self.tx.put_cf(self.meta, &[format::NEXT_PREFIX_SEQ], buf)?;
        }

        Ok(())
    }

    pub fn assert(
        &self,
        name: &str,
        arity: u8,
        input: &[TaggedVal],
    ) -> Result<u32, anyhow::Error> {
        let key = build_predicate_key(name, arity);

        // get predicate's prefix
        let prefix =
            self
            .tx
            .get_cf(self.decl, &key)?
            .unwrap();

        // get schema assoc. with prefix
        let raw_spec =
            self
            .tx
            .get_cf(self.scm, &prefix)?
            .unwrap();
        let atom_spec = AtomSpec::decode(&raw_spec)?;

        // get the current tuple seq
        let id =
            self
            .tx
            .get_cf(self.meta, &[format::NEXT_TUP_SEQ])?
            .unwrap();
        let (id_num, _) = id.as_slice().decode_u32();

        // insert tuple
        {
            let mut tuple_prefix = vec![];
            tuple_prefix.extend_from_slice(&prefix);
            id_num.encode_append(&mut tuple_prefix);
            let encoded =
                TaggedVal::encode_from_scm(
                    input,
                    &atom_spec.schema,
                    0..atom_spec.schema.len().try_into().unwrap(),
                )?;
            self.tx.put_cf(self.tup, &tuple_prefix, encoded)?;

        }

        // insert index projections
        {
            for index in atom_spec.indexes {
                let mut key = vec![];
                key.extend_from_slice(&prefix);
                for &pos in index.iter() {
                    pos.encode_append(&mut key);
                }
                let encoded =
                    TaggedVal::encode_from_scm(
                        input,
                        &atom_spec.schema,
                        index.into_iter(),
                    )?;
                key.extend(encoded.into_iter());
                id_num.encode_append(&mut key);

                let mut val = vec![];
                id_num.encode_append(&mut val);

                self
                .tx
                .put_cf(self.idx, key, val)?;
            }
        }

        // update id
        {
            let mut buf = vec![];
            let next_id_num = id_num.checked_add(1).unwrap();
            next_id_num.encode_append(&mut buf);
            self.tx.put_cf(self.meta, &[format::NEXT_TUP_SEQ], buf)?;
        }

        Ok(id_num)
    }

    pub fn retract(&self, name: &str, arity: u8, tuple_id: u32) -> Result<(), anyhow::Error> {
        if let Some(mut prefix) = self.get_prefix(name, arity)? {
            tuple_id.encode_append(&mut prefix);

            // delete tuple
            self.tx.delete_cf(self.tup, prefix)?;
        }

        Ok(())
    }

    pub fn rule(&self, name: &str, arity: u8) -> Result<Option<String>, anyhow::Error> {
        let key = build_predicate_key(name, arity);

        if let Some(prefix) = self.tx.get_cf(self.decl, key)? {
            if let Some(rules) = self.tx.get_cf(self.def, prefix)? {
                let text = String::from_utf8(rules).unwrap();
                return Ok(Some(text))
            }
        }

        Ok(None)
    }

    pub fn prefix(
        &self,
        name: &str,
        arity: u8,
        with_id: Option<u32>,
    ) -> Result<Option<Query>, anyhow::Error> {
        let key = build_predicate_key(name, arity);

        // get predicate's prefix
        let prefix =
            self
            .tx
            .get_cf(self.decl, key)?;

        // get predicate's prefix
        if let Some(mut prefix) = prefix {
            if let Some(id) = with_id {
                id.encode_append(&mut prefix);
            }

            Ok(Some(prefix))
        } else {
            Ok(None)
        }
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
    use crate::lang::parse::Term;
    use crate::lang::Atom;
    use std::collections::HashSet;

    impl DB {
        fn new_temp_for_testing(test_name: &str) -> Result<Self, anyhow::Error> {
            let mut path = std::env::temp_dir();
            path.push(test_name);
            fs::create_dir(&path)?;
            DB::setup_db(&path, false)
        }
    }

    fn into_atom<'a>(
        name: &'a str,
        scm: &'a [u8]
    ) -> impl Fn((Vec<u8>, Vec<u8>)) -> (u32, Atom) + use<'a> {
        let inner = |(raw_key, raw_val): (Vec<u8>, Vec<u8>)| {
            let (tuple_id, _) = raw_key.decode_u32();
            let tagged_vals =
                TaggedVal::decode_from_scm(
                    &raw_val,
                    scm,
                    0..scm.len().try_into().unwrap(),
                );
            let tuple_atom = Term::Functor(
                name.into(),
                tagged_vals.into_iter().map(Term::from).collect(),
                false,
            );

            (tuple_id, tuple_atom)
        };

        inner
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
            tx.declare(foo_predicate.0, foo_predicate.1.to_vec(), vec![], vec![])?;

            // DECL was set?
            let key = build_predicate_key(foo_predicate.0, foo_predicate.2);
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key)?
                .unwrap();
            assert_eq!(&[0, 0], &prefix[..]);

            // RVLK was set?
            let name_bytes =
                tx
                .tx
                .get_cf(tx.rvlk, &prefix)?
                .unwrap();
            assert_eq!(
                foo_predicate.0,
                String::from_utf8(name_bytes).unwrap(),
            );

            // SCM was set?
            let raw_spec =
                tx
                .tx
                .get_cf(tx.scm, &prefix)?
                .unwrap();
            let atom_spec = AtomSpec::decode(&raw_spec)?;
            assert_eq!(atom_spec.schema, foo_predicate.1);

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            assert_eq!(&[0, 1], &next_prefix[..]);
        }

        // re-declare with same scm
        {
            tx
            .declare(foo_predicate.0, foo_predicate.1.to_vec(), vec![], vec![])?;
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
                .declare(foo_predicate.0, tup_scm_different, vec![], vec![])
                .unwrap_err();
            assert!(err.to_string().contains("does not match"));
        }

        // declare using existing name defined as a rule
        {
            let key: Vec<u8> = build_predicate_key("bar", foo_predicate.2);
            let mut prefix = vec![];
            format::FIRST_RULE_SEQ.encode_append(&mut prefix);

            // simulate have a rule named "bar/3"
            tx
            .tx
            .put_cf(tx.decl, &key, prefix)?;

            let err =
                tx
                .declare("bar", foo_predicate.1.to_vec(), vec![], vec![])
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
            assert_eq!(&[0, 1], &next_prefix[..]);
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
                foo_predicate.1.to_vec(),
            )?;

            // DECL was set?
            let key = build_predicate_key(foo_predicate.0, foo_predicate.2);
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, &key)?
                .unwrap();
            assert_eq!(format::FIRST_RULE_SEQ.to_be_bytes(), &prefix[..]);

            // RVLK was set?
            let raw_name =
                tx
                .tx
                .get_cf(tx.rvlk, &prefix)?
                .unwrap();
            assert_eq!(
                foo_predicate.0,
                String::from_utf8(raw_name).unwrap(),
            );

            // SCM was set?
            let raw_spec =
                tx
                .tx
                .get_cf(tx.scm, &prefix)?
                .unwrap();
            let atom_spec = AtomSpec::decode(&raw_spec)?;
            assert_eq!(atom_spec.schema, foo_predicate.1);

            // DEF was set?
            let rules =
                tx
                .tx
                .get_cf(tx.def, &prefix)?
                .unwrap();
            assert_eq!(
                foo_predicate.3,
                String::from_utf8(rules).unwrap(),
            );

            // next prefix was updated?
            let next_prefix =
                tx
                .tx
                .get_cf(tx.meta, &[format::NEXT_PREFIX_SEQ])?
                .unwrap();
            assert_eq!(&[0, 1], &next_prefix[..]);

            // iterate all rules?
            let rules = tx.rules().collect::<HashSet<_>>();
            assert_eq!(HashSet::from([foo_predicate.3.to_string()]), rules);
        }

        // define subsequent rule
        {
            tx.define("succeed", "succeed :- true.", b"".to_vec())?;

            // prefix (for rule) set correctly?
            let key = build_predicate_key("succeed", 0);
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, &key)?
                .unwrap();
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
                foo_predicate.3.to_string(),
                "succeed :- true.".to_string(),
            ]), rules);
        }

        // overwrite rule with re-definition
        {
            let new_rule = "foo(X, Y) :- baz(X, Y).";
            tx.define(foo_predicate.0, new_rule, foo_predicate.1.to_vec())?;

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
                .define(foo_predicate.0, foo_predicate.3, rule_scm1.to_vec())
                .unwrap_err();
            assert!(err.to_string().contains("does not match"));
        }

        // declare using existing name defined as a fact
        {
            let key = build_predicate_key("bar", foo_predicate.2);
            let mut prefix = vec![];
            0u16.encode_append(&mut prefix);

            // simulate have a fact named "bar/2"
            tx
            .tx
            .put_cf(tx.decl, &key, &prefix)?;

            let err =
                tx
                .define("bar", foo_predicate.3, foo_predicate.1.to_vec())
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
            assert_eq!(&[0, 2], &next_prefix[..]);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_assert_and_query() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_assert_and_query").unwrap();
        let tx = db.begin_transaction();

        let foo_atom = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );
        let bar_atom = (
            "bar",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare facts used
        {
            tx.declare(foo_atom.0, foo_atom.2.to_vec(), vec![], vec![])?;
            tx.declare(bar_atom.0, bar_atom.2.to_vec(), vec![], vec![])?;
        }

        let foo_prefix = tx.get_prefix(foo_atom.0, foo_atom.1)?.unwrap();
        let bar_prefix = tx.get_prefix(bar_atom.0, bar_atom.1)?.unwrap();

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let tuple = &[
                    TaggedVal::Atom("apple".into()),
                    TaggedVal::Int(i),
                ];

                assert_eq!(tx.assert(
                    foo_atom.0,
                    foo_atom.1,
                    tuple,
                )?, i as u32);
            }
        }

        // insert a fact for bar
        {
            let tuple = &[
                TaggedVal::Atom("apple".into()),
                TaggedVal::Int(42),
            ];
            assert_eq!(tx.assert(
                bar_atom.0,
                bar_atom.1,
                tuple,
            )?, k as u32);
        }

        // query all foo(X, Y)?
        {
            let result =
                tx
                .query(foo_prefix.clone())?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(2)],
                    false,
                )),
            ]), result);
        }

        // query all bar(X, Y)?
        {
            let result =
                tx
                .query(bar_prefix)?
                .map(into_atom(bar_atom.0, bar_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (3, Term::Functor(
                    "bar".into(),
                    vec![Term::Atom("apple".into()), Term::Int(42)],
                    false,
                )),
            ]), result);
        }

        // query by specific id
        {
            let mut prefix = foo_prefix;
            1u32.encode_append(&mut prefix);
            let result =
                tx
                .query(prefix)?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (1, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(1)],
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

            assert_eq!(result.len(), k as usize + 1);
        }


        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_retract() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_retract").unwrap();
        let tx = db.begin_transaction();

        let foo_atom = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare fact
        {
            tx.declare(foo_atom.0, foo_atom.2.to_vec(), vec![], vec![])?;
        }

        // insert k facts about foo
        let k = 3;
        {
            for i in 0..k {
                let tuple = &[
                    TaggedVal::Atom("apple".into()),
                    TaggedVal::Int(i as i32),
                ];

                assert_eq!(tx.assert(
                    foo_atom.0,
                    foo_atom.1,
                    tuple,
                )?, i);
            }
        }

        // query all foo(X, Y)?
        {
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(2)],
                    false,
                )),
            ]), result);
        }

        // retract some facts
        {
            tx.retract(foo_atom.0, foo_atom.1, 1)?;
            tx.retract(foo_atom.0, foo_atom.1, 100)?;
            tx.retract(foo_atom.0, foo_atom.1 + 1, 1)?;
        }

        // query all foo(X, Y)? again
        {
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(HashSet::from([
                (0, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(0)],
                    false,
                )),
                (2, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(2)],
                    false,
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

        let foo_atom = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // declare fact
        {
            tx.declare(foo_atom.0, foo_atom.2.to_vec(), vec![], vec![])?;
        }

        {
            // DECL was set?
            let key = build_predicate_key(foo_atom.0, foo_atom.1);
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key)?
                .unwrap();
            assert_eq!(&[0, 0], &prefix[..]);
        }

        // forgot fact
        {
            tx.forget(foo_atom.0, foo_atom.1)?;
        }

        {
            // DECL was unset?
            let key = build_predicate_key(foo_atom.0, foo_atom.1);
            let prefix =
                tx
                .tx
                .get_cf(tx.decl, key)?;
            assert_eq!(prefix, None);
        }

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn it_can_abort_tx() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_abort_tx").unwrap();

        let foo_atom = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
        );

        // set intial db state
        {
            let tx = db.begin_transaction();
            tx.declare(foo_atom.0, foo_atom.2.to_vec(), vec![], vec![])?;

            for i in 0..3 {
                let tuple = &[
                    TaggedVal::Atom("apple".into()),
                    TaggedVal::Int(i as i32),
                ];
                assert_eq!(tx.assert(
                    foo_atom.0, foo_atom.1, tuple,
                )?, i);
            }

            // query all foo(X, Y)?
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(2)],
                    false,
                )),
            ]));

            tx.commit()?;
        }

        // change state, but rollback
        {
            let tx = db.begin_transaction();

            tx.retract(foo_atom.0, foo_atom.1, 0)?;
            tx.retract(foo_atom.0, foo_atom.1, 1)?;
            tx.retract(foo_atom.0, foo_atom.1, 2)?;

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
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();

            assert_eq!(result, HashSet::from([
                (0, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(0)],
                    false,
                )),
                (1, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(1)],
                    false,
                )),
                (2, Term::Functor(
                    "foo".into(),
                    vec![Term::Atom("apple".into()), Term::Int(2)],
                    false,
                )),
            ]));

            tx.commit()?;
        }

        Ok(())
    }

    #[test]
    fn it_can_assert_and_project_indexes() -> Result<(), anyhow::Error> {
        let db = DB::new_temp_for_testing("it_can_assert_and_project_indexes").unwrap();
        let tx = db.begin_transaction();

        let foo_atom = (
            "foo",
            2,
            &[format::ATOM_TYPE, format::INT_TYPE],
            vec![vec![0], vec![1, 0]],
        );

        // declare facts used
        {
            tx.declare(
                foo_atom.0,
                foo_atom.2.to_vec(),
                vec![],
                foo_atom.3
            )?;
        }

        // insert k facts about foo
        let k = 10;
        {
            for i in 0..k {
                let tuple = &[
                    TaggedVal::Atom("apple".into()),
                    TaggedVal::Int(i),
                ];

                assert_eq!(tx.assert(
                    foo_atom.0,
                    foo_atom.1,
                    tuple,
                )?, i as u32);
            }
        }

        // query all tuples
        {
            let result =
                tx
                .query(b"".to_vec())?
                .map(into_atom(foo_atom.0, foo_atom.2))
                .collect::<HashSet<_>>();


            assert_eq!(HashSet::from_iter(
                (0..k)
                .map(|i| {
                    (
                        i as u32,
                        Term::Functor(
                            foo_atom.0.into(),
                            vec![Term::Atom("apple".into()), Term::Int(i)],
                            false,
                        )
                    )
                })
            ), result);
        }

        // lookup index projections
        {
            let result =
                tx
                .index(b"".to_vec())?
                .collect::<Vec<_>>();
            assert_eq!(
                vec![
                    vec![0, 0, 0, 0],
                    vec![0, 0, 0, 1],
                    vec![0, 0, 0, 2],
                    vec![0, 0, 0, 3],
                    vec![0, 0, 0, 4],
                    vec![0, 0, 0, 5],
                    vec![0, 0, 0, 6],
                    vec![0, 0, 0, 7],
                    vec![0, 0, 0, 8],
                    vec![0, 0, 0, 9],
                    vec![0, 0, 0, 0],
                    vec![0, 0, 0, 1],
                    vec![0, 0, 0, 2],
                    vec![0, 0, 0, 3],
                    vec![0, 0, 0, 4],
                    vec![0, 0, 0, 5],
                    vec![0, 0, 0, 6],
                    vec![0, 0, 0, 7],
                    vec![0, 0, 0, 8],
                    vec![0, 0, 0, 9]
                ],
                result
            );
        }

        Ok(())
    }
}
