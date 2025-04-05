use std::rc::Rc;
use std::cell::RefCell;
use std::path::Path;

use crate::storage::engine::btree::format::tuple::Tuple;
use crate::storage::engine::btree::BPlusTree;
use crate::storage::engine::btree::BPlusTreeBuilder;
use crate::storage::engine::btree::pager::Pager;
use crate::storage::engine::btree::pager::PagerBuilder;
use crate::storage::engine::btree::format::CatalogOps;
use crate::storage::engine::btree::format::page::LeafOps;
use crate::storage::engine::btree::format::page::TupVal;
use crate::storage::engine::btree::format::sizedbuf::SizedBuf;
use crate::storage::engine::btree::format::UINT_TYPE;
use crate::storage::engine::btree::format::tuple::ParameterType;
use crate::storage::engine::btree::BPlusTreeIntoIter;
use crate::errors;

pub struct DB {
    pager: Rc<RefCell<Pager>>,
}

pub struct Predicate {
    name: String,
    btree: BPlusTree,
}

pub struct PredicateIterator {
    btree_iter: BPlusTreeIntoIter,
}

impl PredicateIterator {
    pub fn new(btree_iter: BPlusTreeIntoIter) -> Self {
        PredicateIterator { btree_iter }
    }
}

impl DB {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        let pager =
            PagerBuilder::new(file_path)
            .build();

        // TODO: fix
        // reserve dummy page for first page
        pager.fetch(0);

        DB {
            pager: Rc::new(RefCell::new(pager)),
        }
    }

    pub fn create_predicate(&mut self, name: &str, key_scm: &[u8], tup_scm: &[u8]) -> Predicate {
        let root_page_num = {
            let mut pager = self.pager.borrow_mut();
            let root_page_num = pager.get_fresh_page_num();
            let root_cell = pager.fetch(root_page_num);
            let mut root = root_cell.borrow_mut();
            root.leaf_initialize_node(true, key_scm, tup_scm);
            pager.catalog.define_predicate(root_page_num, name);
            root_page_num
        };
        let btree =
            BPlusTreeBuilder::new(Rc::clone(&self.pager), root_page_num)
            .split_strategy_empty_page()
            .build();
        Predicate::new(name.to_string(), btree)
    }

    pub fn get_predicate(&mut self, name: &str) -> Predicate {
        let root_page_num = {
            let pager = self.pager.borrow();
            pager.catalog.get_predicate_root_page_num(name)
        };
        let btree =
            BPlusTreeBuilder::new(Rc::clone(&self.pager), root_page_num)
            .split_strategy_empty_page()
            .build();
        Predicate::new(name.to_string(), btree)
    }

    pub fn save(&self) {
        let pager = self.pager.borrow();
        pager.flush_all_pages();
    }
}

impl Predicate {
    pub fn new(name: String, btree: BPlusTree) -> Self {
        Predicate { name, btree }
    }

    pub fn iter_owned(&self) -> PredicateIterator {
        PredicateIterator::new(self.btree.clone().into_iter())
    }

    //TODO: check types in btree.insert
    pub fn assert(&self, input: &TupVal) -> Result<(), errors::RecallError> {
        let key = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            let pager = self.btree.pager.borrow();
            let key = pager.catalog.get_predicate_last_tuple_id(&self.name);
            tuple.write_u32_offset(0, key);
            tuple.decode(&[UINT_TYPE])
        };
        self.btree.insert(&key, input)?;
        let mut pager = self.btree.pager.borrow_mut();
        pager.catalog.inc_predicate_last_tuple_id(&self.name);
        Ok(())
    }

    // pub fn retract(&self, data: &[ParameterType]) -> Result<(), errors::RecallError> {
    //     todo!()
    // }
}

impl Iterator for PredicateIterator {
    type Item = Vec<ParameterType>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(tuple) = self.btree_iter.next() {
            Some(tuple)
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

    use crate::storage::engine::btree::format::ATOM_TYPE;
    use crate::storage::engine::btree::format::STRING_TYPE;
    use crate::storage::engine::btree::format::UINT_TYPE;
    use crate::storage::engine::btree::format::INT_TYPE;

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
    fn it_can_create_predicates_and_use_them() {
        let fixture = Fixture::new("it_can_create_predicates_and_use_them.db");
        let mut db = DB::new(&fixture.temp_path);

        let key_scm = &[UINT_TYPE];

        let pred = db.create_predicate("foo", key_scm, &vec![ATOM_TYPE, INT_TYPE]);
        pred.assert(&vec![
            ParameterType::Atom("apple".to_string()),
            ParameterType::Int(42),
        ]).unwrap();

        pred.assert(&vec![
            ParameterType::Atom("lemon".to_string()),
            ParameterType::Int(1337),
        ]).unwrap();

        let pred = db.create_predicate("bar", key_scm, &vec![STRING_TYPE, INT_TYPE, UINT_TYPE]);
        pred.assert(&vec![
            ParameterType::String("cake".to_string()),
            ParameterType::Int(2001),
            ParameterType::UInt(99),
        ]).unwrap();

        pred.assert(&vec![
            ParameterType::String("crepe".to_string()),
            ParameterType::Int(99),
            ParameterType::UInt(666),
        ]).unwrap();

        let pred = db.get_predicate("foo");
        let got: Vec<Vec<ParameterType>> = pred.iter_owned().collect();
        let want = vec![
            vec![ParameterType::Atom("apple".to_string()), ParameterType::Int(42)],
            vec![ParameterType::Atom("lemon".to_string()), ParameterType::Int(1337)],
        ];
        assert_eq!(got, want);

        let pred = db.get_predicate("bar");
        let got: Vec<Vec<ParameterType>> = pred.iter_owned().collect();
        let want = vec![
            vec![ParameterType::String("cake".to_string()), ParameterType::Int(2001), ParameterType::UInt(99)],
            vec![ParameterType::String("crepe".to_string()), ParameterType::Int(99), ParameterType::UInt(666)],
        ];
        assert_eq!(got, want);
    }
}
