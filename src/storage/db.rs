use std::rc::Rc;
use std::cell::RefCell;
use std::path::Path;

use crate::storage::engine::btree::BPlusTree;
use crate::storage::engine::btree::BPlusTreeBuilder;
use crate::storage::engine::btree::pager::Pager;
use crate::storage::engine::btree::pager::PagerBuilder;
use crate::storage::engine::btree::format;
use crate::storage::engine::btree::format::page::LeafOps;
use crate::storage::engine::btree::format::page::TupVal;
use crate::storage::engine::btree::format::tuple::ParameterType;
use crate::storage::engine::btree::format::CATALOG_ROOT_PAGE_NUM;
use crate::storage::engine::btree::BPlusTreeIntoIter;
use crate::errors;

use super::engine::btree::format::PREDICATES_KEY;

pub struct DB {
    pager: Rc<RefCell<Pager>>,
}

pub struct Predicate {
    pub name: String,
    pub arity: u32,
    predicates: BPlusTree,
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
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self, errors::RecallError> {
        let pager =
            PagerBuilder::new(file_path)
            .build();
        let stored_pages = pager.stored_pages;
        let db = DB { pager: Rc::new(RefCell::new(pager)) };

        if stored_pages != 0 {
            return Ok(db);
        }

        // bootstrap file format

        // initialize root page
        {
            let pager = db.pager.borrow_mut();

            let root_cell = pager.fetch(CATALOG_ROOT_PAGE_NUM);
            let mut root = root_cell.borrow_mut();
            root.leaf_initialize_node(
                true,
                &[format::UINT_TYPE], // predicates key const
                &[format::UINT_TYPE], // root page num
            );
        }

        // insert predicates object type into catalog
        let predicates_root_page_num = {
            let btree =
                BPlusTreeBuilder::new(Rc::clone(&db.pager), CATALOG_ROOT_PAGE_NUM)
                .split_strategy_empty_page()
                .build();
            let root_page_num = {
                let mut pager = btree.pager.borrow_mut();
                pager.get_fresh_page_num()
            };
            let key = vec!{
                ParameterType::UInt(PREDICATES_KEY)
            };
            let val = vec!{
                ParameterType::UInt(root_page_num)
            };
            btree.insert(&key, &val)?;
            root_page_num
        };

        // initialize predicates predicate object page
        {
            let pager = db.pager.borrow_mut();

            let root_cell = pager.fetch(predicates_root_page_num);
            let mut root = root_cell.borrow_mut();
            root.leaf_initialize_node(
                true,
                &[
                    format::ATOM_TYPE,  // Name
                    format::UINT_TYPE,  // Arity
                ],
                &[
                    format::BYTES_TYPE, // KeyScm
                    format::BYTES_TYPE, // TupScm
                    format::UINT_TYPE,  // RootPageNum
                    format::UINT_TYPE,  // NextKey
                    format::ATOM_TYPE,  // User
                ],
            );
        }

        Ok(db)
    }

    pub fn get_predicates_object_btree(&self) -> Result<BPlusTree, errors::RecallError> {
        let root_page_num = {
            let btree =
                BPlusTreeBuilder::new(Rc::clone(&self.pager), CATALOG_ROOT_PAGE_NUM)
                .split_strategy_empty_page()
                .build();
            let key = vec!{
                ParameterType::UInt(format::PREDICATES_KEY)
            };
            let tup =
                btree
                .find(&key)?
                .expect("expected tuple found by predicates key");
            tup[0].get_uint()
        };

        Ok(
            BPlusTreeBuilder::new(Rc::clone(&self.pager), root_page_num)
            .split_strategy_half_full_page()
            .build()
        )
    }

    pub fn define_predicate(&mut self, name: &str, tup_scm: Vec<u8>) -> Result<Predicate, errors::RecallError> {
        let key_scm = vec![format::UINT_TYPE];
        let predicates = self.get_predicates_object_btree()?;
        let arity = u32::try_from(tup_scm.len()).unwrap();

        match self.get_predicate(name, arity) {
            Ok(predicate) => {
                predicate.btree.typecheck(&tup_scm)?;
                Ok(predicate)
            },
            Err(errors::RecallError::PredicateNotFound(..)) => {
                let root_page_num =
                    predicates
                    .pager
                    .borrow_mut()
                    .get_fresh_page_num();

                // initialize new predicate root page
                {
                    let pager = self.pager.borrow_mut();
                    let root_cell = pager.fetch(root_page_num);
                    let mut root = root_cell.borrow_mut();
                    root.leaf_initialize_node(
                        true,
                        &key_scm,
                        &tup_scm,
                    );
                }

                // insert new predicate
                {
                    let key = vec!{
                        ParameterType::Atom(name.to_string()),
                        ParameterType::UInt(arity),
                    };
                    let tup = vec!{
                        ParameterType::Bytes(key_scm),              // KeyScm
                        ParameterType::Bytes(tup_scm),              // TupScm
                        ParameterType::UInt(root_page_num),         // RootPageNum
                        ParameterType::UInt(0),                     // NextKey
                        ParameterType::Atom("user".to_string()),    // User
                    };
                    predicates.insert(&key, &tup)?;
                }

                let btree =
                    BPlusTreeBuilder::new(Rc::clone(&self.pager), root_page_num)
                    .split_strategy_empty_page()
                    .build();

                Ok(Predicate::new(name.to_string(), arity, predicates, btree))
            },
            Err(err) => Err(err),
        }
    }

    pub fn get_predicate(&mut self, name: &str, arity: u32) -> Result<Predicate, errors::RecallError> {
        let predicates = self.get_predicates_object_btree()?;
        let key = vec!{
            ParameterType::Atom(name.to_string()),
            ParameterType::UInt(arity),
        };

        predicates
        .find(&key)?
        .map_or_else(
            || {
                Err(errors::RecallError::PredicateNotFound(name.to_string(), arity))
            },
            |tup| {
                let root_page_num = *&tup[2].get_uint();
                let btree =
                    BPlusTreeBuilder::new(Rc::clone(&self.pager), root_page_num)
                    .split_strategy_empty_page()
                    .build();
                Ok(Predicate::new(name.to_string(), arity, predicates, btree))
            }
        )
    }

    pub fn save(&self) {
        let pager = self.pager.borrow();
        pager.flush_all_pages();
    }
}

impl Predicate {
    pub fn new(name: String, arity: u32, predicates: BPlusTree, btree: BPlusTree) -> Self {
        Predicate { name, arity, predicates, btree }
    }

    pub fn iter_owned(&self) -> PredicateIterator {
        PredicateIterator::new(self.btree.clone().into_iter())
    }

    pub fn assert(&self, val: &TupVal) -> Result<(), errors::RecallError> {
        let pred_key = vec!{
            ParameterType::Atom(self.name.to_string()),
            ParameterType::UInt(self.arity),
        };

        self
        .predicates
        .find(&pred_key)?
        .map_or_else(
            || {
                Err(errors::RecallError::PredicateNotFound(self.name.to_string(), self.arity))
            },
            |mut pred_tup| {
                let next_key = pred_tup[3].get_uint();

                // insert tuple
                let gen_key = vec!{
                    ParameterType::UInt(next_key)
                };
                self
                .btree
                .insert(&gen_key, val)?;

                // increment next key
                pred_tup[3] = ParameterType::UInt(next_key + 1);
                self
                .predicates
                .replace(&pred_key, &pred_tup)?;

                Ok(())
            }
        )
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
    fn it_can_define_predicates_and_use_them() {
        let fixture = Fixture::new("it_can_define_predicates_and_use_them.db");
        let mut db =
            DB::new(&fixture.temp_path)
            .unwrap();

        let catalog_objects =
            BPlusTreeBuilder::new(Rc::clone(&db.pager), CATALOG_ROOT_PAGE_NUM)
            .split_strategy_empty_page()
            .build()
            .into_iter()
            .collect::<Vec<Vec<ParameterType>>>();
        assert_eq!(catalog_objects.len(), 1);

        {
            let pred_foo =
                db
                .define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE])
                .unwrap();
            let pred_bar =
                db
                .define_predicate("bar", vec![format::STRING_TYPE, format::INT_TYPE, format::UINT_TYPE])
                .unwrap();

            let predicates =
                db
                .get_predicates_object_btree()
                .unwrap()
                .into_iter()
                .collect::<Vec<Vec<ParameterType>>>();
            assert_eq!(predicates.len(), 2);

            pred_foo.assert(&vec![
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ]).unwrap();
            pred_foo.assert(&vec![
                ParameterType::Atom("apple".to_string()),
                ParameterType::Int(42),
            ]).unwrap();
            pred_foo.assert(&vec![
                ParameterType::Atom("lemon".to_string()),
                ParameterType::Int(1337),
            ]).unwrap();

            pred_bar.assert(&vec![
                ParameterType::String("cake".to_string()),
                ParameterType::Int(2001),
                ParameterType::UInt(99),
            ]).unwrap();
            pred_bar.assert(&vec![
                ParameterType::String("crepe".to_string()),
                ParameterType::Int(99),
                ParameterType::UInt(666),
            ]).unwrap();
        }

        let pred =
            db
            .get_predicate("foo", 2)
            .unwrap();
        let got: Vec<Vec<ParameterType>> = pred.iter_owned().collect();
        let want = vec![
            vec![ParameterType::Atom("apple".to_string()), ParameterType::Int(42)],
            vec![ParameterType::Atom("apple".to_string()), ParameterType::Int(42)],
            vec![ParameterType::Atom("lemon".to_string()), ParameterType::Int(1337)],
        ];
        assert_eq!(got, want);

        let pred =
            db
            .get_predicate("bar", 3)
            .unwrap();
        let got: Vec<Vec<ParameterType>> = pred.iter_owned().collect();
        let want = vec![
            vec![ParameterType::String("cake".to_string()), ParameterType::Int(2001), ParameterType::UInt(99)],
            vec![ParameterType::String("crepe".to_string()), ParameterType::Int(99), ParameterType::UInt(666)],
        ];
        assert_eq!(got, want);

        db.save();

        let mut db =
            DB::new(&fixture.temp_path)
            .unwrap();
        let pred =
            db
            .get_predicate("foo", 2)
            .unwrap();

        let got: Vec<Vec<ParameterType>> = pred.iter_owned().collect();
        let want = vec![
            vec![ParameterType::Atom("apple".to_string()), ParameterType::Int(42)],
            vec![ParameterType::Atom("apple".to_string()), ParameterType::Int(42)],
            vec![ParameterType::Atom("lemon".to_string()), ParameterType::Int(1337)],
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn it_can_handle_duplicate_definitions() {
        let fixture = Fixture::new("it_can_handle_duplicate_definitions.db");
        let mut db =
            DB::new(&fixture.temp_path)
            .unwrap();

        assert!(db.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE]).is_ok());
        assert!(db.define_predicate("foo", vec![format::ATOM_TYPE, format::INT_TYPE]).is_ok());
        assert!(db.define_predicate("foo", vec![format::ATOM_TYPE, format::STRING_TYPE]).is_err());
    }
}
