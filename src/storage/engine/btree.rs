pub mod format;
pub mod pager;
mod cursor;

use std::rc::Rc;
use std::cell::RefCell;

use crate::errors;
use cursor::BTreeCursor;
use format::tuple::ParameterType;
use pager::Pager;
use format::page::PageHeader;
use format::page::InternalOps;
use format::page::LeafOps;
use format::page::SlottedPage;
use format::page::KeyVal;
use format::page::TupVal;
use format::page::SplitStrategy;

pub struct BPlusTreeBuilder {
    pager: Rc<RefCell<Pager>>,
    root_page_num: u32,
    split_strategy: SplitStrategy,
}

impl BPlusTreeBuilder {
    pub fn new(pager: Rc<RefCell<Pager>>, root_page_num: u32) -> Self {
        BPlusTreeBuilder { pager, root_page_num, split_strategy: SplitStrategy::HalfFullPage }
    }

    pub fn split_strategy_half_full_page(mut self) -> Self {
        self.split_strategy = SplitStrategy::HalfFullPage;
        self
    }

    pub fn split_strategy_empty_page(mut self) -> Self {
        self.split_strategy = SplitStrategy::EmptyPage;
        self
    }

    pub fn build(self) -> BPlusTree {
        BPlusTree::new(self.pager, self.root_page_num, self.split_strategy)
    }
}

#[derive(Clone)]
pub struct BPlusTree {
    pub pager: Rc<RefCell<Pager>>,
    root_page_num: u32,
    split_strategy: SplitStrategy,
}

impl BPlusTree {
    fn new(pager: Rc<RefCell<Pager>>, root_page_num: u32, split_strategy: SplitStrategy) -> Self {
        BPlusTree { pager, root_page_num, split_strategy}
    }
}

impl BPlusTree {
    /// Inserts a key-value pair into the tree.
    /// Splits nodes if needed and updates internal pointers.
    pub fn insert(&self, key: &KeyVal, val: &TupVal) -> Result<(), errors::RecallError> {
        let mut pager = self.pager.borrow_mut();
        let cursor = cursor::BTreeCursor::find(&mut pager, self.root_page_num, key);
        leaf_node_insert(&mut pager, cursor.page_num, cursor.tuple_index, key, val, self.split_strategy)
    }

    /// Removes the key from the tree.
    /// Handles underflows by merging or redistributing keys.
    // pub fn delete(&mut self, key: u32) -> Result<(), errors::RecallError> {
    //     todo!()
    // }

    /// Performs a binary search within each node to find the key.
    /// Always reaches the leaf node (unlike a B-tree, where it may stop earlier).
    pub fn find(&self, key: &KeyVal) -> Result<Option<TupVal>, errors::RecallError> {
        let mut pager = self.pager.borrow_mut();
        let cursor = cursor::BTreeCursor::find(&mut pager, self.root_page_num, key);
        let node_cell = &mut pager.fetch(cursor.page_num);
        let node = node_cell.borrow();
        let (key_scm, _) = node.leaf_key_and_tup_scm();
        ParameterType::typecheck(key, &key_scm)?;
        if cursor.tuple_index == node.num_tuples() {
            Ok(None)
        } else {
            let stored_key = node.leaf_get_key(cursor.tuple_index);
            let val = node.leaf_get_val(cursor.tuple_index);
            if stored_key == *key {
                Ok(Some(val))
            } else {
                Ok(None)
            }
        }
    }

    /// range matching [start_key, stop_key)
    pub fn range_scan(self, start_key: &KeyVal, stop_key: &KeyVal) -> Result<BPlusTreeIntoIter, errors::RecallError> {
        let (front, back) = {
            let mut pager = self.pager.borrow_mut();
            let front = cursor::BTreeCursor::find(&mut pager, self.root_page_num, start_key);
            let back = cursor::BTreeCursor::find(&mut pager, self.root_page_num, stop_key);
            (front, back)
        };
        {
            let pager = self.pager.borrow_mut();
            let node_cell = &mut pager.fetch(front.page_num);
            let node = node_cell.borrow();
            let (key_scm, _) = node.leaf_key_and_tup_scm();
            ParameterType::typecheck(start_key, &key_scm)?;
            ParameterType::typecheck(stop_key, &key_scm)?;
        }
        Ok(BPlusTreeIntoIter::new(self, front, back))
    }
}

pub struct BPlusTreeIntoIter {
    btree: BPlusTree,
    cursors: Option<(BTreeCursor, BTreeCursor)>,
}

impl BPlusTreeIntoIter {
    pub fn new(
        btree: BPlusTree,
        front: BTreeCursor,
        back: BTreeCursor,
    ) -> Self {
        let front_option = {
            let front_node_num_entries = {
                let pager = btree.pager.borrow_mut();
                let front_node_cell = pager.fetch(front.page_num);
                let front_node = front_node_cell.borrow();
                front_node.num_tuples()
            };

            if front.tuple_index == front_node_num_entries {
                Self::step_forward(&btree, &front)
            } else {
                Some(front)
            }
        };

        let back_option = Self::step_backward(&btree, &back);

        match (front_option, back_option) {
            (Some(front), Some(back)) => {
                if Self::cursors_crossed(&btree, &front, &back) {
                    BPlusTreeIntoIter {
                        btree,
                        cursors: None,
                    }
                } else {
                    BPlusTreeIntoIter {
                        btree,
                        cursors: Some((front, back)),
                    }
                }
            }
            _ => {
                BPlusTreeIntoIter {
                    btree,
                    cursors: None,
                }
            }
        }
    }

    fn cursors_crossed(btree: &BPlusTree, front: &BTreeCursor, back: &BTreeCursor) -> bool {
        let pager = btree.pager.borrow_mut();
        let front_node_cell = pager.fetch(front.page_num);
        let front_node = front_node_cell.borrow();
        let front_key = front_node.leaf_get_key(front.tuple_index);

        let back_node_cell = pager.fetch(back.page_num);
        let back_node = back_node_cell.borrow();
        let back_key = back_node.leaf_get_key(back.tuple_index);

        front_key > back_key
    }

    fn step_forward(btree: &BPlusTree, cursor: &BTreeCursor) -> Option<BTreeCursor> {
        let pager = btree.pager.borrow_mut();
        let node_cell = pager.fetch(cursor.page_num);
        let node = node_cell.borrow();
        let num_tuples = node.num_tuples();

        if cursor.tuple_index >= num_tuples - 1 {
            let r_sibling = node.r_sibling();
            if r_sibling == format::NULL_PTR as u32 {
                None
            } else {
                Some(BTreeCursor {
                    page_num: r_sibling,
                    tuple_index: 0,
                })
            }
        } else {
            Some(BTreeCursor {
                page_num: cursor.page_num,
                tuple_index: cursor.tuple_index + 1,
            })
        }
    }

    fn step_backward(btree: &BPlusTree, cursor: &BTreeCursor) -> Option<BTreeCursor> {
        let pager = btree.pager.borrow_mut();
        let node_cell = pager.fetch(cursor.page_num);
        let node = node_cell.borrow();

        if cursor.tuple_index == 0 {
            let l_sibling = node.l_sibling();
            if l_sibling == format::NULL_PTR as u32 {
                None
            } else {
                let node_cell = pager.fetch(l_sibling);
                let node = node_cell.borrow();
                let num_tuples = node.num_tuples();

                Some(BTreeCursor {
                    page_num: l_sibling,
                    tuple_index: num_tuples - 1,
                })
            }
        } else {
            Some(BTreeCursor {
                page_num: cursor.page_num,
                tuple_index: cursor.tuple_index - 1,
            })
        }
    }
}

impl Iterator for BPlusTreeIntoIter {
    type Item = TupVal;

    fn next(&mut self) -> Option<Self::Item> {
        self
        .cursors
        .take()
        .map(|(front, back)| {
            let val = {
                let pager = self.btree.pager.borrow_mut();
                let node_cell = pager.fetch(front.page_num);
                let node = node_cell.borrow();
                let val= node.leaf_get_val(front.tuple_index);
                val
            };

            self.cursors =
                Self::step_forward(&self.btree, &front)
                .and_then(|new_front| {
                    if Self::cursors_crossed(&self.btree, &new_front, &back) {
                        None
                    } else {
                        Some((new_front, back))
                    }
                });

            val
        })
    }
}

impl<'a> DoubleEndedIterator for BPlusTreeIntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self
        .cursors
        .take()
        .map(|(front, back)| {
            let val = {
                let pager = self.btree.pager.borrow_mut();
                let node_cell = pager.fetch(back.page_num);
                let node = node_cell.borrow();
                let val = node.leaf_get_val(back.tuple_index);
                val
            };

            self.cursors =
                Self::step_backward(&self.btree, &back)
                .and_then(|new_back| {
                    if Self::cursors_crossed(&self.btree, &front, &new_back) {
                        None
                    } else {
                        Some((front, new_back))
                    }
                });

            val
        })
    }
}

impl IntoIterator for BPlusTree {
    type Item = TupVal;

    type IntoIter = BPlusTreeIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let (front, back) = {
            let mut pager = self.pager.borrow_mut();
            let front = BTreeCursor::begin(&mut pager, self.root_page_num);
            let back = BTreeCursor::end(&mut pager, self.root_page_num);
            (front, back)
        };
        BPlusTreeIntoIter::new(self, front, back)
    }
}

fn internal_node_insert(
    pager: &mut Pager,
    page_num: u32,
    tuple_index: usize,
    key: &KeyVal,
    val: u32,
    split_strategy: SplitStrategy,
) -> Result<(), errors::RecallError> {
    let inserted = {
        let node_cell = pager.fetch(page_num);
        let mut node = node_cell.borrow_mut();
        node.internal_put(&key, val, tuple_index)?
    };
    if inserted {
        Ok(())
    } else {
        split_internal_node_insert(pager, page_num, tuple_index, key, val, split_strategy)
    }
}

fn leaf_node_insert(
    pager: &mut Pager,
    page_num: u32,
    tuple_index: usize,
    key: &KeyVal,
    val: &TupVal,
    split_strategy: SplitStrategy,
) -> Result<(), errors::RecallError> {
    // The leaf put routine will return false when there is not enough space
    // for the new tuple. It will return err variant for things like key unique
    // error which will punt that to callers. An important invariant is that
    // a key unique error will be signaled even if the node is full since it
    // checks that condition first. So its important to attempt the leaf put
    // routine before attempting the leaf split routine.
    let inserted = {
        let node_cell = pager.fetch(page_num);
        let mut node = node_cell.borrow_mut();
        node.leaf_put(key, val, tuple_index)?
    };

    if inserted {
        Ok(())
    } else {
        split_leaf_node_insert(pager, page_num, tuple_index, key, val, split_strategy)
    }
}

fn update_parent_ptr_for_children(
    pager: &mut Pager,
    parent: &mut SlottedPage,
    parent_page_num: u32,
) {
    for i in 0..parent.num_tuples() {
        let child_page_num = parent.internal_get_val(i);
        let child_cell = pager.fetch(child_page_num);
        let mut child = child_cell.borrow_mut();
        child.set_parent_ptr(parent_page_num);
    }
    // also set for the right child
    let right_child_page_num = parent.right_child();
    let child_cell = pager.fetch(right_child_page_num);
    let mut child = child_cell.borrow_mut();
    child.set_parent_ptr(parent_page_num);
}

fn split_internal_node_insert(
    pager: &mut Pager,
    page_num: u32,
    tuple_index: usize,
    key: &KeyVal,
    val: u32,
    split_strategy: SplitStrategy,
) -> Result<(), errors::RecallError> {
    let is_root_node = {
        let right_node_cell = pager.fetch(page_num);
        let right_node = right_node_cell.borrow_mut();
        right_node.is_root_node()
    };

    // Handle the case were the internal node (we are splitting) is the root node.
    // In this case, we also need to create a new node as a root node.
    if is_root_node {
        let right_node_cell = pager.fetch(page_num);
        let right_node = right_node_cell.borrow_mut();
        // save some values from right node needed below
        let right_child_page_num = right_node.right_child();
        let key_scm = &right_node.internal_key_scm();

        // get a fresh node for the left child
        let left_node_page_num = pager.get_fresh_page_num();
        let left_node_cell = pager.fetch(left_node_page_num);
        let mut left_node = left_node_cell.borrow_mut();
        left_node.internal_initialize_node(false, key_scm);
        left_node.set_parent_ptr(page_num);

        // also, get a fresh node for the right child since the right node
        // will become the new root
        let new_right_node_page_num = pager.get_fresh_page_num();
        let new_right_node_cell = pager.fetch(new_right_node_page_num);
        let mut new_right_node = new_right_node_cell.borrow_mut();
        new_right_node.internal_initialize_node(false, key_scm);
        new_right_node.set_parent_ptr(page_num);
        new_right_node.set_right_child(right_child_page_num);

        // internal split routine
        let num = right_node.num_tuples();
        let pivot_index = num / 2usize;
        right_node.internal_split_into(
            pivot_index,
            &mut left_node,
            &mut new_right_node,
            tuple_index,
            key,
            val,
            split_strategy,
        )?;

        // move right node into new_root
        let mut new_root = right_node;
        new_root.internal_initialize_node(true, key_scm);
        new_root.set_right_child(new_right_node_page_num);

        // fix the right child ptr for the left node
        let new_parent_key = {
            let num = left_node.num_tuples();
            assert!(num > 1);
            let new_parent_key = left_node.internal_get_key(num - 1);
            let child_page_num = left_node.internal_get_val(num - 1);
            left_node.set_right_child(child_page_num);
            left_node.dec_num_tuples();
            new_parent_key
        };

        // TODO: more efficient way to do this ???
        update_parent_ptr_for_children(pager, &mut new_right_node, new_right_node_page_num);
        update_parent_ptr_for_children(pager, &mut left_node, left_node_page_num);

        // insert new node key/pair into parent
        let tuple_index = new_root.internal_find_tuple_index(&new_parent_key);
        assert!(new_root.internal_put(&new_parent_key, left_node_page_num, tuple_index)?);

        Ok(())
    } else {
        let left_node_page_num = pager.get_fresh_page_num();
        let (new_parent_key, parent_page_num, tuple_index) = {
            let right_node_cell = pager.fetch(page_num);
            let mut right_node = right_node_cell.borrow_mut();
            // save some values from right node needed below
            let parent_page_num = right_node.parent_ptr();
            let right_child_page_num = right_node.right_child();
            let key_scm = &right_node.internal_key_scm();

            // get a fresh node for the left child
            let left_node_cell = pager.fetch(left_node_page_num);
            let mut left_node = left_node_cell.borrow_mut();
            left_node.internal_initialize_node(false, key_scm);
            left_node.set_parent_ptr(parent_page_num);

            // split right node with left node (new node)
            let num = right_node.num_tuples();
            let pivot_index = num / 2usize;
            // TODO: pull items directly from right node and put into left node
            //       instead of creating a copy page ???
            {
                let mut copy = pager.get_empty_page();
                SlottedPage::copy(&right_node, &mut copy);
                // re-initialize right node
                right_node.internal_initialize_node(false, key_scm);
                right_node.set_right_child(right_child_page_num);
                copy.internal_split_into(
                    pivot_index,
                    &mut left_node,
                    &mut right_node,
                    tuple_index,
                    key,
                    val,
                    split_strategy,
                )?;
            }

            // fix the right child ptr for the left node
            let new_parent_key = {
                let num = left_node.num_tuples();
                assert!(num > 1);
                let new_parent_key = left_node.internal_get_key(num - 1);
                let child_page_num = left_node.internal_get_val(num - 1);
                left_node.set_right_child(child_page_num);
                left_node.dec_num_tuples();
                new_parent_key
            };

            // TODO: more efficient way to do this ???
            update_parent_ptr_for_children(pager, &mut left_node, left_node_page_num);

            // insert new node key/pair into parent
            let parent_node_cell = pager.fetch(parent_page_num);
            let parent_node = parent_node_cell.borrow();
            let tuple_index = parent_node.internal_find_tuple_index(&new_parent_key);
            (new_parent_key, parent_page_num, tuple_index)
        };

        internal_node_insert(
            pager,
            parent_page_num,
            tuple_index,
            &new_parent_key,
            left_node_page_num,
            split_strategy,
        )
    }
}

fn split_leaf_node_insert(
    pager: &mut Pager,
    page_num: u32,
    tuple_index: usize,
    key: &KeyVal,
    val: &TupVal,
    split_strategy: SplitStrategy,
) -> Result<(), errors::RecallError> {
    let is_root_node = {
        let right_node_cell = pager.fetch(page_num);
        let right_node = right_node_cell.borrow();
        right_node.is_root_node()
    };

    // Handle the case were the leaf node (we are splitting) is the root node.
    // In this case, we also need to create a new node as a root node.
    if is_root_node {
        let right_node_cell = pager.fetch(page_num);
        let right_node = right_node_cell.borrow_mut();
        let (key_scm, tup_scm) = &right_node.leaf_key_and_tup_scm();

        // get a fresh node for the left child
        let left_node_page_num = pager.get_fresh_page_num();
        let left_node_cell = pager.fetch(left_node_page_num);
        let mut left_node = left_node_cell.borrow_mut();
        left_node.leaf_initialize_node(false, key_scm, tup_scm);

        // also, get a fresh node for the right child since the right node
        // will become the new root
        let new_right_node_page_num = pager.get_fresh_page_num();
        let new_right_node_cell = pager.fetch(new_right_node_page_num);
        let mut new_right_node = new_right_node_cell.borrow_mut();
        new_right_node.leaf_initialize_node(false, key_scm, tup_scm);

        // leaf split routine
        let num = right_node.num_tuples();
        let pivot_index = num / 2usize;
        right_node.leaf_split_into(
            pivot_index,
            &mut left_node,
            &mut new_right_node,
            tuple_index,
            key,
            val,
            split_strategy,
        )?;

        // move right node into new_root
        let mut new_root = right_node;
        new_root.internal_initialize_node(true, key_scm);
        new_root.set_right_child(new_right_node_page_num);

        new_right_node.set_parent_ptr(page_num);
        new_right_node.set_l_sibling(left_node_page_num);
        left_node.set_parent_ptr(page_num);
        left_node.set_r_sibling(new_right_node_page_num);

        // insert new node key/pair into parent
        let new_parent_key = left_node.max_key().unwrap();
        let tuple_index = new_root.leaf_find_tuple_index(&new_parent_key);
        assert!(new_root.internal_put(&new_parent_key, left_node_page_num, tuple_index)?);

        Ok(())
    } else {
        let left_node_page_num = pager.get_fresh_page_num();
        let (new_parent_key, parent_page_num, tuple_index) = {
            let right_node_cell = pager.fetch(page_num);
            let mut right_node = right_node_cell.borrow_mut();
            // save some values from right node needed below
            let parent_page_num = right_node.parent_ptr();
            let left_sibling_page_num = right_node.l_sibling();
            let (key_scm, tup_scm) = &right_node.leaf_key_and_tup_scm();

            // get a fresh node for the left child
            let left_node_cell = pager.fetch(left_node_page_num);
            let mut left_node = left_node_cell.borrow_mut();
            left_node.leaf_initialize_node(false, key_scm, tup_scm);
            left_node.set_parent_ptr(parent_page_num);

            // split right node with left node (new node)
            let num = right_node.num_tuples();
            let pivot_index = num / 2usize;
            // TODO: pull items directly from right node and put into left node
            //       instead of creating a copy page ???
            {
                let mut copy = pager.get_empty_page();
                SlottedPage::copy(&right_node, &mut copy);
                // re-initialize right node
                right_node.leaf_initialize_node(false, key_scm, tup_scm);
                copy.leaf_split_into(
                    pivot_index,
                    &mut left_node,
                    &mut right_node,
                    tuple_index,
                    key,
                    val,
                    split_strategy
                )?;
            }

            // set sibling pointers
            if left_sibling_page_num != format::NULL_PTR as u32 {
                let left_sibling_cell = pager.fetch(left_sibling_page_num);
                let mut left_sibling = left_sibling_cell.borrow_mut();
                left_sibling.set_r_sibling(left_node_page_num);
                left_node.set_l_sibling(left_sibling_page_num);
            }
            left_node.set_r_sibling(page_num);
            right_node.set_l_sibling(left_node_page_num);

            // insert new node key/pair into parent
            let new_parent_key = left_node.max_key().unwrap();
            let parent_node_cell = pager.fetch(parent_page_num);
            let parent_node = parent_node_cell.borrow();
            let tuple_index = parent_node.internal_find_tuple_index(&new_parent_key);
            (new_parent_key, parent_page_num, tuple_index)
        };

        internal_node_insert(
            pager,
            parent_page_num,
            tuple_index,
            &new_parent_key,
            left_node_page_num,
            split_strategy
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::env;
    use std::fs;
    use format::sizedbuf::SizedBuf;
    use format::ATOM_TYPE;
    use format::UINT_TYPE;
    use format::INT_TYPE;
    use format::tuple::Tuple;
    use format::tuple::ParameterType;
    use super::pager::PagerBuilder;

    fn u32_key(key: u32) -> KeyVal {
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, key);
            tuple
        };
        tuple.decode(&[UINT_TYPE])
    }

    struct Fixture {
        temp_path: std::path::PathBuf,
    }

    impl Fixture {
        fn new(file_name: &str) -> Self {
            let mut temp_path = env::temp_dir();
            temp_path.push(file_name);
            Fixture { temp_path }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_file(&self.temp_path).unwrap_or(());
        }
    }

    fn build_tree_from_keys(btree: &mut BPlusTree, keys: impl Iterator<Item = u32>) {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        {
            let pager = btree.pager.borrow_mut();
            let root_node_cell = pager.fetch(0);
            let mut root_node = root_node_cell.borrow_mut();
            root_node.leaf_initialize_node(true, key_scm, tup_scm);
        }
        for key in keys {
            let tuple = {
                let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                tuple.write_u32_offset(0, key);
                tuple.decode(tup_scm)
            };
            btree.insert(&u32_key(key), &tuple).unwrap();
        }
    }

    #[test]
    fn it_can_insert_and_find_leaf_btree() {
        let fixture = Fixture::new("it_can_insert_and_find_leaf_btree.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![42].into_iter());

        // assert can find by key
        let find = btree.find(&u32_key(42));
        assert!(find.is_ok());
        let find = find.unwrap();
        assert!(find.is_some());
        let got = find.unwrap();
        let want = vec![ParameterType::UInt(42)];
        assert_eq!(got, want);

        // assert can find by key that does exist
        let find = btree.find(&u32_key(42 + 1));
        assert!(find.is_ok());
        assert!(find.unwrap().is_none());
    }

    #[test]
    fn it_will_return_uniq_key_error_on_existing_key() {
        let fixture = Fixture::new("it_will_return_uniq_key_error_on_existing_key.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![42].into_iter());

        // assert cannot insert the same key again
        let tuple = {
            let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
            tuple.write_u32_offset(0, 42);
            tuple.decode(&[UINT_TYPE])
        };
        let result = btree.insert(&u32_key(42), &tuple);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), errors::RecallError::UniqueKeyError);
    }

    #[test]
    fn it_can_iterate_leaf_btree() {
        let fixture = Fixture::new("it_can_iterate_leaf_btree.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![6, 1, 3, 69, 42].into_iter());

        // assert can iterate them
        let got: Vec<TupVal> =
            btree
            .into_iter()
            .collect();
        let want: Vec<TupVal> =
            vec![1, 3, 6, 42, 69]
            .into_iter()
            .map(|x| vec![ParameterType::UInt(x)])
            .collect();
        assert_eq!(got, want);
    }


    #[test]
    fn it_can_iterate_leaf_btree_rev() {
        let fixture = Fixture::new("it_can_iterate_leaf_btree_rev.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![6, 1, 3, 69, 42].into_iter());

        // assert can iterate them
        let got: Vec<TupVal> =
            btree
            .into_iter()
            .rev()
            .collect();
        let want: Vec<TupVal> =
            vec![69, 42, 6, 3, 1]
            .into_iter()
            .map(|x| vec![ParameterType::UInt(x)])
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn it_can_range_scan_leaf_btree() {
        let fixture = Fixture::new("it_can_range_scan_leaf_btree.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![6, 1, 3, 69, 42].into_iter());

        // assert can iterate them
        let got: Vec<TupVal> =
            btree
            .range_scan(&u32_key(6), &u32_key(69))
            .unwrap()
            .collect();
        let want: Vec<TupVal> =
            vec![6, 42]
            .into_iter()
            .map(|x| vec![ParameterType::UInt(x)])
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn it_can_split_leaf_root_insert() {
        let fixture = Fixture::new("it_can_split_leaf_root_insert.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .cap_limit(Some(5))
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, vec![1, 3, 6, 42, 69, /*causes split:*/ 1337].into_iter());

        let pager = btree.pager.borrow_mut();
        assert_eq!(pager.pages.borrow().len(), 3);

        // assert root node is correct
        let root_page_num = 0;
        let (left_child_page_num, right_child_page_num) = {
            let root_node_cell = pager.fetch(root_page_num);
            let root_node = root_node_cell.borrow();
            assert_eq!(root_node.num_tuples(), 1);
            assert_eq!(root_node.parent_ptr(), format::NULL_PTR);
            assert_ne!(root_node.right_child(), format::NULL_PTR);
            assert!(root_node.is_root_node());
            assert!(!root_node.is_leaf_node());
            assert_eq!(root_node.internal_get_key(0), u32_key(69));
            (root_node.internal_get_val(0), root_node.right_child())
        };

        // assert left node is correct
        {
            let node = pager.fetch(left_child_page_num);
            let node = node.borrow();
            assert_eq!(node.num_tuples(), 5);
            assert_eq!(node.parent_ptr(), root_page_num);
            assert!(!node.is_root_node());
            assert!(node.is_leaf_node());
            assert_eq!(node.r_sibling(), right_child_page_num);
            assert_eq!(node.l_sibling(), format::NULL_PTR);
            assert_eq!(node.leaf_get_key(0), u32_key(1));
            assert_eq!(node.leaf_get_key(1), u32_key(3));
            assert_eq!(node.leaf_get_key(2), u32_key(6));
            assert_eq!(node.leaf_get_key(3), u32_key(42));
            assert_eq!(node.leaf_get_key(4), u32_key(69));
        }

        // assert right node is correct
        {
            let node = pager.fetch(right_child_page_num);
            let node = node.borrow();
            assert_eq!(node.num_tuples(), 1);
            assert_eq!(node.parent_ptr(), root_page_num);
            assert!(!node.is_root_node());
            assert!(node.is_leaf_node());
            assert_eq!(node.r_sibling(), format::NULL_PTR);
            assert_eq!(node.l_sibling(), left_child_page_num);
            assert_eq!(node.leaf_get_key(0), u32_key(1337));
        }
    }

    #[test]
    fn it_can_split_internal_root_insert() {
        let fixture = Fixture::new("it_can_split_internal_root_insert.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .cap_limit(Some(5))
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, (0..31).into_iter());

        // assert root node is correct
        let root_page_num = 0;
        let (left_child_page_num, right_child_page_num) = {
            let pager = btree.pager.borrow_mut();
            let root_node_cell = pager.fetch(root_page_num);
            let root_node = root_node_cell.borrow();
            assert_eq!(root_node.num_tuples(), 1);
            assert_eq!(root_node.parent_ptr(), format::NULL_PTR);
            assert_ne!(root_node.right_child(), format::NULL_PTR);
            assert!(root_node.is_root_node());
            assert!(!root_node.is_leaf_node());
            assert_eq!(root_node.internal_get_key(0), u32_key(24));
            (root_node.internal_get_val(0), root_node.right_child())
        };

        // assert left node is correct
        {
            let pager = btree.pager.borrow_mut();
            let node = pager.fetch(left_child_page_num);
            let node = node.borrow();
            assert_eq!(node.num_tuples(), 4);
            assert_eq!(node.parent_ptr(), root_page_num);
            assert!(!node.is_root_node());
            assert!(!node.is_leaf_node());
            assert_ne!(node.right_child(), format::NULL_PTR);
            assert_eq!(node.internal_get_key(0), u32_key(4));
            assert_eq!(node.internal_get_key(1), u32_key(9));
            assert_eq!(node.internal_get_key(2), u32_key(14));
            assert_eq!(node.internal_get_key(3), u32_key(19));
        }

        // assert right node is correct
        {
            let pager = btree.pager.borrow_mut();
            let node = pager.fetch(right_child_page_num);
            let node = node.borrow();
            assert_eq!(node.num_tuples(), 1);
            assert_eq!(node.parent_ptr(), root_page_num);
            assert!(!node.is_root_node());
            assert!(!node.is_leaf_node());
            assert_ne!(node.right_child(), format::NULL_PTR);
            assert_eq!(node.internal_get_key(0), u32_key(29));
        }

        let got: Vec<TupVal> =
            btree
            .into_iter()
            .collect();
        let want: Vec<TupVal> =
            (0..31)
            .map(|x| {vec![ParameterType::UInt(x)]})
            .collect();

        assert_eq!(got, want);
    }

    #[test]
    fn it_works_on_100_keys() {
        let fixture = Fixture::new("it_works_on_100_keys.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .cap_limit(Some(5))
                .build()
            ));
        let mut btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();
        build_tree_from_keys(&mut btree, (0..100).into_iter());

        let got: Vec<TupVal> =
            btree
            .into_iter()
            .collect();
        let want: Vec<TupVal> =
            (0..100)
            .map(|x| {vec![ParameterType::UInt(x)]})
            .collect();

        assert_eq!(got, want);
    }

    #[test]
    fn it_creates_a_btree_simulating_predicate_backed_storage() {
        let fixture = Fixture::new("it_creates_a_btree_simulating_predicate_backed_storage.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_empty_page()
            .build();

        // build the btree
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[ATOM_TYPE, INT_TYPE];
        {
            let pager = btree.pager.borrow_mut();
            let root_node_cell = pager.fetch(0);
            let mut root_node = root_node_cell.borrow_mut();
            root_node.leaf_initialize_node(true, key_scm, tup_scm);
        }
        for key in 0..10 {
            let tuple = {
                let atom = "hello_world";
                let storage_size =
                    SizedBuf::atom_storage_size(atom.len()) +
                    SizedBuf::int_storage_size();
                let mut tuple = Tuple::new(storage_size);
                let offset = tuple.write_atom_offset(0, atom);
                tuple.write_i32_offset(offset, key as i32);
                tuple.decode(tup_scm)
            };
            btree.insert(&u32_key(key), &tuple).unwrap();
        }

        let got: Vec<TupVal> =
            btree
            .into_iter()
            .collect();
        let want: Vec<TupVal> =
            (0..10)
            .into_iter()
            .map(|x| vec![ParameterType::Atom("hello_world".to_string()), ParameterType::Int(x)])
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn it_creates_a_btree_simulating_secondary_index_backed_storage() {
        let fixture = Fixture::new("it_creates_a_btree_simulating_secondary_index_backed_storage.db");
        let pager =
            Rc::new(RefCell::new(
                PagerBuilder::new(&fixture.temp_path)
                .build()
            ));
        let btree =
            BPlusTreeBuilder::new(Rc::clone(&pager), 0)
            .split_strategy_half_full_page()
            .build();

        // build the btree
        let key_scm = &[ATOM_TYPE, INT_TYPE];
        let tup_scm = &[UINT_TYPE];
        {
            let pager = btree.pager.borrow_mut();
            let root_node_cell = pager.fetch(0);
            let mut root_node = root_node_cell.borrow_mut();
            root_node.leaf_initialize_node(true, key_scm, tup_scm);
        }
        for key in 0..10 {
            let key_tup = {
                let atom = "hello_world";
                let storage_size =
                    SizedBuf::atom_storage_size(atom.len()) +
                    SizedBuf::int_storage_size();
                let mut tuple = Tuple::new(storage_size);
                let offset = tuple.write_atom_offset(0, atom);
                tuple.write_i32_offset(offset, key as i32);
                tuple.decode(key_scm)
            };
            btree.insert(&key_tup, &u32_key(key)).unwrap();
        }

        let got: Vec<TupVal> =
            btree
            .clone()
            .into_iter()
            .collect();
        let want: Vec<TupVal> =
            (0..10)
            .into_iter()
            .map(|x| vec![ParameterType::UInt(x)])
            .collect();
        assert_eq!(got, want);

        let key_tup = {
            let atom = "hello_world";
            let storage_size =
                SizedBuf::atom_storage_size(atom.len()) +
                SizedBuf::int_storage_size();
            let mut tuple = Tuple::new(storage_size);
            let offset = tuple.write_atom_offset(0, atom);
            tuple.write_i32_offset(offset, 6);
            tuple.decode( key_scm)
        };
        let find = btree.find(&key_tup);
        assert!(find.is_ok());
        let find = find.unwrap();
        assert!(find.is_some());
        let got = find.unwrap();
        let want = vec![ParameterType::UInt(6)];
        assert_eq!(got, want);
    }
}
