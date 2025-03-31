use crate::storage::engine::btree::pager::Pager;

use crate::storage::engine::btree::format::page::PageHeader;
use crate::storage::engine::btree::format::page::InternalOps;
use crate::storage::engine::btree::format::page::LeafOps;

#[derive(PartialEq, Eq)]
pub struct BTreeCursor {
    pub page_num: u32,
    pub tuple_index: usize,
}

impl BTreeCursor {
    fn new(page_num: u32, tuple_index: usize) -> Self {
        BTreeCursor { page_num, tuple_index }
    }
}

impl BTreeCursor {
    pub fn begin(pager: &mut Pager, root_page_num: u32) -> Self {
        let page_num = Self::traverse_leftmost_leaf_node(pager, root_page_num);
        BTreeCursor::new(page_num, 0)
    }

    pub fn end(pager: &mut Pager, root_page_num: u32) -> Self {
        let page_num = Self::traverse_rightmost_leaf_node(pager, root_page_num);
        let node_cell = pager.fetch(page_num);
        let node = node_cell.borrow();
        BTreeCursor::new(page_num, node.num_tuples())
    }

    /// find points to the insertion point
    pub fn find(pager: &mut Pager, root_page_num: u32, key: u32) -> Self {
        let root_node_cell = pager.fetch(root_page_num);
        let root_node = root_node_cell.borrow();

        let (page_num, tuple_index) =
            if root_node.is_leaf_node() {
                Self::find_leaf_node(pager, root_page_num, key)
            } else {
                Self::find_internal_node(pager, root_page_num, key)
            };
        BTreeCursor::new(page_num, tuple_index)
    }

    fn find_internal_node(pager: &mut Pager, page_num: u32, find_key: u32) -> (u32, usize) {
        let parent_cell = pager.fetch(page_num);
        let parent = parent_cell.borrow();
        let tuple_index = parent.internal_find_tuple_index(&find_key);
        let child_page_num =
            if tuple_index == parent.num_tuples() {
                parent.right_child()
            } else {
                let child_page_num = parent.internal_get_val(tuple_index);
                child_page_num
            };

        let child_cell = pager.fetch(child_page_num);
        let child = child_cell.borrow();

        if child.is_leaf_node() {
            Self::find_leaf_node(pager, child_page_num, find_key)
        } else {
            Self::find_internal_node(pager, child_page_num, find_key)
        }
    }

    fn find_leaf_node(pager: &mut Pager, page_num: u32, find_key: u32) -> (u32, usize) {
        let node_cell = pager.fetch(page_num);
        let node = node_cell.borrow();
        let tuple_index = node.leaf_find_tuple_index(&find_key);
        (page_num, tuple_index)
    }

    fn traverse_leftmost_leaf_node(pager: &mut Pager, page_num: u32) -> u32 {
        let node_cell = pager.fetch(page_num);
        let node = node_cell.borrow();

        if node.is_leaf_node() {
            page_num
        } else {
            let child_page_num = node.internal_get_val(0);
            Self::traverse_leftmost_leaf_node(pager, child_page_num)
        }
    }

    fn traverse_rightmost_leaf_node(pager: &mut Pager, page_num: u32) -> u32 {
        let node_cell = pager.fetch(page_num);
        let node = node_cell.borrow();

        if node.is_leaf_node() {
            page_num
        } else {
            let child_page_num = node.right_child();
            Self::traverse_rightmost_leaf_node(pager, child_page_num)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use crate::storage::engine::btree::cursor::BTreeCursor;
    use crate::storage::engine::btree::format::page::InternalOps;
    use crate::storage::engine::btree::format::page::PageHeader;
    use crate::storage::engine::btree::pager::Pager;
    use crate::storage::engine::btree::format::tuple::Tuple;
    use crate::storage::engine::btree::format::sizedbuf::SizedBuf;
    use crate::storage::engine::btree::format::page::LeafOps;
    use crate::storage::engine::btree::format::page::SlottedPageBuilder;

    #[test]
    fn it_creates_cursors_for_tree_with_leaf_root() {
        let mut temp_path = env::temp_dir();
        temp_path.push("it_creates_cursors_for_tree_with_leaf_root.db");
        let page_builder = SlottedPageBuilder::new();
        let mut pager = Pager::open(&temp_path, page_builder);
        let root_page_num = 0;

        {
            let root_node_cell = pager.fetch(root_page_num);
            let mut root_node = root_node_cell.borrow_mut();
            root_node.leaf_initialize_node(true);
            let pairs: Vec<(u32, Tuple)> =
                vec![6, 1, 3, 69, 42]
                .into_iter()
                .map(|key| {
                    let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                    tuple.write_u32_offset(0, key * 2);
                    (key, tuple)
                })
                .collect();
            for (key, val) in pairs.iter() {
                let tuple_index: usize = root_node.leaf_find_tuple_index(key);
                assert!(root_node.leaf_put(key, val, tuple_index).unwrap());
            }
        }

        let begin = BTreeCursor::begin(&mut pager, root_page_num);
        assert_eq!(begin.page_num, root_page_num);
        assert_eq!(begin.tuple_index, 0);

        let end = BTreeCursor::end(&mut pager, root_page_num);
        assert_eq!(end.page_num, root_page_num);
        assert_eq!(end.tuple_index, 5);

        let find_42 = BTreeCursor::find(&mut pager, root_page_num, 42);
        assert_eq!(find_42.page_num, root_page_num);
        assert_eq!(find_42.tuple_index, 3);

        let find_0 = BTreeCursor::find(&mut pager, root_page_num, 0);
        assert_eq!(find_0.page_num, root_page_num);
        assert_eq!(find_0.tuple_index, 0);

        let find_1337 = BTreeCursor::find(&mut pager, root_page_num, 1337);
        assert_eq!(find_1337.page_num, root_page_num);
        assert_eq!(find_1337.tuple_index, 5);

        fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn it_creates_cursors_for_tree_with_depth_1() {
        let mut temp_path = env::temp_dir();
        temp_path.push("it_creates_cursors_for_tree_with_depth_1.db");
        let page_builder = SlottedPageBuilder::new();
        let mut pager = Pager::open(&temp_path, page_builder);
        let root_page_num = 0;
        let left_page_num = 1;
        let right_page_num = 2;

        { /* setup root node */
            let root_node_cell = pager.fetch(root_page_num);
            let mut root_node = root_node_cell.borrow_mut();
            root_node.internal_initialize_node(true);
            let tuple_index = root_node.internal_find_tuple_index(&100);
            assert!(root_node.internal_put(&100, &left_page_num, tuple_index));
            root_node.set_right_child(&right_page_num);
        }

        { /* set up left node */
            let node_cell = pager.fetch(left_page_num);
            let mut node = node_cell.borrow_mut();
            node.leaf_initialize_node(false);
            node.set_parent_ptr(root_page_num);
            node.set_r_sibling(right_page_num);
            let pairs: Vec<(u32, Tuple)> =
                vec![6, 100, 69, 42]
                .into_iter()
                .map(|key| {
                    let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                    tuple.write_u32_offset(0, key * 2);
                    (key, tuple)
                })
                .collect();
            for (key, val) in pairs.iter() {
                let tuple_index: usize = node.leaf_find_tuple_index(key);
                assert!(node.leaf_put(key, val, tuple_index).unwrap());
            }
        }

        { /* set up right node */
            let node_cell = pager.fetch(right_page_num);
            let mut node = node_cell.borrow_mut();
            node.leaf_initialize_node(false);
            node.set_parent_ptr(root_page_num);
            node.set_l_sibling(left_page_num);
            let pairs: Vec<(u32, Tuple)> =
                vec![900, 777, 666, 1337]
                .into_iter()
                .map(|key| {
                    let mut tuple = Tuple::new(SizedBuf::uint_storage_size());
                    tuple.write_u32_offset(0, key * 2);
                    (key, tuple)
                })
                .collect();
            for (key, val) in pairs.iter() {
                let tuple_index: usize = node.leaf_find_tuple_index(key);
                assert!(node.leaf_put(key, val, tuple_index).unwrap());
            }
        }

        let begin = BTreeCursor::begin(&mut pager, root_page_num);
        assert_eq!(begin.page_num, left_page_num);
        assert_eq!(begin.tuple_index, 0);

        let end = BTreeCursor::end(&mut pager, root_page_num);
        assert_eq!(end.page_num, right_page_num);
        assert_eq!(end.tuple_index, 4);

        let find_42 = BTreeCursor::find(&mut pager, root_page_num, 42);
        assert_eq!(find_42.page_num, left_page_num);
        assert_eq!(find_42.tuple_index, 1);

        let find_1337 = BTreeCursor::find(&mut pager, root_page_num, 1337);
        assert_eq!(find_1337.page_num, right_page_num);
        assert_eq!(find_1337.tuple_index, 3);

        fs::remove_file(&temp_path).unwrap();
    }
}
