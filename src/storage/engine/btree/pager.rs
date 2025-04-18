use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::os::unix::fs::OpenOptionsExt;

use crate::storage::engine::btree::format::PAGE_SIZE_4K;
use crate::storage::engine::btree::format::page::SlottedPage;
use crate::storage::engine::btree::format::page::PageHeader;
use crate::storage::engine::btree::format::page::InternalOps;
use crate::storage::engine::btree::format::page::LeafOps;

pub struct PagerBuilder<P: AsRef<Path>> {
    file_path: P,
    page_size: usize,
    cap_limit: Option<usize>,
}

impl<P: AsRef<Path>> PagerBuilder<P> {
    pub fn new(file_path: P) -> Self {
        PagerBuilder { file_path, page_size: PAGE_SIZE_4K, cap_limit: None }
    }

    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn cap_limit(mut self, cap_limit: Option<usize>) -> Self {
        self.cap_limit =
            cap_limit
            .inspect(|cap_limit| { assert!(*cap_limit > 2); });

        self
    }

    pub fn build(self) -> Pager {
        Pager::open(self.file_path, self.page_size, self.cap_limit)
    }
}

pub struct Pager {
    file: RefCell<File>,
    pub stored_pages: u32,
    last_page_num: u32,
    pub pages: RefCell<HashMap<u32, Rc<RefCell<SlottedPage>>>>,
    page_size: usize,
    cap_limit: Option<usize>,
}

impl Pager {
    fn open<P: AsRef<Path>>(file_path: P, page_size: usize, cap_limit: Option<usize>) -> Self {
        let file =
            OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(file_path)
            .unwrap();

        let file_len: usize =
            file
            .metadata()
            .unwrap()
            .len()
            .try_into()
            .unwrap();

        assert!(
            file_len % page_size == 0,
            "Error: Corrupt file: db file not multiple of page size.",
        );

        let stored_pages = file_len / page_size;
        let stored_pages = u32::try_from(stored_pages).unwrap();

        Pager {
            file: RefCell::new(file),
            stored_pages,
            last_page_num: if stored_pages == 0 { 0 } else { stored_pages - 1 },
            pages: RefCell::new(HashMap::new()),
            page_size,
            cap_limit,
        }
    }
}

impl Pager {
    pub fn get_fresh_page_num(&mut self) -> u32 {
        self.last_page_num += 1;
        self.last_page_num
    }

    pub fn get_empty_page(&self) -> SlottedPage {
        SlottedPage::new(self.page_size, self.cap_limit)
    }

    pub fn fetch(&self, page_num: u32) -> Rc<RefCell<SlottedPage>> {
        {
            let mut pages = self.pages.borrow_mut();
            if !pages.contains_key(&page_num) {
                let new_page = {
                    let mut page = self.get_empty_page();
                    if page_num < self.stored_pages {
                        let seek_loc = (page_num as usize * self.page_size) as u64;
                        let mut file = self.file.borrow_mut();
                        file.seek(SeekFrom::Start(seek_loc)).unwrap();
                        file.read_exact(page.get_mut()).unwrap();
                    }
                    page
                };
                pages.insert(page_num, Rc::new(RefCell::new(new_page)));
            }
        }
        // Return a clone of the Rc so that the caller owns it.
        self.pages.borrow().get(&page_num).unwrap().clone()
    }

    pub fn flush_all_pages(&self) {
        let mut page_nums: Vec<u32> = self.pages.borrow().keys().cloned().collect();
        page_nums.sort();

        println!("DEBUG: flush page nums {:?}", page_nums);

        for page_num in page_nums {
            self.flush_page(page_num);
        }
    }

    fn flush_page(&self, page_num: u32) {
        let pages = self.pages.borrow();
        let page = pages.get(&page_num).unwrap();

        let mut file = self.file.borrow_mut();

        file
        .seek(SeekFrom::Start((page_num as usize * self.page_size).try_into().unwrap()))
        .unwrap();

        file.write_all(&page.borrow().get()).unwrap();

        file.flush().unwrap();
    }
}


impl Pager {
    pub fn get_dot_string(&self, root_page_num: u32) -> String {
        let mut result = String::new();
        result.push_str("digraph BPlusTree {\n");
        result.push_str("node [shape=record, fontname=Helvetica];\n");
        result.push_str(&self.get_dot_string_by_page(root_page_num));
        result.push_str("}\n");
        result
    }

    fn get_dot_string_by_page(&self, page_num: u32) -> String {
        let page_cell = self.fetch(page_num);
        let page = page_cell.borrow();
        let count = page.num_tuples();

        if page.is_leaf_node() {
            let mut result = String::new();

            result.push_str(&format!("n{} [label=\"", page_num));

            for i in 0..count {
                let key = page.leaf_get_key(i);
                if i < count - 1 {
                    result.push_str(&format!("{:?} | ", key));
                } else {
                    result.push_str(&format!("{:?}\"];\n", key));
                }
            }

            result
        } else {
            let right_child_num = page.right_child();

            let mut result = String::new();

            result.push_str(&format!("n{} [label=\"", page_num));
            for i in 0..count {
                let key = page.internal_get_key(i);
                let child_page_num = page.internal_get_val(i);
                result.push_str(&format!("<p{}> | {:?} | ", child_page_num, key));
            }
            result.push_str(&format!("<p{}>\"];\n", right_child_num));

            for i in 0..count {
                let child_page_num = page.internal_get_val(i);
                result.push_str(&format!(
                    "n{}:p{} -> n{} [label=\"{},{}\"];\n",
                    page_num,
                    child_page_num,
                    child_page_num,
                    page_num,
                    child_page_num,
                ));
            }
            result.push_str(&format!(
                "n{}:p{} -> n{} [label=\"{},{}\"];\n",
                page_num,
                right_child_num,
                right_child_num,
                page_num,
                right_child_num,
            ));

            for i in 0..count {
                let child_page_num = page.internal_get_val(i);
                result.push_str(&self.get_dot_string_by_page(child_page_num));
            }
            result.push_str(&self.get_dot_string_by_page(right_child_num));

            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use crate::storage::engine::btree::format::UINT_TYPE;
    use crate::storage::engine::btree::format::page::InternalOps;
    use crate::storage::engine::btree::format::page::LeafOps;

    #[test]
    fn it_opens_and_initializes_correctly_new_file() {
        let mut temp_path = env::temp_dir();
        temp_path.push("it_opens_and_initializes_correctly_new_file.db");
        let pager =
            PagerBuilder::new(&temp_path)
            .build();
        assert_eq!(pager.stored_pages, 0);
        assert_eq!(pager.last_page_num, 0);
        fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn it_opens_and_initializes_correctly_existing_file() {
        let mut temp_path = env::temp_dir();
        temp_path.push("it_opens_and_initializes_correctly_existing_file.db");
        let page_size = PAGE_SIZE_4K;
        {
            let mut f = File::create_new(&temp_path).unwrap();
            let empty_page = vec![0u8; page_size];
            f.write_all(&empty_page).unwrap();
        }
        let mut pager =
            PagerBuilder::new(&temp_path)
            .page_size(PAGE_SIZE_4K)
            .build();
        assert_eq!(pager.stored_pages, 1);
        assert_eq!(pager.last_page_num, 0);
        assert_eq!(pager.get_fresh_page_num(), 1);
        assert_eq!(pager.last_page_num, 1);
        fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn it_flushes_pages_to_disk() {
        let key_scm = &[UINT_TYPE];
        let tup_scm = &[UINT_TYPE];
        let mut temp_path = env::temp_dir();
        temp_path.push("it_flushes_pages_to_disk.db");

        {
            let pager =
                PagerBuilder::new(&temp_path)
                .page_size(PAGE_SIZE_4K)
                .build();
            {
                let internal_page_cell = pager.fetch(0);
                let mut internal_page = internal_page_cell.borrow_mut();
                let leaf_page_cell = pager.fetch(1);
                let mut leaf_page = leaf_page_cell.borrow_mut();
                internal_page.internal_initialize_node(true, key_scm);
                leaf_page.leaf_initialize_node(true, key_scm, tup_scm);
            }
            pager.flush_all_pages();
        }

        {
            let pager =
                PagerBuilder::new(&temp_path)
                .page_size(PAGE_SIZE_4K)
                .build();
            assert_eq!(pager.stored_pages, 2);
            assert_eq!(pager.last_page_num, 1);
            let internal_page_cell = pager.fetch(0);
            let internal_page_got = internal_page_cell.borrow();
            let leaf_page_cell = pager.fetch(1);
            let leaf_page_got = leaf_page_cell.borrow();

            let mut internal_page_want = pager.get_empty_page();
            internal_page_want.internal_initialize_node(true, key_scm);
            let mut leaf_page_want = pager.get_empty_page();
            leaf_page_want.leaf_initialize_node(true, key_scm, tup_scm);

            assert_eq!(internal_page_got.get(), internal_page_want.get());
            assert_eq!(leaf_page_got.get(), leaf_page_want.get());
        }

        fs::remove_file(&temp_path).unwrap();
    }
}
