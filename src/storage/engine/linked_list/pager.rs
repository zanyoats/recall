use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::collections::HashMap;
use std::os::unix::fs::OpenOptionsExt;

use crate::storage::engine::linked_list::page::Page;
use crate::storage::engine::linked_list::page::PAGE_SIZE;

pub struct Pager {
    file: File,
    file_pages: usize,
    pages: HashMap<usize, Page>,
}

fn ensure_dir_exists(dir: &str) {
    let path = Path::new(dir);
    if !path.exists() {
        fs::create_dir_all(path).unwrap();
    }
}

impl Pager {
    pub fn create(base_dir: &str, name: &str) -> Self {
        let file_name = format!("{}.{}", name, "db");

        ensure_dir_exists(base_dir);

        let path = Path::new(base_dir).join(file_name);

        let file =
            OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();

        Pager {
            file,
            file_pages: 0,
            pages: HashMap::new(),
        }
    }

    pub fn open(base_dir: &str, name: &str) -> Self {
        let file_name = format!("{}.{}", name, "db");
        let path = Path::new(base_dir).join(file_name);

        let file =
            OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .unwrap();

        let file_len: usize =
            file
            .metadata()
            .unwrap()
            .len()
            .try_into()
            .unwrap();

        assert!(
            file_len > 0,
            "Error: Empty file.",
        );

        assert!(
            file_len % PAGE_SIZE == 0,
            "Error: Corrupt file: db file not multiple of page size.",
        );

        let file_pages = file_len / PAGE_SIZE;

        Pager {
            file,
            file_pages,
            pages: HashMap::new(),
        }
    }
}

impl Pager {
    fn fetch_mut(&mut self, page_num: usize) -> &mut Page {
        self.pages
            .entry(page_num)
            .or_insert_with(|| {
                // Page not loaded
                // Allocate new page or load from db file

                let mut page = [0u8; PAGE_SIZE];

                if page_num < self.file_pages {
                    // We have the page on disk, so load it into `page`
                    let seek_loc = (page_num * PAGE_SIZE).try_into().unwrap();
                    self.file.seek(SeekFrom::Start(seek_loc)).unwrap();
                    self.file.read_exact(&mut page).unwrap();
                }

                page
            })
    }

    pub fn get(&mut self, page_num: usize) -> &Page {
        let page = self.fetch_mut(page_num);
        &*page
    }

    pub fn get_mut(&mut self, page_num: usize) -> &mut Page {
        self.fetch_mut(page_num)
    }

    pub fn flush_all_pages(&mut self) {
        let mut page_nums: Vec<usize> = self.pages.keys().cloned().collect();
        page_nums.sort();

        println!("flush page nums {:?}", page_nums);

        for page_num in page_nums {
            self.flush_page(page_num);
        }
    }

    fn flush_page(&mut self, page_num: usize) {
        let page = self.pages.get(&page_num).unwrap();

        self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE).try_into().unwrap())).unwrap();
        self.file.write_all(page).unwrap();
        self.file.flush().unwrap();
    }
}
