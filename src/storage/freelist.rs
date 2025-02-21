use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;

pub const USIZE_SIZE: usize = std::mem::size_of::<usize>();

fn get_file(base_dir: &str, name: &str) -> File {
    let file_name = format!("{}.{}", name, "freelist");
    let path = Path::new(base_dir).join(file_name);

    let file =
        OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(path)
        .unwrap();

    file
}

pub fn add(base_dir: &str, name: &str, page_num: usize) {
    let mut file = get_file(base_dir, name);
    let mut freelist = Freelist::new(&file);
    freelist.pages.push(page_num);
    freelist.save(&mut file);
}

pub fn get(base_dir: &str, name: &str) -> Option<usize> {
    let mut file = get_file(base_dir, name);
    let mut freelist = Freelist::new(&file);
    let result = freelist.pages.pop();
    freelist.save(&mut file);
    result
}

struct Freelist {
    pages: Vec<usize>,
}

impl Freelist {
    fn new(file: &File) -> Self {
        let file_len = file.metadata().unwrap().len() as usize;

        assert!(
            file_len % USIZE_SIZE == 0,
            "Error: Corrupt file: freelist not multiple of usize size.",
        );

        if file_len == 0 {
            return Freelist { pages: vec![] }
        }

        let mut reader = BufReader::new(file);

        // read size of vector
        let mut buf = [0u8; USIZE_SIZE];
        reader.read_exact(&mut buf).unwrap();
        let size = usize::from_be_bytes(buf);

        // read `size` pages into vector
        let mut pages = Vec::with_capacity(size);
        let mut buf = [0u8; USIZE_SIZE];
        for _ in 0..size {
            reader.read_exact(&mut buf).unwrap();
            let page = usize::from_be_bytes(buf);
            pages.push(page);
        }

        Freelist { pages }
    }
}

impl Freelist {
    fn save(&mut self, file: &mut File) {
        // truncate file
        file.set_len(0).unwrap();

        // reset file position to beginning
        file.seek(SeekFrom::Start(0)).unwrap();

        let mut writer = BufWriter::new(file);

        // write size of vector
        let mut buf = usize::to_be_bytes(self.pages.len());
        writer.write_all(&mut buf).unwrap();

        // write each page element
        for num in self.pages.iter() {
            let mut buf = usize::to_be_bytes(*num);
            writer.write_all(&mut buf).unwrap();
        }

        writer.flush().unwrap();
    }
}
