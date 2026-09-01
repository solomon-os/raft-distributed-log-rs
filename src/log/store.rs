use std::{
    fs::File,
    io::{self, BufWriter, Write},
    os::unix::fs::FileExt,
};

use crate::log::Config;

struct Store {
    size: u64,
    file: File,
    writer: BufWriter<File>,
    read_buf: Vec<u8>,
    config: Config,
}

const LENGTH_WIDTH: usize = std::mem::size_of::<u64>();

impl Store {
    pub fn new(file: File, config: Config) -> Result<Self, io::Error> {
        let size = file.metadata()?.len();
        let writer = BufWriter::new(file.try_clone()?);
        Ok(Store {
            config,
            size,
            writer,
            file,
            read_buf: Vec::new(),
        })
    }

    pub fn append(&mut self, buf: &[u8]) -> io::Result<u64> {
        let len_bytes = (buf.len() as u64).to_be_bytes();
        self.writer.write_all(&len_bytes)?;
        let written = self.writer.write(buf)? + LENGTH_WIDTH;
        self.size += written as u64;
        if self.config.sync_writes {
            self.writer.flush()?;
            self.file.sync_data()?;
        }
        Ok(written as u64)
    }

    fn read(&mut self, pos: u64) -> io::Result<&[u8]> {
        let mut size = [0; LENGTH_WIDTH];
        self.writer.flush()?;
        self.file.read_exact_at(&mut size, pos)?;
        self.read_buf.resize(u64::from_be_bytes(size) as usize, 0);
        self.file
            .read_exact_at(&mut self.read_buf, pos + LENGTH_WIDTH as u64)?;
        Ok(&self.read_buf)
    }

    pub fn read_into_buffer(&mut self, pos: u64, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut size = [0; LENGTH_WIDTH];
        self.writer.flush()?;
        self.file.read_exact_at(&mut size, pos)?;
        buf.resize(u64::from_be_bytes(size) as usize, 0);
        self.file.read_exact_at(buf, pos + (LENGTH_WIDTH as u64))?;
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{env::temp_dir, fs};

    #[test]
    fn store_lifecycle_no_sync_writes() {
        let config = Config{sync_writes: false, ..Config::stub()};
        store_lifecycle(config);
    }

    #[test]
    fn store_lifecycle_sync_writes() {
        let config = Config{sync_writes: true, ..Config::stub()};
        store_lifecycle(config);
    }

    fn store_lifecycle(config: Config) {
        let (file, file_name) = crate::log::test_util::temp_file(".store");

        let mut store = Store::new(file, config).unwrap();

        store_append(&mut store);
        store_read(&mut store);
        store_read_into(&mut store);
        store_close(&mut store);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_new_reopens_existing_file_with_correct_size() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let config = Config::stub();

        {
            let mut store = Store::new(file, config.clone()).unwrap();
            store_append(&mut store);
            store.close().unwrap();
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();
        let mut store = Store::new(file, config).unwrap();

        let expected_size: u64 = (0..4)
            .map(|i| (payload(i).len() + LENGTH_WIDTH) as u64)
            .sum();
        assert_eq!(store.size, expected_size);

        store_read(&mut store);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_read_errors_when_out_of_range() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let mut store = Store::new(file, Config::stub()).unwrap();

        store.append(&payload(0)).unwrap();

        let err = store.read(1000).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_read_into_buffer_errors_when_out_of_range() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let mut store = Store::new(file, Config::stub()).unwrap();

        store.append(&payload(0)).unwrap();

        let mut buf = Vec::new();
        let err = store.read_into_buffer(1000, &mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_append_empty_buffer_round_trips() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let mut store = Store::new(file, Config::stub()).unwrap();

        let written = store.append(&[]).unwrap();
        assert_eq!(written, LENGTH_WIDTH as u64);

        let buf = store.read(0).unwrap();
        assert!(buf.is_empty());

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_read_into_buffer_without_prior_flush() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let config = Config {
            sync_writes: false,
            ..Config::stub()
        };
        let mut store = Store::new(file, config).unwrap();

        let write = payload(0);
        store.append(&write).unwrap();

        let mut buf = Vec::new();
        store.read_into_buffer(0, &mut buf).unwrap();
        assert_eq!(&write[..], &buf[..]);

        fs::remove_file(&file_name).unwrap();
    }

    fn payload(i: u64) -> Vec<u8> {
        format!("hello world {i}").into_bytes()
    }

    fn store_append(store: &mut Store) {
        let mut pos = store.size;
        for i in 0..4 {
            let write = payload(i);
            let written_size = store.append(&write).unwrap();

            assert_eq!(written_size, (write.len() + LENGTH_WIDTH) as u64);
            assert_eq!(store.size - pos, (write.len() + LENGTH_WIDTH) as u64);
            pos = store.size;
        }
    }

    fn store_read(store: &mut Store) {
        let mut pos = 0;
        for i in 0..4 {
            let write = payload(i);
            let buf = store.read(pos).unwrap();
            assert_eq!(buf, &write[..]);
            pos += (write.len() + LENGTH_WIDTH) as u64;
        }
    }

    fn store_read_into(store: &mut Store) {
        let mut pos = 0;
        let mut buf = Vec::new();

        for i in 0..4 {
            let write = payload(i);
            store.read_into_buffer(pos, &mut buf).unwrap();
            assert_eq!(&write[..], &buf[..]);
            pos += (write.len() + LENGTH_WIDTH) as u64;
        }
    }

    fn store_close(store: &mut Store) {
        store.close().unwrap();
    }
}
