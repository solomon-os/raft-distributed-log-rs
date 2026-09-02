use std::{
    fs::File,
    io::{self, BufWriter, Write},
    os::unix::fs::FileExt,
};

#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;

#[derive(Clone, Debug)]
pub struct Config {
    pub sync_writes: bool,
}

#[derive(Debug)]
pub struct Store {
    size: u64,
    file: File,
    writer: BufWriter<File>,
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

    #[cfg(target_os = "linux")]
    pub fn name(&self) -> io::Result<PathBuf> {
        fs::read_link(format!("/proc/self/fd/{}", self.file.as_raw_fd()))
    }

    #[cfg(target_os = "macos")]
    pub fn name(&self) -> io::Result<String> {
        let mut buf = [0u8; libc::PATH_MAX as usize];
        let ret = unsafe {
            libc::fcntl(
                self.file.as_raw_fd(),
                libc::F_GETPATH,
                buf.as_mut_ptr() as *mut libc::c_char,
            )
        };
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }

        let name = std::ffi::CStr::from_bytes_until_nul(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(name.to_string_lossy().into_owned())
    }

    pub fn read(&mut self, pos: u64, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut size = [0; LENGTH_WIDTH];
        self.writer.flush()?;
        self.file.read_exact_at(&mut size, pos)?;
        buf.resize(u64::from_be_bytes(size) as usize, 0);
        self.file.read_exact_at(buf, pos + (LENGTH_WIDTH as u64))?;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.size
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
    use std::fs;

    #[test]
    fn store_lifecycle_no_sync_writes() {
        let config = Config { sync_writes: false };
        store_lifecycle(config);
    }

    #[test]
    fn store_lifecycle_sync_writes() {
        let config = Config { sync_writes: true };
        store_lifecycle(config);
    }

    fn store_lifecycle(config: Config) {
        let (file, file_name) = crate::log::test_util::temp_file(".store");

        let mut store = Store::new(file, config).unwrap();

        store_append(&mut store);
        store_read(&mut store);
        store_close(&mut store);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_new_reopens_existing_file_with_correct_size() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let config = Config { sync_writes: false };

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
        let mut store = Store::new(file, Config { sync_writes: false }).unwrap();

        store.append(&payload(0)).unwrap();

        let mut buf = Vec::new();
        let err = store.read(1000, &mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_append_empty_buffer_round_trips() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let mut store = Store::new(file, Config { sync_writes: false }).unwrap();

        let written = store.append(&[]).unwrap();
        assert_eq!(written, LENGTH_WIDTH as u64);

        let mut buf = Vec::new();
        store.read(0, &mut buf).unwrap();
        assert!(buf.is_empty());

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn store_read_without_prior_flush() {
        let (file, file_name) = crate::log::test_util::temp_file(".store");
        let config = Config { sync_writes: false };
        let mut store = Store::new(file, config).unwrap();

        let write = payload(0);
        store.append(&write).unwrap();

        let mut buf = Vec::new();
        store.read(0, &mut buf).unwrap();
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
        let mut buf = Vec::new();

        for i in 0..4 {
            let write = payload(i);
            store.read(pos, &mut buf).unwrap();
            assert_eq!(&write[..], &buf[..]);
            pos += (write.len() + LENGTH_WIDTH) as u64;
        }
    }

    fn store_close(store: &mut Store) {
        store.close().unwrap();
    }
}
