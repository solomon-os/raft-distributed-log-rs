use memmap2::MmapMut;
#[cfg(target_os = "linux")]
use std::fs;
use std::{
    fs::File,
    io::{self, Result},
    os::unix::io::AsRawFd,
    path::PathBuf,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_size_bytes: u64,
}

#[derive(Debug)]
pub struct Index {
    file: File,
    mmap: MmapMut,
    pub size: u64,
}

const OFF_WIDTH: u64 = 4;
const POSITION_WIDTH: u64 = 8;
pub const ENTIRE_WIDTH: u64 = OFF_WIDTH + POSITION_WIDTH;

impl Index {
    pub fn new(file: File, config: Config) -> io::Result<Self> {
        let mut size = file.metadata()?.len();
        let is_new = size == 0;

        file.set_len(config.max_size_bytes)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file) }.unwrap();

        if is_new {
            // Mark every slot as unwritten so recover_size can tell a real
            // entry (off == 0 is valid for the first one) apart from padding.
            mmap.fill(0xFF);
        } else if size == config.max_size_bytes {
            size = Self::recover_size(&mmap)
        }

        Ok(Self { file, mmap, size })
    }

    fn recover_size(mmap: &MmapMut) -> u64 {
        let mut size: u64 = 0;

        let mut expected: u32 = 0;

        while size + ENTIRE_WIDTH <= mmap.len() as u64 {
            let start = size as usize;
            let off_bytes: [u8; OFF_WIDTH as usize] =
                mmap[start..start + OFF_WIDTH as usize].try_into().unwrap();
            let off = u32::from_be_bytes(off_bytes);

            if off != expected {
                break;
            }

            expected += 1;
            size += ENTIRE_WIDTH;
        }

        size
    }

    #[cfg(target_os = "linux")]
    pub fn name(&self) -> io::Result<PathBuf> {
        fs::read_link(format!("/proc/self/fd/{}", self.file.as_raw_fd()))
    }

    #[cfg(target_os = "macos")]
    pub fn name(&self) -> io::Result<PathBuf> {
        let mut buf = [0u8; libc::PATH_MAX as usize];
        let ret = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETPATH, buf.as_mut_ptr()) };
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }

        let name = std::ffi::CStr::from_bytes_until_nul(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string_lossy()
            .into_owned();
        Ok(PathBuf::from(name))
    }

    pub fn write(&mut self, offset: u32, pos: u64) -> io::Result<()> {
        if self.size + ENTIRE_WIDTH > self.mmap.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "index is full",
            ));
        }

        let start = self.size as usize;
        self.mmap[start..start + OFF_WIDTH as usize].copy_from_slice(&offset.to_be_bytes());
        self.mmap[start + OFF_WIDTH as usize..start + ENTIRE_WIDTH as usize]
            .copy_from_slice(&pos.to_be_bytes());

        self.size += ENTIRE_WIDTH;
        Ok(())
    }

    pub fn read(&self, off: u32) -> io::Result<(u32, u64)> {
        let start = off as usize * ENTIRE_WIDTH as usize;
        if start as u64 + ENTIRE_WIDTH > self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset bigger than index size",
            ));
        }

        let off_bytes: [u8; OFF_WIDTH as usize] = self.mmap[start..start + OFF_WIDTH as usize]
            .try_into()
            .unwrap();
        let pos_start = start + OFF_WIDTH as usize;
        let pos_bytes: [u8; POSITION_WIDTH as usize] = self.mmap
            [pos_start..pos_start + POSITION_WIDTH as usize]
            .try_into()
            .unwrap();

        Ok((u32::from_be_bytes(off_bytes), u64::from_be_bytes(pos_bytes)))
    }

    pub fn len(&self) -> u64 {
        self.size
    }

    pub fn current_offset(&self) -> u64 {
        self.size / ENTIRE_WIDTH
    }

    pub fn close(&mut self) -> Result<()> {
        self.mmap.flush()?;
        self.file.set_len(self.size)?;
        self.file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use crate::log::test_util::temp_file;

    use super::*;

    #[test]
    fn index_lifecycle() {
        let (file, file_name) = temp_file(".index");

        let mut index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();
        index_write(&mut index);
        index_read(&mut index);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_recovers_size_after_abrupt_drop() {
        let (file, file_name) = temp_file(".index");
        let config = Config {
            max_size_bytes: ENTIRE_WIDTH * 8,
        };

        {
            let config = config.clone();
            let mut index = Index::new(file, config).unwrap();
            index_write(&mut index);
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();

        assert_eq!(config.max_size_bytes, file.metadata().unwrap().len());

        let mut index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();

        assert_eq!(index.size, 4 * ENTIRE_WIDTH);
        index_read(&mut index);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_close_truncates_file_on_clean_shutdown() {
        let (file, file_name) = temp_file(".index");

        {
            let mut index = Index::new(
                file,
                Config {
                    max_size_bytes: ENTIRE_WIDTH * 8,
                },
            )
            .unwrap();
            index_write(&mut index);
            index.close().unwrap();
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();

        assert_eq!(file.metadata().unwrap().len(), 4 * ENTIRE_WIDTH);

        let mut index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();
        assert_eq!(index.size, 4 * ENTIRE_WIDTH);
        index_read(&mut index);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_name_returns_file_path() {
        let (file, file_name) = temp_file(".index");
        let index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();

        let name = index.name().unwrap();
        assert_eq!(name, file_name.canonicalize().unwrap());

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_write_errors_when_full() {
        let (file, file_name) = temp_file(".index");
        let config = Config {
            max_size_bytes: 2 * ENTIRE_WIDTH,
        };

        let mut index = Index::new(file, config).unwrap();
        index.write(0, payload(0)).unwrap();
        index.write(1, payload(1)).unwrap();

        let err = index.write(2, payload(2)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_read_errors_when_out_of_range() {
        let (file, file_name) = temp_file(".index");
        let mut index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();

        index.write(0, payload(0)).unwrap();

        let err = index.read(5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_recovers_zero_size_when_nothing_written() {
        let (file, file_name) = temp_file(".index");

        {
            let _index = Index::new(
                file,
                Config {
                    max_size_bytes: ENTIRE_WIDTH * 8,
                },
            )
            .unwrap();
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();

        let index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();
        assert_eq!(index.size, 0);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_recovers_full_size_when_completely_packed() {
        let (file, file_name) = temp_file(".index");
        let config = Config {
            max_size_bytes: 4 * ENTIRE_WIDTH,
            ..Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            }
        };

        {
            let mut index = Index::new(file, config.clone()).unwrap();
            index_write(&mut index);
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();

        let mut index = Index::new(file, config).unwrap();
        assert_eq!(index.size, 4 * ENTIRE_WIDTH);
        index_read(&mut index);

        fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn index_write_after_recovery_appends_correctly() {
        let (file, file_name) = temp_file(".index");

        {
            let mut index = Index::new(
                file,
                Config {
                    max_size_bytes: ENTIRE_WIDTH * 8,
                },
            )
            .unwrap();
            index_write(&mut index);
        }

        let file = File::options()
            .read(true)
            .write(true)
            .open(&file_name)
            .unwrap();

        let mut index = Index::new(
            file,
            Config {
                max_size_bytes: ENTIRE_WIDTH * 8,
            },
        )
        .unwrap();
        assert_eq!(index.size, 4 * ENTIRE_WIDTH);

        index.write(4, payload(4)).unwrap();

        let (off, pos) = index.read(4).unwrap();
        assert_eq!(off, 4);
        assert_eq!(pos, payload(4));

        index_read(&mut index);

        fs::remove_file(&file_name).unwrap();
    }

    fn payload(i: u64) -> u64 {
        1_000_000 + i
    }

    fn index_write(index: &mut Index) {
        for i in 0..4 {
            index.write(i, payload(i.into())).unwrap();
        }
    }

    fn index_read(index: &mut Index) {
        for i in 0..4 {
            let (off, pos) = index.read(i).unwrap();
            assert_eq!(i, off);
            assert_eq!(payload(i.into()), pos);
        }
    }
}
