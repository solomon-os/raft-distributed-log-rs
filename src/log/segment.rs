use std::{
    fs::{self, File},
    io::{self, Result},
    path::PathBuf,
};

use crate::log::{index, store};

struct Config {
    max_index_bytes: u64,
    max_store_bytes: u64,
    sync_writes: bool,
}

struct Segment {
    cfg: Config,
    index: index::Index,
    store: store::Store,
    next_offset: u64,
    base_offset: u64,
}

impl Segment {
    pub fn new(cfg: Config, base_offset: u64, data_dir: PathBuf) -> io::Result<Self> {
        let store_file = File::options()
            .write(true)
            .read(true)
            .open(data_dir.join(format!("{base_offset}.store")))?;
        let index_file = File::options()
            .write(true)
            .read(true)
            .open(data_dir.join(format!("{base_offset}.index")))?;

        let store = store::Store::new(
            store_file,
            store::Config {
                sync_writes: cfg.sync_writes,
            },
        )?;
        let index = index::Index::new(
            index_file,
            index::Config {
                max_size_bytes: cfg.max_index_bytes,
            },
        )?;

        Ok(Self {
            cfg,
            index,
            store,
            base_offset,
            next_offset: base_offset,
        })
    }

    // Append appends the data to the log and returns the offset
    pub fn append(&mut self, record: &[u8]) -> io::Result<u64> {
        let cur = self.next_offset;
        let pos = self.store.len();
        self.store.append(record)?;
        self.index
            .write((self.next_offset - self.base_offset) as u32, pos)?;
        self.next_offset += 1;

        Ok(cur)
    }

    pub fn read(&mut self, off: u64, buf: &mut Vec<u8>) -> io::Result<()> {
        if off < self.base_offset || off >= self.next_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset out of range",
            ));
        }

        let (_, pos) = self.index.read((off - self.base_offset) as u32)?;
        self.store.read(pos, buf)?;
        Ok(())
    }

    pub fn is_maxed(&self) -> bool {
        self.index.len() >= self.cfg.max_index_bytes || self.store.len() >= self.cfg.max_store_bytes
    }

    pub fn remove(&mut self) -> io::Result<()> {
        self.store.close()?;
        self.index.close()?;
        fs::remove_file(self.store.name()?)?;
        fs::remove_file(self.index.name()?)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{self, File},
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("segment-test-{nanos}-{count}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn create_segment_files(dir: &PathBuf, base_offset: u64) {
        File::create(dir.join(format!("{base_offset}.store"))).unwrap();
        File::create(dir.join(format!("{base_offset}.index"))).unwrap();
    }

    fn config(max_index_bytes: u64, max_store_bytes: u64) -> Config {
        Config {
            max_index_bytes,
            max_store_bytes,
            sync_writes: false,
        }
    }

    #[test]
    fn new_initializes_offsets_and_opens_files() {
        let dir = temp_dir();
        let base_offset = 16;
        create_segment_files(&dir, base_offset);

        let segment = Segment::new(config(1024, 1024), base_offset, dir.clone()).unwrap();

        assert_eq!(segment.base_offset, base_offset);
        assert!(dir.join(format!("{base_offset}.store")).exists());
        assert!(dir.join(format!("{base_offset}.index")).exists());
    }

    #[test]
    fn append_returns_incrementing_offsets() {
        let dir = temp_dir();
        let base_offset = 0;
        create_segment_files(&dir, base_offset);

        let mut segment = Segment::new(config(1024, 1024), base_offset, dir).unwrap();

        assert_eq!(segment.append(b"first").unwrap(), base_offset);
        assert_eq!(segment.append(b"second").unwrap(), base_offset + 1);
        assert_eq!(segment.append(b"third").unwrap(), base_offset + 2);
        assert_eq!(segment.next_offset, base_offset + 3);
    }

    #[test]
    fn append_and_read_round_trips_records() {
        let dir = temp_dir();
        let base_offset = 10;
        create_segment_files(&dir, base_offset);

        let mut segment = Segment::new(config(1024, 1024), base_offset, dir).unwrap();

        let records: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        let mut offsets = Vec::new();
        for &record in &records {
            offsets.push(segment.append(record).unwrap());
        }

        for (i, &expected) in records.iter().enumerate() {
            let mut buf = Vec::new();
            segment.read(offsets[i], &mut buf).unwrap();
            assert_eq!(buf, expected);
        }
    }

    #[test]
    fn read_below_base_offset_errors() {
        let dir = temp_dir();
        let base_offset = 5;
        create_segment_files(&dir, base_offset);

        let mut segment = Segment::new(config(1024, 1024), base_offset, dir).unwrap();
        segment.append(b"data").unwrap();

        assert!(segment.read(base_offset - 1, &mut Vec::new()).is_err());
    }

    #[test]
    fn read_above_written_offset_errors() {
        let dir = temp_dir();
        let base_offset = 0;
        create_segment_files(&dir, base_offset);

        let mut segment = Segment::new(config(1024, 1024), base_offset, dir).unwrap();
        segment.append(b"only one").unwrap();

        assert!(segment.read(base_offset + 1, &mut Vec::new()).is_err());
    }

    #[test]
    fn is_maxed_when_index_reaches_limit() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(index::ENTIRE_WIDTH, 1024), 0, dir).unwrap();
        assert!(!segment.is_maxed());

        segment.append(b"fills the only index slot").unwrap();
        assert!(segment.is_maxed());
    }

    #[test]
    fn is_maxed_when_store_reaches_limit() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        // One record of 12 bytes + 8 byte length prefix exactly hits the 20 byte limit.
        let mut segment = Segment::new(config(1024, 20), 0, dir).unwrap();
        assert!(!segment.is_maxed());

        let buf = [0u8; 12];

        segment.append(&buf).unwrap();
        assert!(segment.is_maxed());
    }

    #[test]
    fn remove_deletes_store_and_index_files() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 1024), 0, dir).unwrap();
        segment.append(b"persisted").unwrap();

        let store_path = PathBuf::from(segment.store.name().unwrap());
        let index_path = segment.index.name().unwrap();
        assert!(store_path.exists());
        assert!(index_path.exists());

        segment.remove().unwrap();

        assert!(!store_path.exists());
        assert!(!index_path.exists());
    }

    #[test]
    fn read_unwritten_segment_errors() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 1024), 0, dir).unwrap();

        assert!(segment.read(0, &mut Vec::new()).is_err());
    }

    #[test]
    fn append_empty_record_round_trips() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 1024), 0, dir).unwrap();

        let off = segment.append(&[]).unwrap();
        let mut buf = Vec::new();
        segment.read(off, &mut buf).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn append_updates_store_and_index_sizes() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 1024), 0, dir).unwrap();

        assert_eq!(segment.store.len(), 0);
        assert_eq!(segment.index.len(), 0);

        segment.append(b"first").unwrap();
        assert_eq!(segment.store.len(), 8 + 5);
        assert_eq!(segment.index.len(), index::ENTIRE_WIDTH);

        segment.append(b"second").unwrap();
        assert_eq!(segment.store.len(), (8 + 5) + (8 + 6));
        assert_eq!(segment.index.len(), index::ENTIRE_WIDTH * 2);
    }

    #[test]
    fn append_past_index_limit_errors() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(index::ENTIRE_WIDTH, 1024), 0, dir).unwrap();
        segment.append(b"one").unwrap();
        assert!(segment.append(b"two").is_err());
    }

    #[test]
    fn append_beyond_store_limit_marks_segment_maxed() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 20), 0, dir).unwrap();
        assert!(!segment.is_maxed());

        // Exactly fills the configured store byte limit.
        segment.append(&[0u8; 12]).unwrap();
        assert!(segment.is_maxed());

        // The current implementation does not reject appends once maxed;
        // callers are expected to check is_maxed() before appending.
        let size_before = segment.store.len();
        segment.append(b"extra").unwrap();
        assert!(segment.store.len() > size_before);
    }

    #[test]
    fn is_not_maxed_when_under_both_limits() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(index::ENTIRE_WIDTH * 4, 1024), 0, dir).unwrap();
        segment.append(b"small").unwrap();
        assert!(!segment.is_maxed());
    }

    #[test]
    fn remove_empty_segment_deletes_files() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut segment = Segment::new(config(1024, 1024), 0, dir).unwrap();

        let store_path = PathBuf::from(segment.store.name().unwrap());
        let index_path = segment.index.name().unwrap();

        segment.remove().unwrap();

        assert!(!store_path.exists());
        assert!(!index_path.exists());
    }

    #[test]
    fn lifecycle_with_sync_writes_enabled() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);

        let mut cfg = config(1024, 1024);
        cfg.sync_writes = true;

        let mut segment = Segment::new(cfg, 0, dir).unwrap();

        let off = segment.append(b"synced").unwrap();
        let mut buf = Vec::new();
        segment.read(off, &mut buf).unwrap();
        assert_eq!(buf, b"synced");
    }
}
