use std::{fs, io, path::PathBuf, thread, time::Duration};

use crate::log::segment::Segment;

mod index;
mod segment;
mod store;

#[cfg(test)]
mod test_util;

#[derive(Debug, Clone)]
pub struct Config {
    pub inital_offset: u64,
    pub sync_writes: bool,
    pub max_size_bytes: u64,
    pub max_store_bytes: u64,
}

pub struct Log {
    active_index: usize,
    dir: PathBuf,
    cfg: Config,
    segments: Vec<Segment>,
}

impl Log {
    pub fn new(cfg: Config, dir: PathBuf) -> io::Result<Self> {
        if !dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "path is not a valid directory",
            ));
        }

        // walk through directories
        let mut offsets = Vec::with_capacity(5);
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.path().is_dir() {
                continue;
            }

            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(stem) = name
                .strip_suffix(".store")
                .or_else(|| name.strip_suffix(".index"))
            else {
                continue;
            };

            let Ok(off) = stem.parse::<u64>() else {
                continue;
            };

            if !offsets.contains(&off) {
                offsets.push(off);
            }
        }

        offsets.sort();

        let mut segments = Vec::with_capacity(offsets.len());

        for off in offsets {
            segments.push(Self::new_segment(&cfg, off, dir.clone())?);
        }

        if segments.is_empty() {
            segments.push(Self::new_segment(&cfg, cfg.inital_offset, dir.clone())?);
        }

        Ok(Self {
            active_index: segments.len() - 1,
            dir,
            cfg,
            segments,
        })
    }

    fn new_segment(cfg: &Config, off: u64, dir: PathBuf) -> io::Result<Segment> {
        let segment = Segment::new(
            segment::Config {
                max_store_bytes: cfg.max_store_bytes,
                max_index_bytes: cfg.max_size_bytes,
                sync_writes: cfg.sync_writes,
            },
            off,
            dir,
        )?;

        Ok(segment)
    }

    pub fn append(&mut self, buf: &[u8]) -> io::Result<u64> {
        let off = self.segments[self.active_index].append(buf)?;
        if self.segments[self.active_index].is_maxed() {
            self.segments
                .push(Self::new_segment(&self.cfg, off + 1, self.dir.clone())?);
            self.active_index += 1;
        }
        Ok(off)
    }

    pub fn read(&mut self, off: u64, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut cur = 0;
        while cur < self.segments.len() && !self.segments[cur].is_in_range(off) {
            cur += 1;
        }
        if cur >= self.segments.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of range",
            ));
        }
        self.segments[cur].read(off, buf)?;
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        for i in 0..=self.active_index {
            self.segments[i].close()?;
        }
        Ok(())
    }

    pub fn truncate(&mut self, off: u64) -> io::Result<()> {
        while let Some(segment) = self.segments.last() {
            if segment.is_lesser_than_range(off) {
                if let Some(mut segment) = self.segments.pop() {
                    segment.remove()?;
                    self.active_index = self.active_index.saturating_sub(1);
                } else {
                    println!("break called");
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn next_offset(&self) -> u64 {
        return self.segments[self.active_index].next_offset();
    }

    pub fn dir(&self) -> PathBuf {
        self.dir.clone()
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;
    use crate::log::test_util::{create_segment_files, temp_dir_with_prefix};
    use pretty_assertions::assert_eq;

    fn temp_dir() -> PathBuf {
        temp_dir_with_prefix("log-test")
    }

    fn config(max_index_bytes: u64, max_store_bytes: u64) -> Config {
        Config {
            inital_offset: 0,
            sync_writes: false,
            max_size_bytes: max_index_bytes,
            max_store_bytes,
        }
    }

    #[test]
    fn new_creates_initial_segment_in_empty_dir() {
        let dir = temp_dir();
        let log = Log::new(config(1024, 1024), dir.clone()).unwrap();

        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.active_index, 0);
        assert!(dir.join("0.store").exists());
        assert!(dir.join("0.index").exists());
    }

    #[test]
    fn new_loads_existing_segments() {
        let dir = temp_dir();
        create_segment_files(&dir, 0);
        create_segment_files(&dir, 16);
        create_segment_files(&dir, 32);

        let log = Log::new(config(1024, 1024), dir).unwrap();

        assert_eq!(log.segments.len(), 3);
        assert_eq!(log.active_index, 2);
        assert_eq!(log.segments[0].base_offset(), 0);
        assert_eq!(log.segments[1].base_offset(), 16);
        assert_eq!(log.segments[2].base_offset(), 32);
    }

    #[test]
    fn append_and_read_single_record() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        let off = log.append(b"hello").unwrap();
        assert_eq!(off, 0);

        let mut buf = Vec::new();
        log.read(off, &mut buf).unwrap();
        assert_eq!(buf, b"hello");
    }

    #[test]
    fn append_returns_incrementing_offsets() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        assert_eq!(log.append(b"a").unwrap(), 0);
        assert_eq!(log.append(b"b").unwrap(), 1);
        assert_eq!(log.append(b"c").unwrap(), 2);
    }

    #[test]
    fn append_rotates_to_new_segment_when_maxed() {
        let dir = temp_dir();
        // Each segment can hold exactly one index entry.
        let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir.clone()).unwrap();

        let off0 = log.append(b"first").unwrap();
        assert_eq!(off0, 0);
        // Segment 0 is now maxed, so a new empty segment 1 is created eagerly.
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.active_index, 1);

        let off1 = log.append(b"second").unwrap();
        assert_eq!(off1, 1);
        // Segment 1 is now maxed, so a new empty segment 2 is created eagerly.
        assert_eq!(log.segments.len(), 3);
        assert_eq!(log.active_index, 2);

        // New segment file should have been created.
        assert!(dir.join("2.store").exists());
        assert!(dir.join("2.index").exists());
    }

    #[test]
    fn read_across_segments() {
        let dir = temp_dir();
        let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir).unwrap();

        let mut offsets = Vec::new();
        for i in 0..4 {
            offsets.push(log.append(format!("record-{i}").as_bytes()).unwrap());
        }

        for (i, off) in offsets.iter().enumerate() {
            let mut buf = Vec::new();
            log.read(*off, &mut buf).unwrap();
            assert_eq!(buf, format!("record-{i}").into_bytes());
        }
    }

    #[test]
    fn read_out_of_range_errors() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        log.append(b"only one").unwrap();

        assert!(log.read(1, &mut Vec::new()).is_err());
    }

    #[test]
    fn close_closes_all_segments() {
        let dir = temp_dir();
        let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir).unwrap();

        log.append(b"one").unwrap();
        log.append(b"two").unwrap();

        log.close().unwrap();
    }

    #[test]
    fn truncate_removes_trailing_segments() {
        let dir = temp_dir();
        let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir.clone()).unwrap();

        let off0 = log.append(b"zero").unwrap();
        let _off1 = log.append(b"one").unwrap();
        let _off2 = log.append(b"two").unwrap();

        // Eager rotation creates an extra empty segment after each maxed append.
        assert_eq!(log.segments.len(), 4);
        assert_eq!(log.active_index, 3);

        // Truncate at the boundary between segment 0 and segment 1.
        log.truncate(off0 + 1).unwrap();

        // Segments with base_offset > 1 should be removed.
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.active_index, 1);
        assert!(!dir.join("2.store").exists());
        assert!(!dir.join("2.index").exists());
        assert!(!dir.join("3.store").exists());
        assert!(!dir.join("3.index").exists());
    }

    #[test]
    fn reopen_loads_all_segments() {
        let dir = temp_dir();
        let mut offsets = Vec::new();

        {
            let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir.clone()).unwrap();
            for i in 0..3 {
                offsets.push(log.append(format!("rec-{i}").as_bytes()).unwrap());
            }
            log.close().unwrap();
        }

        {
            let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir).unwrap();
            // Eager rotation left an empty trailing segment, so 4 files/segments.
            assert_eq!(log.segments.len(), 4);

            for (i, off) in offsets.iter().enumerate() {
                let mut buf = Vec::new();
                log.read(*off, &mut buf)
                    .expect(&format!("failed to read offset {} (record rec-{})", off, i));
                assert_eq!(buf, format!("rec-{i}").into_bytes());
            }
        }
    }

    #[test]
    fn new_errors_on_missing_directory() {
        let dir = temp_dir().join("does-not-exist");
        assert!(Log::new(config(1024, 1024), dir).is_err());
    }

    #[test]
    fn truncate_at_offset_beyond_all_segments_is_noop() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        log.append(b"zero").unwrap();
        log.append(b"one").unwrap();

        let segments_before = log.segments.len();
        log.truncate(100).unwrap();

        assert_eq!(log.segments.len(), segments_before);
    }

    #[test]
    fn append_after_close_does_not_error() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        log.append(b"one").unwrap();
        log.close().unwrap();

        // The current implementation does not reject writes to a closed
        // log; callers are expected not to append after close().
        let off = log.append(b"two").unwrap();
        assert_eq!(off, 1);
    }

    #[test]
    fn truncate_to_zero_retains_only_first_segment() {
        let dir = temp_dir();
        let mut log = Log::new(config(index::ENTIRE_WIDTH, 1024), dir).unwrap();

        log.append(b"zero").unwrap();
        log.append(b"one").unwrap();
        log.append(b"two").unwrap();

        // Eager rotation creates an extra empty segment after each maxed append.
        assert_eq!(log.segments.len(), 4);

        log.truncate(0).unwrap();

        // Only the segment containing offset 0 should remain; active_index
        // must not underflow while popping segments 3, 2 and 1.
        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.active_index, 0);
        assert_eq!(log.segments[0].base_offset(), 0);

        let mut buf = Vec::new();
        log.read(0, &mut buf).unwrap();
        assert_eq!(buf, b"zero");
    }

    #[test]
    fn read_on_empty_log_errors() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        assert!(log.read(0, &mut Vec::new()).is_err());
    }

    #[test]
    fn new_creates_missing_index_file_for_partial_segment() {
        let dir = temp_dir();
        // Only the .store file exists on disk; .index is missing.
        fs::File::create(dir.join("5.store")).unwrap();

        let log = Log::new(config(1024, 1024), dir.clone()).unwrap();

        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.segments[0].base_offset(), 5);
        assert!(dir.join("5.index").exists());
    }

    #[test]
    fn new_ignores_unrelated_and_non_numeric_files() {
        let dir = temp_dir();
        fs::File::create(dir.join("notes.txt")).unwrap();
        fs::File::create(dir.join("abc.store")).unwrap();
        fs::create_dir(dir.join("subdir")).unwrap();

        let log = Log::new(config(1024, 1024), dir).unwrap();

        // None of the stray entries produce a valid offset, so a fresh
        // initial segment at the configured initial offset is created.
        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.segments[0].base_offset(), 0);
    }

    #[test]
    fn append_and_read_multiple_records_within_single_segment() {
        let dir = temp_dir();
        let mut log = Log::new(config(1024, 1024), dir).unwrap();

        for i in 0..5 {
            log.append(format!("rec-{i}").as_bytes()).unwrap();
        }

        // All records fit comfortably under the 1024 byte limits, so no
        // rotation should have happened.
        assert_eq!(log.segments.len(), 1);
        assert_eq!(log.active_index, 0);

        for i in 0..5 {
            let mut buf = Vec::new();
            log.read(i, &mut buf).unwrap();
            assert_eq!(buf, format!("rec-{i}").into_bytes());
        }
    }
}
