use std::{fs, io, path::PathBuf};

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
                    break;
                }
            }
        }
        Ok(())
    }
}
