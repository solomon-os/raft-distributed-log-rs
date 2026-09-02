use std::{
    env::temp_dir,
    fs::{self, File},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn temp_file(extension: &str) -> (File, PathBuf) {
    let dir = temp_dir();
    let path = unique_path(&dir, extension);
    let file = File::create_new(&path).unwrap();
    (file, path)
}

pub fn temp_dir_with_prefix(prefix: &str) -> PathBuf {
    let dir = unique_path(&temp_dir(), prefix);
    fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn create_segment_files(dir: &PathBuf, base_offset: u64) {
    File::create(dir.join(format!("{base_offset}.store"))).unwrap();
    File::create(dir.join(format!("{base_offset}.index"))).unwrap();
}

fn unique_path(parent: &PathBuf, suffix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    parent.join(format!("{nanos}-{count}-{suffix}"))
}
