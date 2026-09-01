use std::{
    env::temp_dir,
    fs::File,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn temp_file(extension: &str) -> (File, PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);

    let file_name = temp_dir().join(format!("{nanos}-{count}{extension}"));
    let file = File::create_new(&file_name).unwrap();

    (file, file_name)
}
