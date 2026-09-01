use std::{
    env::temp_dir,
    fs::{self, File},
    io,
    path::PathBuf,
};

pub fn temp_file(extension: &str) -> (File, PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let file_name = temp_dir().join(nanos.to_string() + extension);

    let file = match File::create_new(&file_name) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(&file_name).unwrap();
            File::create_new(&file_name).unwrap()
        }
        Err(error) => panic!("{error}"),
    };

    (file, file_name)
}
