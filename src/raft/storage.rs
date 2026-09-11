use crate::raft::types::{NodeId, State};
use std::{
    fs::File,
    io::{self, BufWriter, Read, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
};

pub struct Storage {
    dir: PathBuf,
    file: File,
}

impl Storage {
    pub fn new(dir: PathBuf) -> Self {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("state.metadata"))
            .expect("metadata file for raft could not be created");
        Self { dir, file }
    }
    pub fn load_state(&mut self) -> State {
        if self
            .file
            .metadata()
            .expect("loading file metadata failed")
            .len()
            == 0
        {
            return State {
                current_term: 0,
                voted_for: None,
            };
        }

        let mut term_buf = [0u8; 8];
        self.file
            .read_exact_at(&mut term_buf, 0)
            .expect("reading metadata file fails");
        let current_term = u64::from_be_bytes(term_buf);

        let mut voted_len_buf = [0u8; 8];
        self.file
            .read_exact_at(&mut voted_len_buf, 8)
            .expect("reading metadata file fails");

        let voted_len = u64::from_be_bytes(voted_len_buf);
        let mut voted_for = None;

        if voted_len != 0 {
            let mut voted_for_buf = vec![0u8; voted_len as usize];
            self.file
                .read_exact_at(&mut voted_for_buf, 16)
                .expect("reading voted_for from metadata fails");
            let voted_for_string = String::from_utf8(voted_for_buf)
                .expect("conversion of voted_for metadata into string fails");
            voted_for = Some(NodeId(voted_for_string));
        }

        State {
            current_term,
            voted_for,
        }
    }

    pub fn save_state(&mut self, state: &State) -> io::Result<()> {
        let current_term_bytes = state.current_term.to_be_bytes();
        self.file.write_all_at(&current_term_bytes, 0)?;
        match &state.voted_for {
            Some(voted_for) => {
                let voted_for_string = &voted_for.0;
                let size = voted_for_string.len() as u64;
                self.file.write_all_at(&size.to_be_bytes(), 8)?;
                self.file.write_all_at(voted_for_string.as_bytes(), 16)?;
                self.file.set_len(16 + size)?;
                self.file.sync_all()?;
                Ok(())
            }
            None => {
                let size: u64 = 0;
                self.file.write_all_at(&size.to_be_bytes(), 8)?;
                self.file.set_len(16 + size)?;
                self.file.sync_all()?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::*;

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "raft-storage-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }

        fn metadata_path(&self) -> PathBuf {
            self.path.join("state.metadata")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn state(current_term: u64, voted_for: Option<&str>) -> State {
        State {
            current_term,
            voted_for: voted_for.map(|node_id| NodeId(node_id.to_owned())),
        }
    }

    fn assert_state_eq(actual: &State, expected: &State) {
        assert_eq!(actual.current_term, expected.current_term);
        assert_eq!(actual.voted_for, expected.voted_for);
    }

    #[test]
    fn new_creates_the_metadata_file() {
        let directory = TestDirectory::new();

        let storage = Storage::new(directory.path().to_owned());

        assert_eq!(storage.dir, directory.path());
        assert!(directory.metadata_path().is_file());
    }

    #[test]
    fn new_storage_loads_the_default_hard_state() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());

        let loaded = storage.load_state();

        assert_state_eq(&loaded, &state(0, None));
    }

    #[test]
    fn saves_and_loads_a_term_without_a_vote() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        let expected = state(7, None);

        storage
            .save_state(&expected)
            .expect("hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
    }

    #[test]
    fn saves_and_loads_a_vote() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        let expected = state(11, Some("node-2"));

        storage
            .save_state(&expected)
            .expect("hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
    }

    #[test]
    fn saved_state_survives_reopening_storage() {
        let directory = TestDirectory::new();
        let expected = state(19, Some("node-3"));

        {
            let mut storage = Storage::new(directory.path().to_owned());
            storage
                .save_state(&expected)
                .expect("hard state should be saved");
        }

        let mut reopened = Storage::new(directory.path().to_owned());
        let loaded = reopened.load_state();

        assert_state_eq(&loaded, &expected);
    }

    #[test]
    fn overwriting_a_vote_with_none_clears_it() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        storage
            .save_state(&state(3, Some("previous-candidate")))
            .expect("initial hard state should be saved");
        let expected = state(4, None);

        storage
            .save_state(&expected)
            .expect("replacement hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
        assert_eq!(fs::metadata(directory.metadata_path()).unwrap().len(), 16);
    }

    #[test]
    fn overwriting_a_long_vote_with_a_shorter_vote_removes_trailing_bytes() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        storage
            .save_state(&state(8, Some("a-very-long-candidate-id")))
            .expect("initial hard state should be saved");
        let expected = state(9, Some("b"));

        storage
            .save_state(&expected)
            .expect("replacement hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
        assert_eq!(fs::metadata(directory.metadata_path()).unwrap().len(), 17);
    }

    #[test]
    fn round_trips_a_unicode_node_id() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        let expected = state(21, Some("nódë-三"));

        storage
            .save_state(&expected)
            .expect("hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
    }

    #[test]
    fn round_trips_the_largest_term() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        let expected = state(u64::MAX, Some("node-max"));

        storage
            .save_state(&expected)
            .expect("hard state should be saved");
        let loaded = storage.load_state();

        assert_state_eq(&loaded, &expected);
    }

    #[test]
    fn writes_the_documented_binary_layout() {
        let directory = TestDirectory::new();
        let mut storage = Storage::new(directory.path().to_owned());
        storage
            .save_state(&state(0x0102_0304_0506_0708, Some("abc")))
            .expect("hard state should be saved");

        let bytes = fs::read(directory.metadata_path()).expect("metadata should be readable");

        assert_eq!(
            bytes,
            vec![
                1, 2, 3, 4, 5, 6, 7, 8, // current term
                0, 0, 0, 0, 0, 0, 0, 3, // voted-for byte length
                b'a', b'b', b'c',
            ]
        );
    }

    #[test]
    fn separate_directories_do_not_share_state() {
        let first_directory = TestDirectory::new();
        let second_directory = TestDirectory::new();
        let mut first = Storage::new(first_directory.path().to_owned());
        let mut second = Storage::new(second_directory.path().to_owned());
        let first_state = state(2, Some("node-a"));
        let second_state = state(15, Some("node-b"));

        first
            .save_state(&first_state)
            .expect("first hard state should be saved");
        second
            .save_state(&second_state)
            .expect("second hard state should be saved");

        assert_state_eq(&first.load_state(), &first_state);
        assert_state_eq(&second.load_state(), &second_state);
    }
}
