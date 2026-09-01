mod index;
mod store; 

#[cfg(test)]
mod test_util;

#[derive(Debug, Clone)]
pub struct Config {
    pub inital_offset: u64,
    pub sync_writes: bool,
    pub max_index_bytes: u64,
    pub max_store_bytes: u64,
}

impl Config {
    pub fn stub() -> Config {
        Config {
            inital_offset: 0,
            sync_writes: true,
            max_store_bytes: 1_048_576, // 1mb
            max_index_bytes: index::ENTIRE_WIDTH * 87331, // 1mb
        }
    }
}
