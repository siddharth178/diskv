use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs;
use std::io;
use std::path;
use std::sync;

// ref: https://doc.rust-lang.org/stable/rust-by-example/error/multiple_error_types/wrap_error.html
type DiskvResult<T> = Result<T, DiskvError>;

#[derive(Debug)]
pub enum DiskvError {
    IOError(io::Error),
}

impl fmt::Display for DiskvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // this is wrapper so defer to underlying type's impl of fmt
            DiskvError::IOError(e) => e.fmt(f),
        }
    }
}

impl error::Error for DiskvError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            DiskvError::IOError(e) => Some(e),
        }
    }
}

impl From<io::Error> for DiskvError {
    fn from(e: io::Error) -> DiskvError {
        DiskvError::IOError(e)
    }
}

pub struct Options {
    pub base_path: String,
    pub cache_size_max: u32,
}

#[derive(Debug)]
pub struct DiskvCache {
    cache: HashMap<String, Vec<u8>>,
    cache_size: u32,
    cache_size_max: u32,
}

impl DiskvCache {
    fn new(cache_size_max: u32) -> DiskvCache {
        DiskvCache {
            cache: HashMap::new(),
            cache_size: 0,
            cache_size_max: cache_size_max,
        }
    }

    fn put(&mut self, key: String, val: Vec<u8>) {
        let val_len = val.len() as u32;
        if self.cache_size + val_len > self.cache_size_max {
            eprintln!("\t ==> cache full, no more caching");
        } else {
            self.cache.insert(key, val);
            self.cache_size += val_len;
            eprintln!("\t ==> cached. cache_size: {}", self.cache_size);
        }
    }

    fn get(&self, key: &String) -> Option<Vec<u8>> {
        match self.cache.get(key) {
            Some(v) => {
                eprintln!("\t ==> cach hit. key: {}", key);
                Some(v.to_vec())
            }
            None => {
                eprintln!("\t ==> cach miss. key: {}", key);
                None
            }
        }
    }

    fn delete(&mut self, key: &String) {
        match self.cache.remove_entry(key) {
            Some(v) => {
                eprintln!("\t ==> cached. cache_size: {}", self.cache_size);
                self.cache_size -= v.1.len() as u32
            }
            None => return,
        }
    }
}

pub struct Diskv {
    options: Options,
    cache: sync::RwLock<DiskvCache>,
}

impl fmt::Display for Diskv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "base path: {}", self.options.base_path)?;
        writeln!(f, "locked: {:?}", self.cache)
    }
}

impl Diskv {
    pub fn new(options: Options) -> DiskvResult<Diskv> {
        fs::create_dir_all(&options.base_path)?;
        let cache_size_max = options.cache_size_max;
        Ok(Diskv {
            options: options,
            cache: sync::RwLock::new(DiskvCache::new(cache_size_max)),
        })
    }

    pub fn put(&self, key: String, val: Vec<u8>) -> Result<(), DiskvError> {
        let cache_key = key.clone();
        let cache_val = val.clone();
        let mut cache = self.cache.write().unwrap();
        match fs::write(path::Path::new(&self.options.base_path).join(key), val) {
            Ok(_) => Ok(cache.put(cache_key, cache_val)),
            Err(e) => Err(DiskvError::IOError(e)),
        }
    }

    pub fn get(&self, key: String) -> Result<Option<Vec<u8>>, DiskvError> {
        let cache = self.cache.read().unwrap();
        match cache.get(&key) {
            Some(v) => Ok(Some(v.to_vec())),
            None => match fs::read(path::Path::new(&self.options.base_path).join(key)) {
                Ok(v) => Ok(Some(v)),
                Err(e) => {
                    if e.kind() == io::ErrorKind::NotFound {
                        Ok(None)
                    } else {
                        Err(DiskvError::IOError(e))
                    }
                }
            },
        }
    }

    pub fn delete(&self, key: String) -> Result<(), DiskvError> {
        let mut cache = self.cache.write().unwrap();
        let cache_key = key.clone();
        match fs::remove_file(path::Path::new(&self.options.base_path).join(key)) {
            Ok(_) => Ok(cache.delete(&cache_key)),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(DiskvError::IOError(e))
                }
            }
        }
    }
}
