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

//
// DiskvCache
//
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

    fn put(&mut self, key: &String, val: Vec<u8>) {
        let val_len = val.len() as u32;

        if val_len > self.cache_size_max {
            eprintln!(
                "cache max size: {}, val size: {}, ignored.",
                self.cache_size_max, val_len
            );
            return;
        }

        let existing_val_len = match self.cache.get_key_value(key) {
            Some(v) => v.1.len() as u32,
            None => 0,
        };

        if self.cache_size + (val_len - existing_val_len) > self.cache_size_max {
            eprintln!("\t ==> cache full, no more caching");
        } else {
            self.cache.insert(key.clone(), val);
            self.cache_size += val_len - existing_val_len;
            eprintln!("\t ==> cached. cache_size: {}", self.cache_size);
        }
    }

    fn get(&self, key: &String) -> Option<Vec<u8>> {
        match self.cache.get(key) {
            Some(v) => {
                eprintln!("\t ==> cache hit. key: {}", key);
                Some(v.to_vec())
            }
            None => {
                eprintln!("\t ==> cache miss. key: {}", key);
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

//
// Options
//
pub struct Options {
    pub base_path: String,
    pub cache_size_max: u32,
}

//
// Diskv
//
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

    pub fn put(&self, key: &String, val: Vec<u8>) -> Result<(), DiskvError> {
        let cache_val = val.clone();
        let mut cache = self.cache.write().unwrap();
        match fs::write(path::Path::new(&self.options.base_path).join(key), val) {
            Ok(_) => Ok(cache.put(key, cache_val)),
            Err(e) => Err(DiskvError::IOError(e)),
        }
    }

    fn try_get(&self, key: &String) -> Result<Option<Vec<u8>>, DiskvError> {
        let cache = self.cache.read().unwrap();
        match cache.get(&key) {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &String) -> Result<Option<Vec<u8>>, DiskvError> {
        match self.try_get(key) {
            Ok(v) => match v {
                Some(v) => Ok(Some(v)),
                None => match fs::read(path::Path::new(&self.options.base_path).join(&key)) {
                    Ok(v) => {
                        self.put(&key, v.clone())?;
                        Ok(Some(v))
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            Ok(None)
                        } else {
                            Err(DiskvError::IOError(e))
                        }
                    }
                },
            },
            Err(e) => Err(e),
        }
    }

    pub fn delete(&self, key: &String) -> Result<(), DiskvError> {
        let mut cache = self.cache.write().unwrap();
        match fs::remove_file(path::Path::new(&self.options.base_path).join(key)) {
            Ok(_) => Ok(cache.delete(key)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_get_put_get_put_get_delete_get() {
        let key = String::from("k1");

        let mut c = DiskvCache::new(10);
        assert_eq!(None, c.get(&key));

        c.put(&key, String::from("abcd").into_bytes());
        assert_eq!(Some(String::from("abcd").into_bytes()), c.get(&key));

        c.put(&key, String::from("pqrs").into_bytes());
        assert_eq!(Some(String::from("pqrs").into_bytes()), c.get(&key));

        c.delete(&key);
        assert_eq!(None, c.get(&key));
    }

    #[test]
    fn cache_key_overwrite_size_check() {
        let key1 = String::from("k1");
        let mut c = DiskvCache::new(10);
        assert_eq!(None, c.get(&key1));

        c.put(&key1, String::from("0123456789").into_bytes());
        assert_eq!(Some(String::from("0123456789").into_bytes()), c.get(&key1));

        c.put(&key1, String::from("9876543210").into_bytes());
        assert_eq!(Some(String::from("9876543210").into_bytes()), c.get(&key1));

        let key2 = String::from("k2");
        c.put(&key2, String::from("abcd").into_bytes());
        assert_eq!(None, c.get(&key2));
    }

    #[test]
    fn cache_ignore_large_vals() {
        let key = String::from("k1");
        let mut c = DiskvCache::new(10);
        assert_eq!(None, c.get(&key));

        c.put(&key, String::from("abcdpqrsxy").into_bytes()); // gets cached
        assert_eq!(Some(String::from("abcdpqrsxy").into_bytes()), c.get(&key));

        c.put(&key, String::from("abcdpqrsxyz").into_bytes()); // won't get cached
        assert_eq!(Some(String::from("abcdpqrsxy").into_bytes()), c.get(&key));
    }

    #[test]
    fn diskv_get_put_get() -> DiskvResult<()> {
        let test_data_path = String::from("test_data");
        let dkv = Diskv::new(Options {
            base_path: test_data_path.clone(),
            cache_size_max: 12,
        })
        .expect("failed to init diskv");

        let key1 = String::from("k1");
        let key2 = String::from("k2");

        dkv.put(&key2, String::from("aa").into_bytes())?;
        assert_eq!(
            String::from("aa").into_bytes(),
            dkv.get(&key2).unwrap().unwrap()
        );

        // get
        assert!(dkv.get(&key1).unwrap().is_none());

        // put get
        dkv.put(&key1, String::from("0123456789").into_bytes())?;
        assert_eq!(
            String::from("0123456789").into_bytes(),
            dkv.get(&key1).unwrap().unwrap()
        );

        // put get
        dkv.put(&key1, String::from("1111111111").into_bytes())?;
        assert_eq!(
            String::from("1111111111").into_bytes(),
            dkv.get(&key1).unwrap().unwrap()
        );

        // delete get
        dkv.delete(&key1)?;
        assert!(dkv.get(&key1).unwrap().is_none());

        assert_eq!(
            String::from("aa").into_bytes(),
            dkv.get(&key2).unwrap().unwrap()
        );

        fs::remove_dir_all(&test_data_path)?;
        Ok(())
    }
}
