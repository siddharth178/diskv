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
}

pub struct Diskv {
    options: Options,

    // we don't want to lock any data as such. we just want to make access to diskv methods
    // synchronous
    lock: sync::RwLock<bool>,
}

impl fmt::Display for Diskv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "base path: {}", self.options.base_path)?;
        writeln!(f, "locked: {:?}", self.lock)
    }
}

impl Diskv {
    pub fn new(options: Options) -> DiskvResult<Diskv> {
        fs::create_dir_all(&options.base_path)?;
        Ok(Diskv {
            options: options,
            lock: sync::RwLock::new(true),
        })
    }

    pub fn put(&self, key: String, val: Vec<u8>) -> Result<(), DiskvError> {
        let _ = self.lock.write().unwrap();
        Ok(fs::write(
            path::Path::new(&self.options.base_path).join(key),
            val,
        )?)
    }

    pub fn get(&self, key: String) -> Result<Option<Vec<u8>>, DiskvError> {
        let _ = self.lock.read().unwrap();
        match fs::read(path::Path::new(&self.options.base_path).join(key)) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(DiskvError::IOError(e))
                }
            }
        }
    }

    pub fn delete(&self, key: String) -> Result<(), DiskvError> {
        let _ = self.lock.write().unwrap();
        match fs::remove_file(path::Path::new(&self.options.base_path).join(key)) {
            Ok(_) => Ok(()),
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
