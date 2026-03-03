use super::*;

const DATA_PATH_LOCK_FILE_NAME: &str = ".tsink.lock";

#[derive(Debug)]
pub(super) struct DataPathProcessLock {
    _lock_path: PathBuf,
    lock_file: std::fs::File,
}

impl DataPathProcessLock {
    pub(super) fn acquire(data_path: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_path)?;
        let lock_path = data_path.join(DATA_PATH_LOCK_FILE_NAME);
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| TsinkError::IoWithPath {
                path: lock_path.clone(),
                source,
            })?;

        if let Err(source) = lock_file.try_lock() {
            return match source {
                std::fs::TryLockError::WouldBlock => {
                    Err(TsinkError::InvalidConfiguration(format!(
                        "data path {} is already locked by another tsink process ({})",
                        data_path.display(),
                        lock_path.display()
                    )))
                }
                std::fs::TryLockError::Error(source) => Err(TsinkError::IoWithPath {
                    path: lock_path.clone(),
                    source,
                }),
            };
        }

        Ok(Self {
            _lock_path: lock_path,
            lock_file,
        })
    }
}

impl Drop for DataPathProcessLock {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}
