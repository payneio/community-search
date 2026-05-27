use crate::crawler::error::{CrawlError, CrawlResult};
use std::path::Path;

/// Returns the total disk usage in bytes of the directory tree rooted at `path`.
/// If `path` does not exist, returns `Ok(0)`.
pub fn index_dir_size_bytes(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    let mut total: u64 = 0;
    let mut stack: Vec<std::path::PathBuf> = vec![path.to_path_buf()];

    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }

    Ok(total)
}

/// Returns `Ok(())` if `used < max`, or `Err(CrawlError::IndexFull)` if `used >= max`.
pub fn check_capacity(used: u64, max: u64) -> CrawlResult<()> {
    if used >= max {
        Err(CrawlError::IndexFull { used, max })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn size_of_empty_dir_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(index_dir_size_bytes(dir.path()).unwrap(), 0);
    }

    #[test]
    fn size_sums_files_recursively() {
        let dir = tempfile::tempdir().unwrap();

        // Write 100 bytes to a.bin
        fs::write(dir.path().join("a.bin"), vec![0u8; 100]).unwrap();

        // Write 250 bytes to sub/b.bin
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("b.bin"), vec![0u8; 250]).unwrap();

        assert_eq!(index_dir_size_bytes(dir.path()).unwrap(), 350);
    }

    #[test]
    fn check_capacity_errors_when_full() {
        // used == max => full
        assert!(matches!(
            check_capacity(100, 100),
            Err(CrawlError::IndexFull {
                used: 100,
                max: 100
            })
        ));

        // used > max => full
        assert!(matches!(
            check_capacity(101, 100),
            Err(CrawlError::IndexFull {
                used: 101,
                max: 100
            })
        ));

        // used < max => ok
        assert!(check_capacity(99, 100).is_ok());
    }
}
