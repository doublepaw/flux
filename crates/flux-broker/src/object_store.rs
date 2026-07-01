// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Object storage abstraction for S3 and local filesystem.

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use thiserror::Error;

/// Error type for object store operations.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found: {key}")]
    NotFound { key: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("s3 error: {message}")]
    S3 { message: String },

    #[error("invalid range: start={start}, len={len}, object_size={object_size}")]
    InvalidRange {
        start: u64,
        len: u64,
        object_size: u64,
    },
}

/// Object storage abstraction trait.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Put an object with the given key and data.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError>;

    /// Get an entire object by key.
    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError>;

    /// Get a byte range from an object.
    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Bytes, ObjectStoreError>;

    /// Delete an object by key.
    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;

    /// List objects with the given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;

    /// Get the size of an object.
    async fn size(&self, key: &str) -> Result<u64, ObjectStoreError>;
}

/// Local filesystem-based object store for testing.
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    /// Create a new local filesystem store with the given root directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        let path = self.key_path(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, &data).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let path = self.key_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Bytes::from(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(ObjectStoreError::NotFound {
                key: key.to_string(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Bytes, ObjectStoreError> {
        let path = self.key_path(key);
        let data = match tokio::fs::read(&path).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectStoreError::NotFound {
                    key: key.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        };

        let object_size = data.len() as u64;
        if start >= object_size || start + len > object_size {
            return Err(ObjectStoreError::InvalidRange {
                start,
                len,
                object_size,
            });
        }

        let start_usize = start as usize;
        let end_usize = start_usize + len as usize;
        Ok(Bytes::from(data[start_usize..end_usize].to_vec()))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.key_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(ObjectStoreError::NotFound {
                key: key.to_string(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut keys = Vec::new();
        let mut dirs_to_visit = vec![self.root.clone()];

        while let Some(dir) = dirs_to_visit.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;

                if metadata.is_dir() {
                    dirs_to_visit.push(path);
                } else if let Ok(rel_path) = path.strip_prefix(&self.root) {
                    let key = rel_path.to_string_lossy().to_string();
                    if key.starts_with(prefix) {
                        keys.push(key);
                    }
                }
            }
        }

        Ok(keys)
    }

    async fn size(&self, key: &str) -> Result<u64, ObjectStoreError> {
        let path = self.key_path(key);
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjectStoreError::NotFound {
                    key: key.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        };
        Ok(metadata.len())
    }
}

/// S3-based object store for production.
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3ObjectStore {
    /// Create a new S3 object store with the given bucket.
    pub async fn new(bucket: impl Into<String>) -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    /// Create a new S3 object store with a custom endpoint (for LocalStack).
    pub async fn new_with_endpoint(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .endpoint_url(&endpoint)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(s3_config);
        Self {
            client,
            bucket: bucket.into(),
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await
            .map_err(|e| ObjectStoreError::S3 {
                message: e.to_string(),
            })?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("NoSuchKey") {
                    ObjectStoreError::NotFound {
                        key: key.to_string(),
                    }
                } else {
                    ObjectStoreError::S3 {
                        message: e.to_string(),
                    }
                }
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| ObjectStoreError::S3 {
                message: e.to_string(),
            })?;

        Ok(data.into_bytes())
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Bytes, ObjectStoreError> {
        let range = format!("bytes={}-{}", start, start + len - 1);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("NoSuchKey") {
                    ObjectStoreError::NotFound {
                        key: key.to_string(),
                    }
                } else {
                    ObjectStoreError::S3 {
                        message: e.to_string(),
                    }
                }
            })?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| ObjectStoreError::S3 {
                message: e.to_string(),
            })?;

        Ok(data.into_bytes())
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ObjectStoreError::S3 {
                message: e.to_string(),
            })?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .send()
            .await
            .map_err(|e| ObjectStoreError::S3 {
                message: e.to_string(),
            })?;

        let keys = resp
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(String::from))
            .collect();

        Ok(keys)
    }

    async fn size(&self, key: &str) -> Result<u64, ObjectStoreError> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("NotFound") {
                    ObjectStoreError::NotFound {
                        key: key.to_string(),
                    }
                } else {
                    ObjectStoreError::S3 {
                        message: e.to_string(),
                    }
                }
            })?;

        Ok(resp.content_length().unwrap_or(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_fs_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        let data = Bytes::from("hello world");
        store.put("test/file.txt", data.clone()).await.unwrap();

        let retrieved = store.get("test/file.txt").await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_local_fs_get_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        let result = store.get("nonexistent.txt").await;
        assert!(matches!(result, Err(ObjectStoreError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_local_fs_get_range() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        let data = Bytes::from("hello world");
        store.put("test.txt", data).await.unwrap();

        // Get "world" (bytes 6-10)
        let range = store.get_range("test.txt", 6, 5).await.unwrap();
        assert_eq!(range.as_ref(), b"world");
    }

    #[tokio::test]
    async fn test_local_fs_get_range_invalid() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        let data = Bytes::from("hello");
        store.put("test.txt", data).await.unwrap();

        let result = store.get_range("test.txt", 10, 5).await;
        assert!(matches!(result, Err(ObjectStoreError::InvalidRange { .. })));
    }

    #[tokio::test]
    async fn test_local_fs_delete() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        store.put("test.txt", Bytes::from("data")).await.unwrap();
        store.delete("test.txt").await.unwrap();

        let result = store.get("test.txt").await;
        assert!(matches!(result, Err(ObjectStoreError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_local_fs_size() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        let data = Bytes::from("hello world");
        store.put("test.txt", data.clone()).await.unwrap();

        let size = store.size("test.txt").await.unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_local_fs_list() {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalFsStore::new(temp_dir.path());

        store.put("prefix/a.txt", Bytes::from("a")).await.unwrap();
        store.put("prefix/b.txt", Bytes::from("b")).await.unwrap();
        store.put("other/c.txt", Bytes::from("c")).await.unwrap();

        let keys = store.list("prefix/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|k| k.starts_with("prefix/")));
    }
}
