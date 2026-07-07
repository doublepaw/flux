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
    part_size: usize,
    part_concurrency: usize,
}

impl S3ObjectStore {
    /// Create a new S3 object store with the given bucket.
    pub async fn new(bucket: impl Into<String>) -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        Self {
            client,
            bucket: bucket.into(),
            part_size: multipart_part_size(),
            part_concurrency: multipart_concurrency(),
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
            part_size: multipart_part_size(),
            part_concurrency: multipart_concurrency(),
        }
    }
}

/// Objects at or above one part size upload as concurrent multipart parts;
/// a single PUT is limited to one HTTP stream (~60-90 MB/s on S3). Part size
/// bounds the ack-latency floor (one part = one HTTP stream), so it is
/// env-tunable; S3 requires parts >= 5 MB.
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 8;
const S3_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

fn multipart_part_size() -> usize {
    std::env::var("S3_MULTIPART_PART_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(MULTIPART_PART_SIZE)
        .max(S3_MIN_PART_SIZE)
}

fn multipart_concurrency() -> usize {
    std::env::var("S3_MULTIPART_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(MULTIPART_CONCURRENCY)
        .max(1)
}

impl S3ObjectStore {
    async fn put_multipart(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        use futures::StreamExt;

        let s3_err = |e: String| ObjectStoreError::S3 { message: e };

        let upload = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| s3_err(format!("create multipart: {e}")))?;
        let upload_id = upload
            .upload_id()
            .ok_or_else(|| s3_err("missing upload id".into()))?
            .to_string();

        let chunks: Vec<(i32, Bytes)> = (0..data.len())
            .step_by(self.part_size)
            .enumerate()
            .map(|(i, start)| {
                let end = (start + self.part_size).min(data.len());
                (i as i32 + 1, data.slice(start..end))
            })
            .collect();

        let results: Vec<Result<aws_sdk_s3::types::CompletedPart, String>> =
            futures::stream::iter(chunks.into_iter().map(|(part_number, chunk)| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let key = key.to_string();
                let upload_id = upload_id.clone();
                async move {
                    let part = client
                        .upload_part()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .part_number(part_number)
                        .body(chunk.into())
                        .send()
                        .await
                        .map_err(|e| format!("upload part {part_number}: {e}"))?;
                    Ok(aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(part_number)
                        .set_e_tag(part.e_tag().map(str::to_string))
                        .build())
                }
            }))
            .buffer_unordered(self.part_concurrency)
            .collect()
            .await;

        let mut parts = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(part) => parts.push(part),
                Err(message) => {
                    let _ = self
                        .client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send()
                        .await;
                    return Err(s3_err(message));
                }
            }
        }
        parts.sort_by_key(|p| p.part_number());

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| s3_err(format!("complete multipart: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        if data.len() >= self.part_size {
            return self.put_multipart(key, data).await;
        }
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

/// Adapter over the `object_store` crate, used for GCS and Azure Blob.
///
/// AWS keeps the native `S3ObjectStore`; this adapter exists so one impl
/// covers the remaining clouds. Credentials come from each backend's
/// standard environment (metadata server / workload identity / env vars).
pub struct CloudObjectStore {
    inner: Box<dyn object_store::ObjectStore>,
    part_size: usize,
    part_concurrency: usize,
}

impl CloudObjectStore {
    /// GCS-backed store. Uses `GOOGLE_APPLICATION_CREDENTIALS`,
    /// `GOOGLE_SERVICE_ACCOUNT*` env vars, or the GCE/GKE metadata server.
    pub fn gcs(bucket: &str) -> Result<Self, ObjectStoreError> {
        let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| ObjectStoreError::S3 {
                message: format!("gcs init: {e}"),
            })?;
        Ok(Self {
            inner: Box::new(store),
            part_size: multipart_part_size(),
            part_concurrency: multipart_concurrency(),
        })
    }

    /// Azure Blob-backed store. Uses `AZURE_STORAGE_ACCOUNT_NAME` +
    /// `AZURE_STORAGE_ACCOUNT_KEY` env vars or workload identity.
    pub fn azure(container: &str) -> Result<Self, ObjectStoreError> {
        let store = object_store::azure::MicrosoftAzureBuilder::from_env()
            .with_container_name(container)
            .build()
            .map_err(|e| ObjectStoreError::S3 {
                message: format!("azure init: {e}"),
            })?;
        Ok(Self {
            inner: Box::new(store),
            part_size: multipart_part_size(),
            part_concurrency: multipart_concurrency(),
        })
    }

    fn map_err(key: &str, e: object_store::Error) -> ObjectStoreError {
        match e {
            object_store::Error::NotFound { .. } => ObjectStoreError::NotFound {
                key: key.to_string(),
            },
            other => ObjectStoreError::S3 {
                message: other.to_string(),
            },
        }
    }
}

#[async_trait]
impl ObjectStore for CloudObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ObjectStoreError> {
        let path = object_store::path::Path::from(key);
        if data.len() < self.part_size {
            self.inner
                .put(&path, data.into())
                .await
                .map_err(|e| Self::map_err(key, e))?;
            return Ok(());
        }
        // Parallel multipart: a single upload stream tops out at ~50-90 MB/s
        // on GCS/Azure, same as S3; concurrent parts are the throughput and
        // ack-latency lever (same knobs as the native S3 store).
        let upload = self
            .inner
            .put_multipart(&path)
            .await
            .map_err(|e| Self::map_err(key, e))?;
        let mut writer =
            object_store::WriteMultipart::new_with_chunk_size(upload, self.part_size);
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + self.part_size).min(data.len());
            writer
                .wait_for_capacity(self.part_concurrency)
                .await
                .map_err(|e| Self::map_err(key, e))?;
            writer.put(data.slice(offset..end));
            offset = end;
        }
        writer.finish().await.map_err(|e| Self::map_err(key, e))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        let path = object_store::path::Path::from(key);
        let result = self
            .inner
            .get(&path)
            .await
            .map_err(|e| Self::map_err(key, e))?;
        result.bytes().await.map_err(|e| Self::map_err(key, e))
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Bytes, ObjectStoreError> {
        let path = object_store::path::Path::from(key);
        let range = (start as usize)..((start + len) as usize);
        self.inner
            .get_range(&path, range)
            .await
            .map_err(|e| Self::map_err(key, e))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = object_store::path::Path::from(key);
        self.inner
            .delete(&path)
            .await
            .map_err(|e| Self::map_err(key, e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        use futures::TryStreamExt;
        let path = object_store::path::Path::from(prefix);
        let entries: Vec<_> = self
            .inner
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(|e| Self::map_err(prefix, e))?;
        Ok(entries
            .into_iter()
            .map(|m| m.location.to_string())
            .collect())
    }

    async fn size(&self, key: &str) -> Result<u64, ObjectStoreError> {
        let path = object_store::path::Path::from(key);
        let meta = self
            .inner
            .head(&path)
            .await
            .map_err(|e| Self::map_err(key, e))?;
        Ok(meta.size as u64)
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
