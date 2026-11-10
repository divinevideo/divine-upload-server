//! Provider-aware authoritative media storage. Public URLs never expose these references.
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use google_cloud_storage::{
    client::Client as GcsClient,
    http::objects::{
        delete::DeleteObjectRequest,
        download::Range,
        get::GetObjectRequest,
        upload::{UploadObjectRequest, UploadType},
        Object,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, str::FromStr};

mod b2;
mod primary;
pub use b2::B2Store;

#[cfg(test)]
mod b2_primary_tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WriteMode {
    #[default]
    Gcs,
    B2,
    Dual,
    #[serde(rename = "b2_primary")]
    B2Primary,
}
impl WriteMode {
    pub(crate) fn requires_both(self) -> bool {
        matches!(self, Self::Dual | Self::B2Primary)
    }
}
impl FromStr for WriteMode {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "gcs" => Ok(Self::Gcs),
            "b2" => Ok(Self::B2),
            "dual" => Ok(Self::Dual),
            "b2_primary" => Ok(Self::B2Primary),
            _ => bail!("MEDIA_WRITE_PROVIDER must be gcs, b2, dual, or b2_primary"),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageProvider {
    Gcs,
    B2,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub provider: StorageProvider,
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}
#[derive(Clone, Debug)]
pub struct StoredObject {
    pub reference: ObjectRef,
    pub size: u64,
    pub content_type: String,
    pub owner: Option<String>,
}
#[derive(Clone, Debug)]
pub struct WriteResult {
    pub objects: Vec<ObjectRef>,
    pub newly_stored: bool,
}

#[derive(Clone)]
pub struct MediaStorage {
    pub mode: WriteMode,
    gcs: Option<GcsClient>,
    gcs_bucket: String,
    b2: Option<B2Store>,
    database_url: std::sync::Arc<tokio::sync::Mutex<Option<String>>>,
}
impl MediaStorage {
    pub fn new(
        mode: WriteMode,
        gcs: Option<GcsClient>,
        gcs_bucket: String,
        b2: Option<B2Store>,
    ) -> Result<Self> {
        if mode != WriteMode::B2 && (gcs.is_none() || gcs_bucket.is_empty()) {
            bail!("GCS storage configuration required");
        }
        if mode != WriteMode::Gcs && b2.is_none() {
            bail!("B2 storage configuration required");
        }
        Ok(Self {
            mode,
            gcs,
            gcs_bucket,
            b2,
            database_url: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
    pub async fn initialize_coordination(&self, database_url: Option<&str>) -> Result<()> {
        if let Some(b2) = &self.b2 {
            if matches!(self.mode, WriteMode::B2Primary | WriteMode::Gcs) && database_url.is_none()
            {
                // New-mode multipart/admission receipts use the existing GCS bucket.
                return Ok(());
            }
            b2.initialize_coordination(
                database_url.ok_or_else(|| anyhow!("B2 coordination database required"))?,
            )
            .await?;
        }
        *self.database_url.lock().await = database_url.map(str::to_owned);
        Ok(())
    }
    pub async fn sweep_stale_multipart(&self, limit: u32) -> Result<u64> {
        match &self.b2 {
            Some(b2) => b2.sweep_stale_multipart(limit).await,
            None => Ok(0),
        }
    }
    /// A GCS rollback stops new B2 admissions without retargeting a recorded
    /// B2-primary direct retry or a derivative of an already accepted original.
    /// Outbox callers keep using their own historical intent resolution.
    pub(crate) async fn for_recorded_write(&self, key: &str) -> Result<Self> {
        if self.mode == WriteMode::Gcs && self.b2.is_some() && primary::exists(self, key).await? {
            self.with_mode(WriteMode::B2Primary)
        } else {
            Ok(self.clone())
        }
    }
    pub fn with_mode(&self, mode: WriteMode) -> Result<Self> {
        let mut storage = Self::new(
            mode,
            self.gcs.clone(),
            self.gcs_bucket.clone(),
            self.b2.clone(),
        )?;
        storage.database_url = self.database_url.clone();
        Ok(storage)
    }
    async fn write_guard(&self, key: &str) -> Result<Option<CanonicalWriteGuard>> {
        let Some(database_url) = self.database_url.lock().await.clone() else {
            return Ok(None);
        };
        let client = crate::coordination::connect(&database_url).await?;
        let name = format!("divine-upload-canonical:{key}");
        client
            .query_one("SELECT pg_advisory_lock(hashtextextended($1,0))", &[&name])
            .await
            .map_err(|_| anyhow!("Canonical write lock acquisition failed"))?;
        Ok(Some(CanonicalWriteGuard {
            client: Some(client),
            name,
        }))
    }
    pub fn gcs_bucket(&self) -> &str {
        &self.gcs_bucket
    }
    pub fn b2_bucket(&self) -> Option<&str> {
        self.b2.as_ref().map(B2Store::bucket)
    }
    fn gcs(&self) -> Result<&GcsClient> {
        self.gcs
            .as_ref()
            .ok_or_else(|| anyhow!("GCS storage unavailable"))
    }
    fn b2(&self) -> Result<&B2Store> {
        self.b2
            .as_ref()
            .ok_or_else(|| anyhow!("B2 storage unavailable"))
    }
    async fn gcs_stat(&self, key: &str) -> Result<Option<StoredObject>> {
        match self
            .gcs()?
            .get_object(&GetObjectRequest {
                bucket: self.gcs_bucket.clone(),
                object: key.into(),
                ..Default::default()
            })
            .await
        {
            Ok(o) => Ok(Some(StoredObject {
                reference: ObjectRef {
                    provider: StorageProvider::Gcs,
                    bucket: self.gcs_bucket.clone(),
                    key: key.into(),
                    version: Some(o.generation.to_string()),
                },
                size: o.size as u64,
                content_type: o
                    .content_type
                    .unwrap_or_else(|| "application/octet-stream".into()),
                owner: o.metadata.and_then(|m| m.get("owner").cloned()),
            })),
            Err(e) if matches!(&e, google_cloud_storage::http::Error::Response(r) if r.code == 404) => {
                Ok(None)
            }
            Err(_) => bail!("GCS metadata request failed"),
        }
    }
    pub async fn stat(&self, key: &str) -> Result<Option<StoredObject>> {
        match self.mode {
            WriteMode::Gcs | WriteMode::Dual => self.gcs_stat(key).await,
            WriteMode::B2 | WriteMode::B2Primary => self.b2()?.stat(key).await,
        }
    }
    pub async fn read(&self, key: &str) -> Result<Bytes> {
        let object = self
            .stat(key)
            .await?
            .ok_or_else(|| anyhow!("Media object not found"))?;
        let bytes = self.read_ref(&object.reference).await?;
        if bytes.len() as u64 != object.size {
            bail!("Stored media length mismatch");
        }
        Ok(bytes)
    }
    /// A dual-mode cache hit is valid only when both authoritative copies
    /// independently contain the same bytes. Reads never repair storage.
    pub async fn read_verified(&self, key: &str) -> Result<Bytes> {
        use futures::StreamExt;
        let bytes = self.read(key).await?;
        if !self.mode.requires_both() {
            return Ok(bytes);
        }
        let backup = if self.mode == WriteMode::B2Primary {
            self.gcs_stat(key).await?
        } else {
            self.b2()?.stat(key).await?
        }
        .ok_or_else(|| anyhow!("Required backup copy missing"))?;
        if backup.size != bytes.len() as u64 {
            bail!("Dual storage copies differ in size");
        }
        let mut stream = self.stream_ref(&backup.reference).await?;
        let mut length = 0usize;
        let mut hash = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            length = length
                .checked_add(chunk.len())
                .ok_or_else(|| anyhow!("Media length overflow"))?;
            if length > bytes.len() {
                bail!("Dual storage copies differ in size");
            }
            hash.update(&chunk);
        }
        if length != bytes.len() || hash.finalize() != Sha256::digest(&bytes) {
            bail!("Dual storage copies failed integrity verification");
        }
        Ok(bytes)
    }
    async fn inherited_metadata(
        &self,
        key: &str,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        // In dual mode a retry repairing B2 preserves existing GCS metadata,
        // rather than assigning a deduplicated original to the retrying user.
        if self.mode == WriteMode::Dual {
            if let Some(existing) = self.gcs_stat(key).await? {
                return Ok((existing.content_type, existing.owner));
            }
        }
        if self.mode == WriteMode::B2Primary {
            if let Some(existing) = self.b2()?.stat(key).await? {
                return Ok((existing.content_type, existing.owner));
            }
            // Bootstrap a missing primary without changing historical GCS
            // provenance. This does not make GCS a read/source fallback.
            if let Some(existing) = self.gcs_stat(key).await? {
                return Ok((existing.content_type, existing.owner));
            }
        }
        Ok((content_type.into(), owner.map(str::to_owned)))
    }
    pub async fn read_ref(&self, reference: &ObjectRef) -> Result<Bytes> {
        match reference.provider {
            StorageProvider::B2 => self.b2()?.read(reference).await,
            StorageProvider::Gcs => {
                if reference.bucket != self.gcs_bucket {
                    bail!("GCS bucket mismatch");
                }
                let generation = reference
                    .version
                    .as_ref()
                    .map(|v| v.parse::<i64>())
                    .transpose()
                    .map_err(|_| anyhow!("Invalid GCS generation"))?;
                self.gcs()?
                    .download_object(
                        &GetObjectRequest {
                            bucket: reference.bucket.clone(),
                            object: reference.key.clone(),
                            generation,
                            ..Default::default()
                        },
                        &Range::default(),
                    )
                    .await
                    .map(Bytes::from)
                    .map_err(|_| anyhow!("GCS media read failed"))
            }
        }
    }
    async fn put_gcs(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<(ObjectRef, bool)> {
        if let Some(existing) = self.gcs_stat(key).await? {
            self.verify(&existing.reference, &bytes).await?;
            return Ok((existing.reference, false));
        }
        let mut metadata = HashMap::new();
        if let Some(owner) = owner {
            metadata.insert("owner".into(), owner.into());
        }
        let upload = UploadType::Multipart(Box::new(Object {
            name: key.into(),
            content_type: Some(content_type.into()),
            metadata: Some(metadata),
            ..Default::default()
        }));
        match self
            .gcs()?
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.gcs_bucket.clone(),
                    if_generation_match: Some(0),
                    ..Default::default()
                },
                bytes.clone(),
                &upload,
            )
            .await
        {
            Ok(o) => {
                let r = ObjectRef {
                    provider: StorageProvider::Gcs,
                    bucket: self.gcs_bucket.clone(),
                    key: key.into(),
                    version: Some(o.generation.to_string()),
                };
                self.verify(&r, &bytes).await?;
                Ok((r, true))
            }
            Err(_) => {
                let existing = self
                    .gcs_stat(key)
                    .await?
                    .ok_or_else(|| anyhow!("GCS write outcome unresolved"))?;
                self.verify(&existing.reference, &bytes).await?;
                Ok((existing.reference, false))
            }
        }
    }
    async fn verify(&self, reference: &ObjectRef, bytes: &Bytes) -> Result<()> {
        use futures::StreamExt;
        if reference.provider == StorageProvider::B2 {
            return self.b2()?.verify(reference, bytes).await;
        }
        let generation = reference
            .version
            .as_ref()
            .ok_or_else(|| anyhow!("Exact generation required"))?
            .parse::<i64>()?;
        let mut stream = self
            .gcs()?
            .download_streamed_object(
                &GetObjectRequest {
                    bucket: reference.bucket.clone(),
                    object: reference.key.clone(),
                    generation: Some(generation),
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .map_err(|_| anyhow!("GCS verification read failed"))?;
        let mut total = 0usize;
        let mut hash = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("GCS verification body failed"))?;
            total = total
                .checked_add(chunk.len())
                .ok_or_else(|| anyhow!("GCS length overflow"))?;
            if total > bytes.len() {
                bail!("GCS stored media length mismatch");
            }
            hash.update(&chunk);
        }
        if total != bytes.len() || hash.finalize() != Sha256::digest(bytes) {
            bail!("Stored media integrity verification failed");
        }
        Ok(())
    }
    pub async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<WriteResult> {
        if self.mode == WriteMode::B2Primary {
            use std::io::Write;
            let mut file = tempfile::NamedTempFile::new()?;
            file.write_all(&bytes)?;
            file.flush()?;
            return primary::put_file(self, key, file.path(), content_type, owner).await;
        }
        let _guard = self.write_guard(key).await?;
        let (mut content_type, mut owner) =
            self.inherited_metadata(key, content_type, owner).await?;
        let mut objects = Vec::new();
        let mut newly_stored = false;
        if self.mode != WriteMode::B2 {
            let (r, new) = self
                .put_gcs(key, bytes.clone(), &content_type, owner.as_deref())
                .await?;
            objects.push(r);
            newly_stored |= new;
        }
        if self.mode == WriteMode::Dual {
            let winner = self
                .gcs_stat(key)
                .await?
                .ok_or_else(|| anyhow!("GCS canonical winner disappeared"))?;
            content_type = winner.content_type;
            owner = winner.owner;
        }
        if matches!(self.mode, WriteMode::B2 | WriteMode::Dual) {
            let (r, new) = self
                .b2()?
                .put(key, bytes, &content_type, owner.as_deref())
                .await?;
            objects.push(r);
            newly_stored |= new;
        }
        Ok(WriteResult {
            objects,
            newly_stored,
        })
    }
    pub async fn stream_ref(
        &self,
        reference: &ObjectRef,
    ) -> Result<futures::stream::BoxStream<'static, Result<Bytes>>> {
        use futures::StreamExt;
        match reference.provider {
            StorageProvider::B2 => self.b2()?.stream(reference).await,
            StorageProvider::Gcs => {
                if reference.bucket != self.gcs_bucket {
                    bail!("GCS bucket mismatch");
                }
                let generation = reference
                    .version
                    .as_ref()
                    .ok_or_else(|| anyhow!("Exact GCS generation required"))?
                    .parse::<i64>()
                    .map_err(|_| anyhow!("Invalid GCS generation"))?;
                let stream = self
                    .gcs()?
                    .download_streamed_object(
                        &GetObjectRequest {
                            bucket: reference.bucket.clone(),
                            object: reference.key.clone(),
                            generation: Some(generation),
                            ..Default::default()
                        },
                        &Range::default(),
                    )
                    .await
                    .map_err(|_| anyhow!("GCS media stream failed"))?;
                Ok(stream
                    .map(|c| c.map_err(|_| anyhow!("GCS media body failed")))
                    .boxed())
            }
        }
    }
    pub async fn put_file(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<WriteResult> {
        if self.mode == WriteMode::B2Primary {
            return primary::put_file(self, key, path, content_type, owner).await;
        }
        let _guard = self.write_guard(key).await?;
        let (mut content_type, mut owner) =
            self.inherited_metadata(key, content_type, owner).await?;
        let mut objects = Vec::new();
        let mut newly_stored = false;
        if self.mode != WriteMode::B2 {
            if let Some(existing) = self.gcs_stat(key).await? {
                verify_stream(self.stream_ref(&existing.reference).await?, path).await?;
                objects.push(existing.reference);
            } else {
                let mut metadata = HashMap::new();
                if let Some(owner) = owner.as_deref() {
                    metadata.insert("owner".into(), owner.into());
                }
                let upload = UploadType::Multipart(Box::new(Object {
                    name: key.into(),
                    content_type: Some(content_type.clone()),
                    metadata: Some(metadata),
                    ..Default::default()
                }));
                let file = tokio::fs::File::open(path).await?;
                let stream = tokio_util::io::ReaderStream::with_capacity(file, 1024 * 1024);
                let request = UploadObjectRequest {
                    bucket: self.gcs_bucket.clone(),
                    if_generation_match: Some(0),
                    ..Default::default()
                };
                let result = self
                    .gcs()?
                    .upload_streamed_object(&request, stream, &upload)
                    .await;
                let reference = match result {
                    Ok(object) => {
                        newly_stored = true;
                        ObjectRef {
                            provider: StorageProvider::Gcs,
                            bucket: self.gcs_bucket.clone(),
                            key: key.into(),
                            version: Some(object.generation.to_string()),
                        }
                    }
                    Err(_) => {
                        self.gcs_stat(key)
                            .await?
                            .ok_or_else(|| anyhow!("GCS write outcome unresolved"))?
                            .reference
                    }
                };
                verify_stream(self.stream_ref(&reference).await?, path).await?;
                objects.push(reference);
            }
        }
        if self.mode == WriteMode::Dual {
            let winner = self
                .gcs_stat(key)
                .await?
                .ok_or_else(|| anyhow!("GCS canonical winner disappeared"))?;
            content_type = winner.content_type;
            owner = winner.owner;
        }
        if matches!(self.mode, WriteMode::B2 | WriteMode::Dual) {
            let (reference, new) = self
                .b2()?
                .put_file(key, path, &content_type, owner.as_deref())
                .await?;
            objects.push(reference);
            newly_stored |= new;
        }
        Ok(WriteResult {
            objects,
            newly_stored,
        })
    }
    pub async fn delete_ref(&self, reference: &ObjectRef) -> Result<()> {
        match reference.provider {
            StorageProvider::B2 => self.b2()?.delete(reference).await,
            StorageProvider::Gcs => {
                if reference.bucket != self.gcs_bucket {
                    bail!("GCS bucket mismatch");
                }
                let generation = reference
                    .version
                    .as_ref()
                    .ok_or_else(|| anyhow!("Exact GCS generation required"))?
                    .parse::<i64>()
                    .map_err(|_| anyhow!("Invalid GCS generation"))?;
                match self
                    .gcs()?
                    .delete_object(&DeleteObjectRequest {
                        bucket: reference.bucket.clone(),
                        object: reference.key.clone(),
                        generation: Some(generation),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(google_cloud_storage::http::Error::Response(r)) if r.code == 404 => Ok(()),
                    Err(_) => Err(anyhow!("GCS delete failed")),
                }
            }
        }
    }
}
#[cfg(test)]
mod tests;

/// Verify a pinned provider stream with bounded memory against a stable spool.
async fn verify_stream(
    mut stream: futures::stream::BoxStream<'static, Result<Bytes>>,
    path: &std::path::Path,
) -> Result<()> {
    use futures::StreamExt;
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let expected = file.metadata().await?.len();
    let mut local_hash = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut local_size = 0u64;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        local_hash.update(&buffer[..read]);
        local_size += read as u64;
    }
    if local_size != expected {
        bail!("Upload source changed while hashing");
    }
    let mut remote_hash = Sha256::new();
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        total = total
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| anyhow!("Media length overflow"))?;
        if total > expected {
            bail!("Stored media length mismatch");
        }
        remote_hash.update(&chunk);
    }
    if total != expected || local_hash.finalize() != remote_hash.finalize() {
        bail!("Stored media integrity verification failed");
    }
    Ok(())
}

/// A session-level PostgreSQL lock remains held throughout remote writes and
/// independent verification. Dropping the dedicated connection also releases
/// the lock if cancellation or a process crash interrupts the operation.
struct CanonicalWriteGuard {
    client: Option<tokio_postgres::Client>,
    name: String,
}
impl Drop for CanonicalWriteGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let name = self.name.clone();
            tokio::spawn(async move {
                let _ = client
                    .query_one(
                        "SELECT pg_advisory_unlock(hashtextextended($1,0))",
                        &[&name],
                    )
                    .await;
            });
        }
    }
}
