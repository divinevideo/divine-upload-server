//! Narrow B2-primary admission and receipts in the existing GCS state bucket.
//! No expiry or automatic takeover: an issued effect without verified receipt
//! remains pending. These records do not grant policy or erasure authority.
use super::*;
use google_cloud_storage::http::objects::upload::Media;
use rand::RngCore;
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Slot {
    NotIssued,
    Issued,
    Acknowledged { object: ObjectRef },
    Verified { object: ObjectRef },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u8,
    mode: WriteMode,
    key: String,
    gcs_bucket: String,
    b2_bucket: String,
    size: u64,
    sha256: String,
    content_type: String,
    owner: Option<String>,
    attempt: String,
    multipart_upload_id: Option<String>,
    b2: Slot,
    gcs: Slot,
}
struct Versioned {
    generation: i64,
    record: Record,
}
pub(super) struct Admission {
    client: GcsClient,
    bucket: String,
    key: String,
    state: Mutex<Versioned>,
}
fn record_key(key: &str) -> String {
    format!(
        "__resumable/b2-primary-writes/v1/{}.json",
        hex::encode(Sha256::digest(key.as_bytes()))
    )
}
async fn read_record(storage: &MediaStorage, key: &str) -> Result<Option<Versioned>> {
    let Some(client) = storage.gcs.as_ref() else {
        return Ok(None);
    };
    let metadata = match client
        .get_object(&GetObjectRequest {
            bucket: storage.gcs_bucket().into(),
            object: record_key(key),
            ..Default::default()
        })
        .await
    {
        Ok(value) => value,
        Err(error) if is_gcs_missing(&error) => return Ok(None),
        Err(_) => bail!("B2-primary admission lookup failed"),
    };
    if metadata.generation <= 0 || !(0..=16384).contains(&metadata.size) {
        bail!("Invalid B2-primary admission record");
    }
    let bytes = client
        .download_object(
            &GetObjectRequest {
                bucket: storage.gcs_bucket().into(),
                object: record_key(key),
                generation: Some(metadata.generation),
                ..Default::default()
            },
            &Range::default(),
        )
        .await
        .map_err(|_| anyhow!("B2-primary admission read failed"))?;
    let old: Record = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow!("Invalid B2-primary admission record"))?;
    if old.schema_version != 1
        || old.mode != WriteMode::B2Primary
        || old.key != key
        || old.gcs_bucket != storage.gcs_bucket()
        || Some(old.b2_bucket.as_str()) != storage.b2_bucket()
    {
        bail!("B2-primary admission identity or provider mapping changed");
    }
    for (provider, slot, bucket) in [
        (StorageProvider::B2, &old.b2, &old.b2_bucket),
        (StorageProvider::Gcs, &old.gcs, &old.gcs_bucket),
    ] {
        if let Slot::Acknowledged { object } | Slot::Verified { object } = slot {
            if object.provider != provider
                || object.bucket != *bucket
                || object.key != old.key
                || object
                    .version
                    .as_deref()
                    .is_none_or(|v| v.is_empty() || v == "null")
                || (provider == StorageProvider::Gcs
                    && object
                        .version
                        .as_ref()
                        .and_then(|v| v.parse::<i64>().ok())
                        .is_none_or(|v| v <= 0))
            {
                bail!("Invalid B2-primary recorded provider receipt");
            }
        }
    }
    Ok(Some(Versioned {
        generation: metadata.generation,
        record: old,
    }))
}
pub(super) async fn exists(storage: &MediaStorage, key: &str) -> Result<bool> {
    Ok(read_record(storage, key).await?.is_some())
}
impl Admission {
    async fn persist(&self, state: &mut Versioned, record: Record) -> Result<()> {
        let payload = serde_json::to_vec(&record)?;
        let mut media = Media::new(self.key.clone());
        media.content_type = "application/json".into();
        media.content_length = Some(payload.len() as u64);
        let object = self
            .client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    if_generation_match: Some(state.generation),
                    ..Default::default()
                },
                payload,
                &UploadType::Simple(media),
            )
            .await
            .map_err(|_| anyhow!("B2-primary admission update unresolved; retain pending state"))?;
        if object.generation <= 0 {
            bail!("B2-primary admission generation missing");
        }
        state.generation = object.generation;
        state.record = record;
        Ok(())
    }
    async fn open(
        storage: &MediaStorage,
        key: &str,
        size: u64,
        sha256: String,
        mime: &str,
        owner: Option<&str>,
        completed_only: bool,
    ) -> Result<Self> {
        let name = record_key(key);
        let client = storage.gcs()?.clone();
        let mut nonce = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let attempt = hex::encode(nonce);
        let record = Record {
            schema_version: 1,
            mode: WriteMode::B2Primary,
            key: key.into(),
            gcs_bucket: storage.gcs_bucket().into(),
            b2_bucket: storage.b2()?.bucket().into(),
            size,
            sha256,
            content_type: mime.into(),
            owner: owner.map(str::to_owned),
            attempt: attempt.clone(),
            multipart_upload_id: None,
            b2: Slot::NotIssued,
            gcs: Slot::NotIssued,
        };
        let (generation, stored) = match read_record(storage, key).await? {
            Some(value) => {
                if value.record.size != size || value.record.sha256 != record.sha256 {
                    bail!("B2-primary admission byte identity changed");
                }
                (value.generation, value.record)
            }
            None => {
                if completed_only {
                    bail!("Primary session receipts missing");
                }
                (0, record)
            }
        };
        let admission = Self {
            client,
            bucket: storage.gcs_bucket().into(),
            key: name,
            state: Mutex::new(Versioned {
                generation,
                record: stored,
            }),
        };
        {
            let mut state = admission.state.lock().await;
            if completed_only {
                if !matches!(state.record.b2, Slot::Verified { .. })
                    || !matches!(state.record.gcs, Slot::Verified { .. })
                {
                    bail!("Primary session receipts are not complete");
                }
            } else if state.generation == 0 {
                let record = state.record.clone();
                admission.persist(&mut state, record).await?;
            } else if matches!(state.record.b2, Slot::Verified { .. })
                && matches!(state.record.gcs, Slot::Verified { .. })
            {
                // Completed receipts are read-only replays, independently verified below.
            } else if matches!(state.record.b2, Slot::NotIssued | Slot::Verified { .. })
                && matches!(state.record.gcs, Slot::NotIssued)
            {
                // Exactly one retry can acquire an unissued backup slot. An older
                // actor's generation will fail before it can issue that effect.
                let mut next = state.record.clone();
                next.attempt = attempt;
                admission.persist(&mut state, next).await?;
            } else {
                bail!("B2-primary write outcome pending; explicit reconciliation required");
            }
        }
        Ok(admission)
    }
    pub(super) async fn issued(&self, provider: StorageProvider) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.record.clone();
        let slot = match provider {
            StorageProvider::B2 => &mut next.b2,
            StorageProvider::Gcs => &mut next.gcs,
        };
        if !matches!(slot, Slot::NotIssued) {
            bail!("Provider write already issued; retain pending outcome");
        }
        *slot = Slot::Issued;
        self.persist(&mut state, next).await
    }
    pub(super) async fn multipart(&self, id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.record.clone();
        if !matches!(next.b2, Slot::Issued)
            || next.multipart_upload_id.is_some()
            || id.is_empty()
            || id.len() > 4096
        {
            bail!("Invalid multipart admission");
        }
        next.multipart_upload_id = Some(id.into());
        self.persist(&mut state, next).await
    }
    pub(super) async fn acknowledged(&self, object: &ObjectRef) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.record.clone();
        let bucket = match object.provider {
            StorageProvider::B2 => &next.b2_bucket,
            StorageProvider::Gcs => &next.gcs_bucket,
        };
        if object.bucket != *bucket
            || object.key != next.key
            || (object.provider == StorageProvider::Gcs
                && object
                    .version
                    .as_ref()
                    .and_then(|v| v.parse::<i64>().ok())
                    .is_none_or(|v| v <= 0))
            || object
                .version
                .as_deref()
                .is_none_or(|v| v.is_empty() || v == "null")
        {
            bail!("Invalid acknowledged provider reference");
        }
        let slot = match object.provider {
            StorageProvider::B2 => &mut next.b2,
            StorageProvider::Gcs => &mut next.gcs,
        };
        if !matches!(slot, Slot::NotIssued | Slot::Issued) {
            bail!("Provider receipt already recorded");
        }
        *slot = Slot::Acknowledged {
            object: object.clone(),
        };
        self.persist(&mut state, next).await
    }
    pub(super) async fn verified(&self, object: &ObjectRef) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.record.clone();
        let slot = match object.provider {
            StorageProvider::B2 => &mut next.b2,
            StorageProvider::Gcs => &mut next.gcs,
        };
        if !matches!(slot,Slot::Acknowledged{object:old} if old==object) {
            bail!("Provider verification does not match acknowledgement");
        }
        *slot = Slot::Verified {
            object: object.clone(),
        };
        self.persist(&mut state, next).await
    }
    async fn reference(&self, provider: StorageProvider) -> Option<ObjectRef> {
        let state = self.state.lock().await;
        match match provider {
            StorageProvider::B2 => &state.record.b2,
            StorageProvider::Gcs => &state.record.gcs,
        } {
            Slot::Verified { object } => Some(object.clone()),
            _ => None,
        }
    }
}
fn is_gcs_missing(error: &google_cloud_storage::http::Error) -> bool {
    matches!(error,google_cloud_storage::http::Error::Response(response) if response.code==404)
}

pub(super) async fn receipts(
    storage: &MediaStorage,
    key: &str,
    size: u64,
) -> Result<Vec<ObjectRef>> {
    let admission = Admission::open(
        storage,
        key,
        size,
        key.into(),
        "application/octet-stream",
        None,
        true,
    )
    .await?;
    let state = admission.state.lock().await;
    match (&state.record.b2, &state.record.gcs) {
        (Slot::Verified { object: primary }, Slot::Verified { object: backup }) => {
            Ok(vec![primary.clone(), backup.clone()])
        }
        _ => bail!("Primary session receipts are not complete"),
    }
}

pub(super) async fn put_file(
    storage: &MediaStorage,
    key: &str,
    path: &std::path::Path,
    mime: &str,
    owner: Option<&str>,
) -> Result<WriteResult> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hash = Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| anyhow!("Upload size overflow"))?;
        crate::resources::check_size(size)?;
        hash.update(&buffer[..count]);
    }
    let (mime, owner) = storage.inherited_metadata(key, mime, owner).await?;
    let admission = Admission::open(
        storage,
        key,
        size,
        hex::encode(hash.finalize()),
        &mime,
        owner.as_deref(),
        false,
    )
    .await?;
    let (mime, owner) = {
        let state = admission.state.lock().await;
        (
            state.record.content_type.clone(),
            state.record.owner.clone(),
        )
    };
    let mut newly_stored = false;
    let primary = if let Some(reference) = admission.reference(StorageProvider::B2).await {
        verify_stream(storage.stream_ref(&reference).await?, path).await?;
        reference
    } else {
        let (reference, new) = storage
            .b2()?
            .put_file_admitted(key, path, &mime, owner.as_deref(), &admission)
            .await?;
        newly_stored |= new;
        reference
    };
    let winner = storage
        .b2()?
        .stat_reference(&primary)
        .await?
        .ok_or_else(|| anyhow!("B2 canonical winner disappeared"))?;
    if winner.size != size || winner.content_type != mime || winner.owner != owner {
        bail!("B2 pinned canonical metadata does not match admitted provenance");
    }
    let backup = if let Some(reference) = admission.reference(StorageProvider::Gcs).await {
        verify_stream(storage.stream_ref(&reference).await?, path).await?;
        reference
    } else {
        let reference = if let Some(existing) = storage.gcs_stat(key).await? {
            if existing.size != size || existing.content_type != mime || existing.owner != owner {
                bail!("Existing GCS backup provenance does not match admitted primary");
            }
            admission.acknowledged(&existing.reference).await?;
            existing.reference
        } else {
            admission.issued(StorageProvider::Gcs).await?;
            let mut metadata = HashMap::new();
            if let Some(owner) = &owner {
                metadata.insert("owner".into(), owner.clone());
            }
            let upload = UploadType::Multipart(Box::new(Object {
                name: key.into(),
                content_type: Some(mime),
                metadata: Some(metadata),
                ..Default::default()
            }));
            let file = tokio::fs::File::open(path).await?;
            let stream = tokio_util::io::ReaderStream::with_capacity(file, 1024 * 1024);
            let object = storage
                .gcs()?
                .upload_streamed_object(
                    &UploadObjectRequest {
                        bucket: storage.gcs_bucket().into(),
                        if_generation_match: Some(0),
                        ..Default::default()
                    },
                    stream,
                    &upload,
                )
                .await
                .map_err(|_| {
                    anyhow!("GCS backup write outcome unresolved; retain pending admission")
                })?;
            let reference = ObjectRef {
                provider: StorageProvider::Gcs,
                bucket: storage.gcs_bucket().into(),
                key: key.into(),
                version: Some(object.generation.to_string()),
            };
            admission.acknowledged(&reference).await?;
            newly_stored = true;
            reference
        };
        verify_stream(storage.stream_ref(&reference).await?, path).await?;
        admission.verified(&reference).await?;
        reference
    };
    Ok(WriteResult {
        objects: vec![primary, backup],
        newly_stored,
    })
}
