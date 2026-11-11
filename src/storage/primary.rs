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
/// Backoff base for control-record write retries; tests keep the schedule shape
/// without spending wall-clock seconds.
const CONTROL_RETRY_BASE_MS: u64 = if cfg!(test) { 1 } else { 1000 };
const CONTROL_RETRY_JITTER_MS: u64 = if cfg!(test) { 2 } else { 250 };
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
    let Some((generation, bytes)) =
        read_record_bytes(client, storage.gcs_bucket(), &record_key(key)).await?
    else {
        return Ok(None);
    };
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
        generation,
        record: old,
    }))
}

async fn read_record_bytes(
    client: &GcsClient,
    bucket: &str,
    name: &str,
) -> Result<Option<(i64, Vec<u8>)>> {
    let metadata = match client
        .get_object(&GetObjectRequest {
            bucket: bucket.into(),
            object: name.into(),
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
                bucket: bucket.into(),
                object: name.into(),
                generation: Some(metadata.generation),
                ..Default::default()
            },
            &Range::default(),
        )
        .await
        .map_err(|_| anyhow!("B2-primary admission read failed"))?;
    Ok(Some((metadata.generation, bytes)))
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
        let request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            if_generation_match: Some(state.generation),
            ..Default::default()
        };
        let upload = UploadType::Simple(media);
        let mut retries = 0;
        let generation = loop {
            let result = self
                .client
                .upload_object(&request, payload.clone(), &upload)
                .await;
            let status = result.as_ref().err().and_then(gcs_status);
            if let Ok(object) = result {
                if object.generation > 0 {
                    break object.generation;
                }
            }

            // The control write may have committed before its response was lost.
            // Only the exact proposed bytes can establish this actor's transition.
            if let Ok(Some((generation, stored))) =
                read_record_bytes(&self.client, &self.bucket, &self.key).await
            {
                if generation != state.generation {
                    if stored == payload {
                        break generation;
                    }
                    bail!("B2-primary admission changed during update; retain pending state");
                }
            }

            tracing::warn!(
                provider_http_status = ?status,
                retry = retries,
                "B2-primary admission control update failed"
            );
            if status != Some(429) {
                bail!("B2-primary admission update unresolved; retain pending state");
            }
            if retries == 2 {
                // A 429 rejects the request, so this update is known not to have landed.
                bail!("B2-primary admission update throttled after retries; retain pending state");
            }
            let delay = std::time::Duration::from_millis(
                CONTROL_RETRY_BASE_MS * 2u64.pow(retries)
                    + rand::random::<u64>() % CONTROL_RETRY_JITTER_MS,
            );
            tokio::time::sleep(delay).await;
            retries += 1;
        };
        state.generation = generation;
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
            } else if (matches!(state.record.b2, Slot::NotIssued | Slot::Verified { .. })
                && matches!(state.record.gcs, Slot::NotIssued))
                || (matches!(state.record.b2, Slot::Verified { .. })
                    && matches!(state.record.gcs, Slot::Acknowledged { .. }))
            {
                // Exactly one retry can acquire an unissued backup slot or finish an
                // acknowledged one. An older actor's generation fails before it can
                // issue that effect, and its readback cannot match this attempt's
                // final receipt.
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
    async fn slot(&self, provider: StorageProvider) -> Slot {
        let state = self.state.lock().await;
        match provider {
            StorageProvider::B2 => state.record.b2.clone(),
            StorageProvider::Gcs => state.record.gcs.clone(),
        }
    }
}
fn is_gcs_missing(error: &google_cloud_storage::http::Error) -> bool {
    matches!(error,google_cloud_storage::http::Error::Response(response) if response.code==404)
}
/// HTTP status of a failed GCS call, including responses whose error body did not
/// parse. Absence checks deliberately keep using parsed 404s only.
fn gcs_status(error: &google_cloud_storage::http::Error) -> Option<u16> {
    match error {
        google_cloud_storage::http::Error::Response(response) => Some(response.code),
        google_cloud_storage::http::Error::HttpClient(error) => {
            error.status().map(|status| status.as_u16())
        }
        _ => None,
    }
}

/// Every stored copy must carry the admitted size, content type and owner.
fn matches_provenance(object: &StoredObject, size: u64, mime: &str, owner: Option<&str>) -> bool {
    object.size == size && object.content_type == mime && object.owner.as_deref() == owner
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
    let primary = match admission.slot(StorageProvider::B2).await {
        Slot::Verified { object: reference } => {
            verify_stream(storage.stream_ref(&reference).await?, path).await?;
            reference
        }
        _ => {
            let (reference, new) = storage
                .b2()?
                .put_file_admitted(key, path, &mime, owner.as_deref(), &admission)
                .await?;
            newly_stored |= new;
            reference
        }
    };
    let winner = storage
        .b2()?
        .stat_reference(&primary)
        .await?
        .ok_or_else(|| anyhow!("B2 canonical winner disappeared"))?;
    if !matches_provenance(&winner, size, &mime, owner.as_deref()) {
        bail!("B2 pinned canonical metadata does not match admitted provenance");
    }
    let slot = admission.slot(StorageProvider::Gcs).await;
    let backup = if let Slot::Verified { object: reference } = slot {
        verify_stream(storage.stream_ref(&reference).await?, path).await?;
        reference
    } else {
        let reference = if let Slot::Acknowledged { object: reference } = slot {
            let existing = storage
                .stat_ref(&reference)
                .await?
                .ok_or_else(|| anyhow!("Recorded GCS backup generation is missing"))?;
            if !matches_provenance(&existing, size, &mime, owner.as_deref()) {
                bail!("Recorded GCS backup provenance does not match admitted primary");
            }
            // An earlier attempt recorded this backup but never returned, so hooks
            // gated on a new store have not run for this upload. The fence in
            // `open` lets only one request finish it. A backup adopted from an
            // earlier storage mode looks the same here, so this is at-least-once:
            // that rare retry repeats a best-effort hook rather than skipping one.
            newly_stored = true;
            reference
        } else if let Some(existing) = storage.gcs_stat(key).await? {
            if !matches_provenance(&existing, size, &mime, owner.as_deref()) {
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
