//! Durable upload intents and authenticated, at-least-once processing dispatch.
use crate::{
    processing::{MediaFinalizedV1, ProcessingClient},
    storage::{MediaStorage, ObjectRef, StorageProvider, WriteMode},
};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

const SCHEMA: &str = include_str!("../database/migrations/002_processing_outbox.sql");
fn schema() -> String {
    format!(
        "{SCHEMA}\n{}",
        include_str!("../database/migrations/007_sync_thumbnail_barrier.sql")
    )
}
const SESSION_RECEIPT_EVENT: &str = "SELECT o.payload FROM upload_processing_session_receipts r JOIN upload_processing_outbox o ON o.job_id=r.job_id WHERE r.upload_id=$1";

#[derive(Debug, thiserror::Error)]
#[error("Upload intent requires its recorded storage policy and destination mapping; retain the required providers or reconcile the intent")]
pub struct IntentConflict;

#[derive(Debug, thiserror::Error)]
#[error("Dual upload is pending verified thumbnail finalization; retry the same upload")]
pub struct ThumbnailPending;

pub struct DirectUploadIntent {
    pub storage: MediaStorage,
    pub thumbnail: Option<ThumbnailReservation>,
}

#[derive(Clone)]
pub struct ThumbnailReservation {
    generation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedThumbnail {
    pub sha256: String,
    pub size: u64,
    pub objects: Vec<ObjectRef>,
}

#[derive(Serialize, Deserialize)]
struct ThumbnailReceipt {
    originals: Vec<ObjectRef>,
    thumbnail: VerifiedThumbnail,
}

#[derive(Clone, Copy)]
enum UploadAdmission {
    Session,
    Resume,
    Direct,
    PinnedDirect,
    PinnedResume,
}
impl UploadAdmission {
    fn is_direct(self) -> bool {
        matches!(self, Self::Direct | Self::PinnedDirect)
    }
    fn pinned(self) -> bool {
        matches!(self, Self::PinnedDirect | Self::PinnedResume)
    }
}
pub(crate) struct ResolvedAdmission {
    pub storage: MediaStorage,
    pub thumbnail: Option<ThumbnailReservation>,
    pub processing_outbox: Option<ProcessingOutbox>,
}

#[derive(Clone)]
pub struct ProcessingOutbox {
    database: Arc<Mutex<Client>>,
    consumer: Option<ProcessingClient>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingUpload {
    pub sha256: String,
    pub size: u64,
    pub content_type: String,
    pub owner: String,
    pub mode: String,
    pub gcs_bucket: String,
    pub b2_bucket: Option<String>,
    pub sync_thumbnail_pending: bool,
}

pub(crate) async fn initialize_schema(client: &Client) -> Result<()> {
    client
        .batch_execute(&schema())
        .await
        .map_err(|_| anyhow!("processing database schema initialization failed"))
}

/// Only reuse an already accepted pending intent. This creates neither a new
/// database/schema nor intents for new B2-primary admissions. Dispatch remains
/// the responsibility of the historical deployment's configured consumer.
pub(crate) async fn historical_outbox(
    storage: &MediaStorage,
    hash: &str,
    owner: &str,
) -> Result<Option<ProcessingOutbox>> {
    let Some(url) = storage.coordination_url().await else {
        return Ok(None);
    };
    let database = crate::coordination::connect(&url).await?;
    let existing=match database.query_opt("SELECT 1 FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND NOT completed",&[&hash,&owner]).await {
        Ok(row)=>row.is_some(),
        Err(error) if error.code()==Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE)=>false,
        Err(_)=>bail!("Historical upload intent lookup failed"),
    };
    Ok(existing.then(|| ProcessingOutbox {
        database: Arc::new(Mutex::new(database)),
        consumer: None,
    }))
}

pub(crate) struct SourceHistory {
    pub objects: Vec<ObjectRef>,
    // True only when one committed event itself names B2 then GCS. A union
    // assembled from separate legacy events cannot establish this policy.
    pub explicit_primary: bool,
}
impl SourceHistory {
    fn empty() -> Self {
        Self {
            objects: Vec::new(),
            explicit_primary: false,
        }
    }
}

/// Reading accepted source contracts does not require dispatch credentials or
/// initialize a schema. Absence of a configured history database is the normal
/// PG-free path; deployments with history must retain that existing binding.
pub(crate) async fn historical_source_history(
    storage: &MediaStorage,
    hash: &str,
) -> Result<SourceHistory> {
    let Some(url) = storage.coordination_url().await else {
        return Ok(SourceHistory::empty());
    };
    let database = crate::coordination::connect(&url).await?;
    let present: bool = database
        .query_one(
            "SELECT to_regclass('upload_processing_outbox') IS NOT NULL",
            &[],
        )
        .await
        .map_err(|_| anyhow!("Historical source contract lookup failed"))?
        .get(0);
    if !present {
        return Ok(SourceHistory::empty());
    }
    source_history_from_client(&database, hash).await
}

/// Accepted GCS primary records precede existing PG contracts, which precede
/// current new-admission defaults. Contradictory accepted policies fail closed.
pub(crate) async fn resolve_admission(
    storage: &MediaStorage,
    global: Option<&ProcessingOutbox>,
    hash: &str,
    size: u64,
    mime: &str,
    owner: &str,
    direct: bool,
) -> Result<ResolvedAdmission> {
    let primary = storage.accepted_primary(hash).await?;
    let pinned = primary.is_some();
    let storage = primary.unwrap_or_else(|| storage.clone());
    let processing_outbox = match global {
        Some(value) => Some(value.clone()),
        None => historical_outbox(&storage, hash, owner).await?,
    };
    let (storage, thumbnail) = if let Some(outbox) = &processing_outbox {
        let admission = match (direct, pinned) {
            (true, true) => UploadAdmission::PinnedDirect,
            (false, true) => UploadAdmission::PinnedResume,
            (true, false) => UploadAdmission::Direct,
            (false, false) => UploadAdmission::Resume,
        };
        let intent = if pinned {
            outbox
                .reserve_upload(hash, size, mime, owner, &storage, admission)
                .await?
        } else if direct {
            outbox
                .begin_direct_upload(hash, size, mime, owner, &storage)
                .await?
        } else {
            DirectUploadIntent {
                storage: outbox
                    .begin_or_resume_upload(hash, size, mime, owner, &storage)
                    .await?,
                thumbnail: None,
            }
        };
        (intent.storage, intent.thumbnail)
    } else {
        (storage, None)
    };
    Ok(ResolvedAdmission {
        storage,
        thumbnail,
        processing_outbox,
    })
}
#[derive(Debug, thiserror::Error)]
#[error("Historical session processing policy unavailable; retain the original processing configuration")]
pub(crate) struct HistoricalSessionUnavailable;

pub(crate) async fn history_for_accepted_session(
    url: &str,
    id: &str,
    hash: &str,
    owner: &str,
    old_non_gcs: bool,
    birth_policy: Option<bool>,
) -> Result<Option<ProcessingOutbox>> {
    let database = crate::coordination::connect(url).await?;
    let required = birth_policy == Some(true) || (old_non_gcs && birth_policy.is_none());
    let tables = database.query_one("SELECT to_regclass('upload_sessions') IS NOT NULL, to_regclass('upload_processing_intents') IS NOT NULL, to_regclass('upload_processing_session_receipts') IS NOT NULL, to_regclass('upload_processing_outbox') IS NOT NULL", &[]).await?;
    let session_table = tables.get::<_, bool>(0);
    let processing_tables = (1..=3).filter(|i| tables.get::<_, bool>(*i)).count();
    if !session_table || processing_tables != 3 {
        return if required || processing_tables != 0 {
            Err(HistoricalSessionUnavailable.into())
        } else {
            Ok(None)
        };
    }
    let row = database.query_one(
        "SELECT EXISTS(SELECT 1 FROM upload_sessions WHERE upload_id=$3 AND state->>'final_sha256'=$1 AND state->>'owner'=$2), EXISTS(SELECT 1 FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND NOT completed) OR EXISTS(SELECT 1 FROM upload_processing_session_receipts WHERE upload_id=$3)",
        &[&hash, &owner, &id],
    ).await.map_err(|_| anyhow!("Historical session contract lookup failed"))?;
    let accepted_session = row.get::<_, bool>(0);
    let accepted_work = row.get::<_, bool>(1);
    if required && !accepted_session {
        return Err(HistoricalSessionUnavailable.into());
    }
    // Before this field existed, a native PG session alone cannot distinguish
    // legacy processing from outbox processing. Never guess away that contract.
    if accepted_session && birth_policy.is_none() && !old_non_gcs && !accepted_work {
        return Err(HistoricalSessionUnavailable.into());
    }
    Ok(
        (accepted_work || (required && accepted_session)).then(|| ProcessingOutbox {
            database: Arc::new(Mutex::new(database)),
            consumer: None,
        }),
    )
}

pub(crate) async fn history_for_session_cleanup(
    url: &str,
    id: &str,
    declared_processing: bool,
) -> Result<Option<ProcessingOutbox>> {
    let database = crate::coordination::connect(url).await?;
    let tables = database.query_one("SELECT to_regclass('upload_processing_intents') IS NOT NULL, to_regclass('upload_processing_session_receipts') IS NOT NULL, to_regclass('upload_processing_outbox') IS NOT NULL", &[]).await
        .map_err(|_| anyhow!("Historical cleanup schema lookup failed"))?;
    let present = (0..3).filter(|i| tables.get::<_, bool>(*i)).count();
    if present != 3 {
        return if present == 0 && !declared_processing {
            Ok(None)
        } else {
            Err(HistoricalSessionUnavailable.into())
        };
    }
    let has_receipt = database
        .query_opt(
            "SELECT 1 FROM upload_processing_session_receipts WHERE upload_id=$1",
            &[&id],
        )
        .await
        .map_err(|_| anyhow!("Historical session cleanup lookup failed"))?
        .is_some();
    Ok(has_receipt.then(|| ProcessingOutbox {
        database: Arc::new(Mutex::new(database)),
        consumer: None,
    }))
}

impl ProcessingOutbox {
    pub async fn connect(database_url: &str, endpoint: &str, bearer_token: &str) -> Result<Self> {
        let consumer = ProcessingClient::new(endpoint, bearer_token)?;
        let client = crate::coordination::connect(database_url).await?;
        initialize_schema(&client).await?;
        Ok(Self {
            database: Arc::new(Mutex::new(client)),
            consumer: Some(consumer),
        })
    }

    pub async fn begin_upload(
        &self,
        hash: &str,
        size: u64,
        content_type: &str,
        owner: &str,
        storage: &MediaStorage,
    ) -> Result<()> {
        self.reserve_upload(
            hash,
            size,
            content_type,
            owner,
            storage,
            UploadAdmission::Session,
        )
        .await
        .map(|_| ())
    }

    /// Direct and mirror requests have no session policy of their own. Resume
    /// the pending intent before writing; only a new intent uses admission mode.
    pub async fn begin_or_resume_upload(
        &self,
        hash: &str,
        size: u64,
        content_type: &str,
        owner: &str,
        storage: &MediaStorage,
    ) -> Result<MediaStorage> {
        self.reserve_upload(
            hash,
            size,
            content_type,
            owner,
            storage,
            UploadAdmission::Resume,
        )
        .await
        .map(|intent| intent.storage)
    }

    pub async fn begin_direct_upload(
        &self,
        hash: &str,
        size: u64,
        content_type: &str,
        owner: &str,
        storage: &MediaStorage,
    ) -> Result<DirectUploadIntent> {
        self.reserve_upload(
            hash,
            size,
            content_type,
            owner,
            storage,
            UploadAdmission::Direct,
        )
        .await
    }

    async fn reserve_upload(
        &self,
        hash: &str,
        size: u64,
        content_type: &str,
        owner: &str,
        storage: &MediaStorage,
        admission: UploadAdmission,
    ) -> Result<DirectUploadIntent> {
        let size =
            i64::try_from(size).map_err(|_| anyhow!("upload size exceeds database range"))?;
        let mode = serde_json::to_value(storage.mode)?
            .as_str()
            .ok_or_else(|| anyhow!("invalid write mode"))?
            .to_owned();
        let gcs_bucket = storage.gcs_bucket();
        let b2_bucket = storage.b2_bucket();
        let mut client = self.database.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|_| anyhow!("upload intent transaction failed"))?;
        tx.execute("INSERT INTO upload_processing_intents(sha256,owner,size,content_type,write_mode,gcs_bucket,b2_bucket) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(sha256,owner) DO NOTHING", &[&hash,&owner,&size,&content_type,&mode,&gcs_bucket,&b2_bucket]).await.map_err(|_|anyhow!("upload intent persistence failed"))?;
        let previous = tx.query_one("SELECT completed,write_mode,gcs_bucket,b2_bucket FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 FOR UPDATE", &[&hash,&owner]).await.map_err(|_|anyhow!("upload intent lookup failed"))?;
        if self.consumer.is_none()
            && previous.get::<_, bool>(0)
            && !matches!(admission, UploadAdmission::Session)
        {
            // The historical lookup raced completion. Do not turn this adapter
            // into authority to create/reset a new admission on a stale lookup.
            // A separately accepted session can attach its own receipt to a
            // completed historical event; its pinned Session contract is retained.
            return Err(IntentConflict.into());
        }
        let changed = previous.get::<_, String>(1) != mode
            || previous.get::<_, String>(2) != gcs_bucket
            || previous.get::<_, Option<String>>(3).as_deref() != b2_bucket;
        if admission.pinned() && !previous.get::<_, bool>(0) && changed {
            return Err(IntentConflict.into());
        }
        if previous.get::<_, bool>(0) && (changed || admission.is_direct()) {
            tx.execute("UPDATE upload_processing_intents SET write_mode=$3,gcs_bucket=$4,b2_bucket=$5,completed=false,sync_thumbnail_generation=NULL,sync_thumbnail_required=false,sync_thumbnail_receipt=NULL WHERE sha256=$1 AND owner=$2", &[&hash,&owner,&mode,&gcs_bucket,&b2_bucket]).await.map_err(|_|anyhow!("upload intent readmission failed"))?;
        }
        let row = tx.query_one("SELECT size,content_type,write_mode,gcs_bucket,b2_bucket,sync_thumbnail_required,sync_thumbnail_generation FROM upload_processing_intents WHERE sha256=$1 AND owner=$2", &[&hash,&owner]).await.map_err(|_|anyhow!("upload intent lookup failed"))?;
        let recorded_mode: WriteMode = row.get::<_, String>(2).parse()?;
        let recorded_gcs: String = row.get(3);
        let recorded_b2: Option<String> = row.get(4);
        let available = required_destinations(recorded_mode, &recorded_gcs, recorded_b2.as_deref())
            .is_ok_and(|destinations| destinations_configured(storage, &destinations));
        let compatible = available
            && (!matches!(admission, UploadAdmission::Session) || recorded_mode == storage.mode);
        if row.get::<_, i64>(0) != size || !compatible {
            return Err(IntentConflict.into());
        }
        let required = row.get::<_, bool>(5)
            || (admission.is_direct()
                && recorded_mode.requires_both()
                && size > 0
                && size <= crate::thumbnail::MAX_SYNC_VIDEO_BYTES as i64
                && crate::thumbnail::is_video_type(&row.get::<_, String>(1)));
        let mut generation: Option<String> = row.get(6);
        if required && generation.is_none() {
            generation = Some(format!("{:032x}", rand::random::<u128>()));
            tx.execute("UPDATE upload_processing_intents SET completed=false,sync_thumbnail_required=true,sync_thumbnail_generation=$3,sync_thumbnail_receipt=NULL WHERE sha256=$1 AND owner=$2", &[&hash,&owner,&generation]).await.map_err(|_|anyhow!("thumbnail reservation failed"))?;
        }
        tx.execute(
            "UPDATE upload_processing_intents SET updated_at=now() WHERE sha256=$1 AND owner=$2",
            &[&hash, &owner],
        )
        .await
        .map_err(|_| anyhow!("upload intent scheduling failed"))?;
        let storage = storage
            .with_mode(recorded_mode)
            .map_err(|_| IntentConflict)?;
        tx.commit()
            .await
            .map_err(|_| anyhow!("upload intent commit failed"))?;
        Ok(DirectUploadIntent {
            storage,
            thumbnail: if required {
                Some(ThumbnailReservation {
                    generation: generation.ok_or(ThumbnailPending)?,
                })
            } else {
                None
            },
        })
    }

    pub async fn record_thumbnail(
        &self,
        hash: &str,
        owner: &str,
        reservation: &ThumbnailReservation,
        originals: Vec<ObjectRef>,
        thumbnail: VerifiedThumbnail,
    ) -> Result<()> {
        let mut client = self.database.lock().await;
        let tx = client.transaction().await.map_err(|_| ThumbnailPending)?;
        let row = tx.query_opt("SELECT write_mode,gcs_bucket,b2_bucket FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND sync_thumbnail_required AND sync_thumbnail_generation=$3 FOR UPDATE", &[&hash,&owner,&reservation.generation]).await.map_err(|_|ThumbnailPending)?.ok_or(ThumbnailPending)?;
        let mode: WriteMode = row.get::<_, String>(0).parse()?;
        let gcs_bucket: String = row.get(1);
        let b2_bucket: Option<String> = row.get(2);
        let expected = required_destinations(mode, &gcs_bucket, b2_bucket.as_deref())?;
        let receipt = ThumbnailReceipt {
            originals,
            thumbnail,
        };
        validate_thumbnail_receipt(hash, &receipt, &expected)?;
        tx.execute("UPDATE upload_processing_intents SET sync_thumbnail_receipt=$3 WHERE sha256=$1 AND owner=$2", &[&hash,&owner,&serde_json::to_value(receipt)?]).await.map_err(|_|ThumbnailPending)?;
        tx.commit().await.map_err(|_| ThumbnailPending)?;
        Ok(())
    }

    pub async fn enqueue_upload(
        &self,
        hash: &str,
        size: u64,
        _content_type: &str,
        owner: &str,
        objects: Vec<ObjectRef>,
    ) -> Result<()> {
        self.enqueue_upload_inner(hash, size, owner, objects, None)
            .await
    }

    /// A completed session may be replayed after a newer upload changes the
    /// mutable intent. An exact event already committed needs no new admission.
    pub async fn enqueue_completed_session(
        &self,
        upload_id: &str,
        hash: &str,
        size: u64,
        owner: &str,
        objects: Vec<ObjectRef>,
    ) -> Result<()> {
        self.enqueue_upload_inner(hash, size, owner, objects, Some(upload_id))
            .await
    }

    /// Whether this session already committed its processing event, so a
    /// replay can be answered without provider lookups. Finalization still
    /// rechecks the receipt under its lock.
    pub async fn has_session_receipt(
        &self,
        upload_id: &str,
        hash: &str,
        size: u64,
        owner: &str,
    ) -> Result<bool> {
        let client = self.database.lock().await;
        let Some(receipt) = client
            .query_opt(SESSION_RECEIPT_EVENT, &[&upload_id])
            .await
            .map_err(|_| anyhow!("processing receipt lookup failed"))?
        else {
            return Ok(false);
        };
        check_session_receipt(receipt.get(0), hash, size, owner)?;
        Ok(true)
    }

    /// Drop the receipt of a session being deleted after expiry. An expired
    /// session's `complete` is refused before any receipt is read, so the receipt
    /// can never answer a replay again. Its outbox event is retained.
    pub async fn forget_session_receipt(&self, upload_id: &str) -> Result<()> {
        let client = self.database.lock().await;
        client
            .execute(
                "DELETE FROM upload_processing_session_receipts WHERE upload_id=$1",
                &[&upload_id],
            )
            .await
            .map_err(|_| anyhow!("processing receipt cleanup failed"))?;
        Ok(())
    }

    async fn enqueue_upload_inner(
        &self,
        hash: &str,
        size: u64,
        owner: &str,
        mut objects: Vec<ObjectRef>,
        upload_id: Option<&str>,
    ) -> Result<()> {
        let stored_size =
            i64::try_from(size).map_err(|_| anyhow!("upload size exceeds database range"))?;
        if objects.is_empty() {
            bail!("processing requires a verified storage object");
        }
        let mut client = self.database.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| anyhow!("processing transaction failed"))?;
        if let Some(upload_id) = upload_id {
            // Serialize receipt creation across replicas independently of the
            // resumable store's session lock. No provider I/O occurs here.
            transaction
                .query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 7304))",
                    &[&upload_id],
                )
                .await
                .map_err(|_| anyhow!("processing receipt lock failed"))?;
            if let Some(receipt) = transaction
                .query_opt(SESSION_RECEIPT_EVENT, &[&upload_id])
                .await
                .map_err(|_| anyhow!("processing receipt lookup failed"))?
            {
                return check_session_receipt(receipt.get(0), hash, size, owner);
            }
        }
        // MIME declarations may change on a retry of identical content. Retain
        // the first intent's metadata instead of rejecting a valid hash forever.
        let intent = transaction.query_opt(
            "SELECT content_type,write_mode,gcs_bucket,b2_bucket,sync_thumbnail_required,sync_thumbnail_receipt FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND size=$3 FOR UPDATE",
            &[&hash, &owner, &stored_size],
        ).await.map_err(|_| anyhow!("upload intent lookup failed"))?
            .ok_or_else(|| anyhow!("processing event has no matching upload intent"))?;
        let content_type: String = intent.get(0);
        let mode: WriteMode = intent.get::<_, String>(1).parse()?;
        let gcs_bucket: String = intent.get(2);
        let b2_bucket: Option<String> = intent.get(3);
        let expected = required_destinations(mode, &gcs_bucket, b2_bucket.as_deref())?;
        let original_objects = objects.clone();
        let matches_intent = objects.len() == expected.len()
            && objects
                .iter()
                .zip(expected)
                .all(|(object, (provider, bucket))| {
                    object.provider == provider && object.bucket == bucket
                });
        // Storage orders the currently authoritative source first. Replicas are durable
        // additional copies, never additional processing jobs for the same upload.
        let source = objects.remove(0);
        let mut event = MediaFinalizedV1::new(hash, size, &content_type, owner, source)?;
        for replica in &objects {
            MediaFinalizedV1::new(hash, size, &content_type, owner, replica.clone())?;
        }
        event.replicas = objects;
        event.refresh_job_id()?;
        let payload = serde_json::to_value(&event)?;
        // TODO(#33): remove this pre-receipt replay path, its lookup query and
        // the matching contract and deployment text once every session completed
        // before receipts has expired: one session lifetime after the receipts
        // schema is live in every environment.
        let historical_replay = upload_id.is_some()
            && !matches_intent
            && transaction
                .query_opt(
                    "SELECT 1 FROM upload_processing_outbox WHERE job_id=$1 AND payload=$2",
                    &[&event.job_id, &payload],
                )
                .await
                .map_err(|_| anyhow!("completed processing event lookup failed"))?
                .is_some();
        if !matches_intent && !historical_replay {
            return Err(IntentConflict.into());
        }
        if !historical_replay {
            if intent.get::<_, bool>(4) {
                let receipt: ThumbnailReceipt = serde_json::from_value(
                    intent
                        .get::<_, Option<serde_json::Value>>(5)
                        .ok_or(ThumbnailPending)?,
                )
                .map_err(|_| ThumbnailPending)?;
                validate_thumbnail_receipt(
                    hash,
                    &receipt,
                    &required_destinations(mode, &gcs_bucket, b2_bucket.as_deref())?,
                )?;
                if receipt.originals != original_objects {
                    return Err(ThumbnailPending.into());
                }
            }
            transaction.execute("INSERT INTO upload_processing_outbox(job_id,payload) VALUES($1,$2) ON CONFLICT(job_id) DO UPDATE SET finalized_at=clock_timestamp()",&[&event.job_id,&payload]).await.map_err(|_|anyhow!("processing event persistence failed"))?;
            let updated=transaction.execute("UPDATE upload_processing_intents SET completed=true,updated_at=now() WHERE sha256=$1 AND owner=$2 AND size=$3 AND content_type=$4",&[&hash,&owner,&stored_size,&content_type]).await.map_err(|_|anyhow!("upload intent completion failed"))?;
            if updated != 1 {
                bail!("processing event has no matching upload intent");
            }
        }
        if let Some(upload_id) = upload_id {
            transaction.execute(
                "INSERT INTO upload_processing_session_receipts(upload_id,job_id) VALUES($1,$2)",
                &[&upload_id, &event.job_id],
            ).await.map_err(|_| anyhow!("processing receipt persistence failed"))?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| anyhow!("processing event commit failed"))
    }

    /// Reads a bounded page of unfinished intents so upload recovery can reconcile
    /// provider outcomes. It does not infer that missing local state means no bytes.
    pub async fn pending_intents(&self, limit: u32) -> Result<Vec<PendingUpload>> {
        let client = self.database.lock().await;
        let rows=client.query("SELECT sha256,size,content_type,owner,write_mode,gcs_bucket,b2_bucket,sync_thumbnail_required AND sync_thumbnail_receipt IS NULL FROM upload_processing_intents WHERE NOT completed ORDER BY updated_at LIMIT $1",&[&(limit.min(1000) as i64)]).await.map_err(|_|anyhow!("upload intent scan failed"))?;
        Ok(rows
            .into_iter()
            .map(|row| PendingUpload {
                sha256: row.get(0),
                size: row.get::<_, i64>(1) as u64,
                content_type: row.get(2),
                owner: row.get(3),
                mode: row.get(4),
                gcs_bucket: row.get(5),
                b2_bucket: row.get(6),
                sync_thumbnail_pending: row.get(7),
            })
            .collect())
    }

    /// Reconcile already complete, verified destinations without copying media.
    pub async fn recover_intents(
        &self,
        storage: &MediaStorage,
        limit: u32,
        max_bytes: u64,
    ) -> Result<u64> {
        use sha2::{Digest, Sha256};
        let mut recovered = 0;
        let mut budget = max_bytes;
        for intent in self.pending_intents(limit).await? {
            {
                let db = self.database.lock().await;
                db.execute("UPDATE upload_processing_intents SET updated_at=now() WHERE sha256=$1 AND owner=$2",&[&intent.sha256,&intent.owner]).await.map_err(|_|anyhow!("upload recovery scheduling failed"))?;
            }
            if intent.sync_thumbnail_pending {
                continue;
            }
            if intent.size > budget {
                continue;
            }
            let mode: WriteMode = intent.mode.parse()?;
            let available =
                required_destinations(mode, &intent.gcs_bucket, intent.b2_bucket.as_deref())
                    .is_ok_and(|destinations| destinations_configured(storage, &destinations));
            if !available {
                continue;
            }
            let pinned = storage.with_mode(mode)?;
            let primary = match pinned.accepted_primary(&intent.sha256).await {
                Ok(value) => value,
                Err(_) => continue,
            };
            if mode == WriteMode::B2Primary || primary.is_some() {
                let Some(primary) = primary else {
                    continue;
                };
                let Ok(receipts) = primary.primary_receipts(&intent.sha256, intent.size).await
                else {
                    continue;
                };
                if intent.size.saturating_mul(receipts.len() as u64) > budget {
                    continue;
                }
                let mut present = true;
                for reference in &receipts {
                    if !matches!(primary.stat_ref(reference).await,Ok(Some(object)) if object.size==intent.size)
                    {
                        present = false;
                        break;
                    }
                }
                if !present {
                    continue;
                }
                let mut verified = true;
                for reference in &receipts {
                    budget = budget.saturating_sub(intent.size);
                    if primary
                        .verify_ref_identity(reference, &intent.sha256, intent.size)
                        .await
                        .is_err()
                    {
                        verified = false;
                        break;
                    }
                }
                if !verified {
                    continue;
                }
                // Preserve an older intent's ordering using settled exact copies;
                // no latest HEAD can create authority around an unknown effect.
                let expected =
                    required_destinations(mode, &intent.gcs_bucket, intent.b2_bucket.as_deref())?;
                let objects: Option<Vec<_>> = expected
                    .into_iter()
                    .map(|(provider, bucket)| {
                        receipts
                            .iter()
                            .find(|object| object.provider == provider && object.bucket == bucket)
                            .cloned()
                    })
                    .collect();
                let Some(objects) = objects else {
                    continue;
                };
                let result = self
                    .enqueue_upload(
                        &intent.sha256,
                        intent.size,
                        &intent.content_type,
                        &intent.owner,
                        objects,
                    )
                    .await;
                if result
                    .as_ref()
                    .is_err_and(|error| error.is::<ThumbnailPending>())
                {
                    continue;
                }
                result?;
                recovered += 1;
                continue;
            }
            let modes = match mode {
                WriteMode::Gcs => vec![WriteMode::Gcs],
                WriteMode::B2 => vec![WriteMode::B2],
                WriteMode::Dual => vec![WriteMode::Gcs, WriteMode::B2],
                WriteMode::B2Primary => continue,
            };
            // Discover all required copies before reading any bodies. A partial
            // dual write can remain pending indefinitely; repeatedly hashing its
            // surviving copy cannot advance it and creates unbounded read cost.
            let mut candidates = Vec::new();
            for destination in &modes {
                let Ok(reader) = pinned.with_mode(*destination) else {
                    break;
                };
                let Ok(Some(object)) = reader.stat(&intent.sha256).await else {
                    break;
                };
                if object.size != intent.size {
                    break;
                }
                candidates.push((reader, object.reference));
            }
            if candidates.len() != modes.len()
                || intent.size.saturating_mul(modes.len() as u64) > budget
            {
                continue;
            }
            let mut objects = Vec::new();
            for (reader, reference) in candidates {
                budget = budget.saturating_sub(intent.size);
                let Ok(mut stream) = reader.stream_ref(&reference).await else {
                    break;
                };
                let mut hasher = Sha256::new();
                let mut count = 0_u64;
                let mut valid = true;
                use futures::StreamExt;
                while let Some(chunk) = stream.next().await {
                    let Ok(chunk) = chunk else {
                        valid = false;
                        break;
                    };
                    count = count.saturating_add(chunk.len() as u64);
                    if count > intent.size {
                        valid = false;
                        break;
                    }
                    hasher.update(&chunk);
                }
                if !valid || count != intent.size || hex::encode(hasher.finalize()) != intent.sha256
                {
                    break;
                }
                objects.push(reference);
            }
            // Background reconciliation never writes media: a lone surviving copy
            // could be racing erasure. Only an explicit upload retry may repair it.
            if objects.len() == modes.len() {
                let result = self
                    .enqueue_upload(
                        &intent.sha256,
                        intent.size,
                        &intent.content_type,
                        &intent.owner,
                        objects,
                    )
                    .await;
                if result.as_ref().is_err_and(|e| e.is::<ThumbnailPending>()) {
                    continue;
                }
                result?;
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    /// Resolve every storage copy recorded for these bytes, across owners. The
    /// newest finalization leads, so its order and versions win. A provider it
    /// lacks comes from the newest event that recorded one: a later GCS-only
    /// completion (a stale intent, background recovery, or admission rolled back
    /// to GCS) must not drop a destination this media already requires.
    pub async fn source_objects(&self, hash: &str) -> Result<Vec<ObjectRef>> {
        let database = self.database.lock().await;
        source_objects_from_client(&database, hash).await
    }
    pub(crate) async fn source_history(&self, hash: &str) -> Result<SourceHistory> {
        let database = self.database.lock().await;
        source_history_from_client(&database, hash).await
    }

    pub async fn dispatch_once(&self, limit: u32) -> Result<u64> {
        let consumer = self.consumer.as_ref().ok_or_else(|| {
            anyhow!("Historical upload receipts require their existing configured dispatcher")
        })?;
        let mut delivered = 0;
        for _ in 0..limit.min(100) {
            let token = format!("{:032x}", rand::random::<u128>());
            let row = {
                let client = self.database.lock().await;
                client.query_opt("WITH candidate AS (SELECT job_id FROM upload_processing_outbox WHERE delivered_at IS NULL AND next_attempt_at<=now() ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE upload_processing_outbox AS jobs SET lease_token=$1,next_attempt_at=now()+interval '60 seconds',attempts=attempts+1 FROM candidate WHERE jobs.job_id=candidate.job_id RETURNING jobs.job_id,jobs.payload,jobs.attempts",&[&token]).await.map_err(|_|anyhow!("processing claim failed"))?
            };
            let Some(row) = row else { break };
            let id: String = row.get(0);
            let payload: serde_json::Value = row.get(1);
            let attempts: i32 = row.get(2);
            let event = serde_json::from_value::<MediaFinalizedV1>(payload);
            let success = match event {
                Ok(event) => consumer.deliver(&event).await.is_ok(),
                Err(_) => false,
            };
            let client = self.database.lock().await;
            if success {
                let count=client.execute("UPDATE upload_processing_outbox SET delivered_at=now(),lease_token=NULL WHERE job_id=$1 AND lease_token=$2",&[&id,&token]).await.map_err(|_|anyhow!("processing acknowledgement persistence failed"))?;
                delivered += count;
            } else {
                let seconds = retry_delay(attempts);
                client.execute("UPDATE upload_processing_outbox SET next_attempt_at=now()+($3::bigint*interval '1 second'),lease_token=NULL WHERE job_id=$1 AND lease_token=$2",&[&id,&token,&seconds]).await.map_err(|_|anyhow!("processing retry persistence failed"))?;
                tracing::warn!(attempts, "processing delivery deferred");
            }
        }
        Ok(delivered)
    }
}

/// A stored event's verified copies, its authoritative source first.
/// Accepted finalization history, with the newest event's source first.
/// Missing providers may be appended from older events. This is not current
/// provider selection, policy approval, or a storage/erasure fence.
pub(crate) async fn source_objects_from_client(
    database: &Client,
    hash: &str,
) -> Result<Vec<ObjectRef>> {
    Ok(source_history_from_client(database, hash).await?.objects)
}

async fn source_history_from_client(database: &Client, hash: &str) -> Result<SourceHistory> {
    let Some(row) = database.query_opt("SELECT payload FROM upload_processing_outbox WHERE payload->>'sha256'=$1 ORDER BY finalized_at DESC,job_id DESC LIMIT 1", &[&hash]).await.map_err(|_| anyhow!("Processing source lookup failed"))? else {
            return Ok(SourceHistory::empty());
        };
    let mut objects = event_objects(row.get(0))?;
    let explicit_primary = objects
        .first()
        .is_some_and(|object| object.provider == StorageProvider::B2)
        && objects
            .iter()
            .any(|object| object.provider == StorageProvider::Gcs);
    for provider in [StorageProvider::Gcs, StorageProvider::B2] {
        if objects.iter().any(|object| object.provider == provider) {
            continue;
        }
        let recorded = serde_json::json!({ "provider": provider });
        let row = database.query_opt("SELECT payload FROM upload_processing_outbox WHERE payload->>'sha256'=$1 AND (payload->'source' @> $2 OR payload->'replicas' @> jsonb_build_array($2)) ORDER BY finalized_at DESC,job_id DESC LIMIT 1", &[&hash, &recorded]).await.map_err(|_| anyhow!("Processing source lookup failed"))?;
        if let Some(row) = row {
            objects.extend(
                event_objects(row.get(0))?
                    .into_iter()
                    .filter(|object| object.provider == provider),
            );
        }
    }
    Ok(SourceHistory {
        objects,
        explicit_primary,
    })
}

fn event_objects(payload: serde_json::Value) -> Result<Vec<ObjectRef>> {
    let event: MediaFinalizedV1 = serde_json::from_value(payload)
        .map_err(|_| anyhow!("Stored processing source is invalid"))?;
    let mut objects = vec![event.source];
    objects.extend(event.replicas);
    Ok(objects)
}

/// A receipt names the event its session committed. A mismatch is corrupt or
/// foreign state, not a storage-policy conflict the client could resolve.
pub(crate) fn check_session_receipt(
    payload: serde_json::Value,
    hash: &str,
    size: u64,
    owner: &str,
) -> Result<()> {
    let event: MediaFinalizedV1 = serde_json::from_value(payload)
        .map_err(|_| anyhow!("stored session receipt event is invalid"))?;
    if event.sha256 != hash || event.size != size || event.owner.as_deref() != Some(owner) {
        bail!("session receipt does not match the completed upload");
    }
    Ok(())
}

/// Destinations a recorded intent requires, in finalization order. Only these
/// buckets bind the intent: a configured adapter its mode never writes does not.
pub(crate) fn required_destinations<'a>(
    mode: WriteMode,
    gcs_bucket: &'a str,
    b2_bucket: Option<&'a str>,
) -> std::result::Result<Vec<(StorageProvider, &'a str)>, IntentConflict> {
    let b2 = || b2_bucket.ok_or(IntentConflict);
    Ok(match mode {
        WriteMode::Gcs => vec![(StorageProvider::Gcs, gcs_bucket)],
        WriteMode::B2 => vec![(StorageProvider::B2, b2()?)],
        WriteMode::Dual => vec![
            (StorageProvider::Gcs, gcs_bucket),
            (StorageProvider::B2, b2()?),
        ],
        WriteMode::B2Primary => vec![
            (StorageProvider::B2, b2()?),
            (StorageProvider::Gcs, gcs_bucket),
        ],
    })
}

fn destinations_configured(
    storage: &MediaStorage,
    destinations: &[(StorageProvider, &str)],
) -> bool {
    destinations.iter().all(|(provider, bucket)| {
        let configured = match provider {
            StorageProvider::Gcs => Some(storage.gcs_bucket()),
            StorageProvider::B2 => storage.b2_bucket(),
        };
        configured == Some(*bucket)
    })
}

fn validate_thumbnail_receipt(
    hash: &str,
    receipt: &ThumbnailReceipt,
    expected: &[(StorageProvider, &str)],
) -> Result<()> {
    let matches = |objects: &[ObjectRef], key: &str| {
        objects.len() == 2
            && expected.len() == 2
            && objects
                .iter()
                .zip(expected)
                .all(|(object, (provider, bucket))| {
                    object.provider == *provider
                        && object.bucket == *bucket
                        && object.key == key
                        && object.version.as_ref().is_some_and(|v| !v.is_empty())
                })
    };
    if !matches(&receipt.originals, hash)
        || !matches(&receipt.thumbnail.objects, &format!("{hash}.jpg"))
        || receipt.thumbnail.size == 0
        || receipt.thumbnail.size > crate::thumbnail::MAX_SYNC_VIDEO_BYTES as u64
        || receipt.thumbnail.sha256.len() != 64
        || !receipt
            .thumbnail
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ThumbnailPending.into());
    }
    Ok(())
}

fn retry_delay(attempt: i32) -> i64 {
    (5_i64.saturating_mul(1_i64 << attempt.clamp(0, 14))).min(86400)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    static DB_TEST_LOCK: Mutex<()> = Mutex::const_new(());

    fn thumbnail_refs(hash: &str, version: &str) -> Vec<ObjectRef> {
        vec![
            (StorageProvider::Gcs, "fixture-gcs"),
            (StorageProvider::B2, "fixture-b2"),
        ]
        .into_iter()
        .map(|(provider, bucket)| ObjectRef {
            provider,
            bucket: bucket.into(),
            key: hash.into(),
            version: Some(version.into()),
        })
        .collect()
    }

    fn thumbnail_proof(hash: &str) -> VerifiedThumbnail {
        VerifiedThumbnail {
            sha256: "c".repeat(64),
            size: 12,
            objects: thumbnail_refs(&format!("{hash}.jpg"), "jpeg-v1"),
        }
    }

    #[tokio::test]
    async fn thumbnail_receipt_is_bound_to_original_versions_and_readmission() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processor.invalid/finalized", "fixture-token")
                .await
                .unwrap();
        let hash = "a".repeat(64);
        let owner = "b".repeat(64);
        let storage = fixture_storage(WriteMode::Dual, Some("fixture-b2"));
        let first = outbox
            .begin_direct_upload(&hash, 3, "Video/MP4; codecs=avc1", &owner, &storage)
            .await
            .unwrap()
            .thumbnail
            .unwrap();
        let originals = thumbnail_refs(&hash, "original-v1");
        let mut partial = thumbnail_proof(&hash);
        partial.objects.pop();
        assert!(outbox
            .record_thumbnail(&hash, &owner, &first, originals.clone(), partial)
            .await
            .is_err());
        assert!(outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals.clone())
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        outbox
            .record_thumbnail(
                &hash,
                &owner,
                &first,
                originals.clone(),
                thumbnail_proof(&hash),
            )
            .await
            .unwrap();
        assert!(outbox
            .enqueue_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                thumbnail_refs(&hash, "original-v2")
            )
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        // A pending retry retains its accepted dual mapping even after static
        // admission changes, including when the retry declares a different MIME.
        let gcs = storage.with_mode(WriteMode::Gcs).unwrap();
        let retry = outbox
            .begin_direct_upload(&hash, 3, "application/octet-stream", &owner, &gcs)
            .await
            .unwrap();
        assert_eq!(retry.storage.mode, WriteMode::Dual);
        assert_eq!(retry.thumbnail.unwrap().generation, first.generation);
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals.clone())
            .await
            .unwrap();
        let repeated = outbox
            .begin_direct_upload(&hash, 3, "video/mp4", &owner, &storage)
            .await
            .unwrap()
            .thumbnail
            .unwrap();
        assert_ne!(
            first.generation, repeated.generation,
            "a completed direct admission must not reuse a prior thumbnail receipt"
        );
        assert!(outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals.clone())
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        outbox
            .record_thumbnail(
                &hash,
                &owner,
                &repeated,
                originals.clone(),
                thumbnail_proof(&hash),
            )
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals.clone())
            .await
            .unwrap();
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &gcs)
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![originals[0].clone()])
            .await
            .unwrap();
        let second = outbox
            .begin_direct_upload(&hash, 3, "video/mp4", &owner, &storage)
            .await
            .unwrap()
            .thumbnail
            .unwrap();
        assert_ne!(first.generation, second.generation);
        assert!(outbox
            .record_thumbnail(
                &hash,
                &owner,
                &first,
                originals.clone(),
                thumbnail_proof(&hash)
            )
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        assert!(outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals.clone())
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        outbox
            .record_thumbnail(
                &hash,
                &owner,
                &second,
                originals.clone(),
                thumbnail_proof(&hash),
            )
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, originals)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sync_thumbnail_barrier_scope_preserves_other_upload_paths() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processor.invalid/finalized", "fixture-token")
                .await
                .unwrap();
        let dual = fixture_storage(WriteMode::Dual, Some("fixture-b2"));
        let gcs = dual.with_mode(WriteMode::Gcs).unwrap();
        for (index, (size, mime, storage)) in [
            (3, "video/mp4", &gcs),
            (0, "video/mp4", &dual),
            (3, "image/jpeg", &dual),
            (
                crate::thumbnail::MAX_SYNC_VIDEO_BYTES as u64 + 1,
                "video/mp4",
                &dual,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let hash = format!("{index:064x}");
            assert!(outbox
                .begin_direct_upload(&hash, size, mime, &"b".repeat(64), storage)
                .await
                .unwrap()
                .thumbnail
                .is_none());
        }
    }

    #[tokio::test]
    async fn direct_dual_original_waits_for_sync_thumbnail_before_finalization() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        use axum::{
            http::{Method, StatusCode, Uri},
            response::IntoResponse,
            routing::any,
            Json, Router,
        };
        use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
        use sha2::{Digest, Sha256};
        let bytes = bytes::Bytes::from_static(b"synthetic-invalid-video");
        let hash = hex::encode(Sha256::digest(&bytes));
        let jpeg_available = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let availability = jpeg_available.clone();
        let fixture_hash = hash.clone();
        let fixture_bytes = bytes.clone();
        let app = Router::new().route("/*path", any(move |method: Method, uri: Uri| {
            let mut hash = fixture_hash.clone(); let mut bytes = fixture_bytes.clone(); let availability=availability.clone();
            async move {
                if uri.path().contains("b2-primary-writes") {return (axum::http::StatusCode::NOT_FOUND,Json(serde_json::json!({"error":{"code":404,"message":"No such object","errors":[]}}))).into_response();}

                let thumbnail=uri.path().ends_with(".jpg");
                if thumbnail { hash.push_str(".jpg"); bytes=bytes::Bytes::from_static(b"existing-synthetic-jpeg"); }
                if method != Method::GET && method != Method::HEAD { return StatusCode::INTERNAL_SERVER_ERROR.into_response(); }
                if uri.path().starts_with("/storage/") && !uri.query().unwrap_or_default().contains("alt=media") {
                    return Json(serde_json::json!({"name":hash,"bucket":"fixture-gcs","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"fixture","size":bytes.len().to_string(),"contentType":"video/mp4"})).into_response();
                }
                if thumbnail && !uri.path().starts_with("/storage/") && !availability.load(std::sync::atomic::Ordering::SeqCst) {return StatusCode::NOT_FOUND.into_response();}
                let mut response = bytes.clone().into_response();
                response.headers_mut().insert("x-amz-version-id", "b2-v1".parse().unwrap());
                response.headers_mut().insert("content-length", bytes.len().to_string().parse().unwrap());
                response
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let gcs = GcsClient::new(
            ClientConfig {
                storage_endpoint: endpoint.clone(),
                ..Default::default()
            }
            .anonymous(),
        );
        let b2 = crate::storage::B2Store::new(
            "fixture-b2".into(),
            "us-west-001".into(),
            "fixture".into(),
            "fixture".into(),
        )
        .unwrap()
        .with_endpoint(endpoint);
        let storage =
            MediaStorage::new(WriteMode::Dual, Some(gcs), "fixture-gcs".into(), Some(b2)).unwrap();
        let outbox =
            ProcessingOutbox::connect(&url, "https://processor.invalid/finalized", "fixture-token")
                .await
                .unwrap();
        let owner = "b".repeat(64);
        let stored = super::super::stream_to_gcs_with_hash(
            &storage,
            Some(&outbox),
            "video/mp4",
            axum::body::Body::from(bytes.clone()),
            &owner,
            None,
        )
        .await
        .unwrap();
        let client = outbox.database.lock().await;
        let count: i64 = client
            .query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            count, 0,
            "verified originals alone must not finalize a direct dual video"
        );
        let complete: bool = client
            .query_one(
                "SELECT completed FROM upload_processing_intents WHERE sha256=$1",
                &[&hash],
            )
            .await
            .unwrap()
            .get(0);
        assert!(!complete);
        drop(client);
        assert_eq!(
            outbox.pending_intents(10).await.unwrap().len(),
            1,
            "unverified thumbnails must remain visible as pending work"
        );
        let peer =
            ProcessingOutbox::connect(&url, "https://processor.invalid/finalized", "fixture-token")
                .await
                .unwrap();
        assert_eq!(peer.recover_intents(&storage, 10, 10000).await.unwrap(), 0);
        let (reservation, originals) = stored.thumbnail_finalization.unwrap();
        assert!(
            super::super::extract_and_upload_thumbnail(
                &storage,
                "https://media.example",
                &hash,
                &bytes
            )
            .await
            .is_err(),
            "failed B2 JPEG must remain pending"
        );
        assert!(outbox
            .enqueue_upload(
                &hash,
                bytes.len() as u64,
                "video/mp4",
                &owner,
                originals.clone()
            )
            .await
            .unwrap_err()
            .is::<ThumbnailPending>());
        jpeg_available.store(true, std::sync::atomic::Ordering::SeqCst);
        let thumbnail = super::super::extract_and_upload_thumbnail(
            &storage,
            "https://media.example",
            &hash,
            &bytes,
        )
        .await
        .unwrap();
        outbox
            .record_thumbnail(&hash, &owner, &reservation, originals, thumbnail.verified)
            .await
            .unwrap();
        // Simulate a crash between durable thumbnail receipt and enqueue. A
        // different process may now reconcile the exact original versions.
        assert_eq!(peer.recover_intents(&storage, 10, 10000).await.unwrap(), 1);
        assert!(peer.pending_intents(10).await.unwrap().is_empty());
        server.abort();
    }

    /// Anonymous GCS plus an optional B2 adapter; constructing it performs no I/O.
    pub(crate) fn fixture_storage(mode: WriteMode, b2_bucket: Option<&str>) -> MediaStorage {
        let gcs = google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig::default().anonymous(),
        );
        let b2 = b2_bucket.map(|bucket| {
            crate::storage::B2Store::new(
                bucket.into(),
                "us-west-001".into(),
                "fixture".into(),
                "fixture".into(),
            )
            .unwrap()
        });
        MediaStorage::new(mode, Some(gcs), "fixture-gcs".into(), b2).unwrap()
    }

    /// Copies named by the newest finalization for these bytes. Unlike
    /// `source_objects`, this shows finalization order directly.
    pub(crate) async fn newest_event_objects(
        outbox: &ProcessingOutbox,
        hash: &str,
    ) -> Vec<ObjectRef> {
        let row = outbox.database.lock().await.query_one("SELECT payload FROM upload_processing_outbox WHERE payload->>'sha256'=$1 ORDER BY finalized_at DESC,job_id DESC LIMIT 1", &[&hash]).await.unwrap();
        event_objects(row.get(0)).unwrap()
    }

    pub(crate) async fn isolated_database_url() -> Option<String> {
        let base = std::env::var("TEST_COORDINATION_DATABASE_URL").ok()?;
        let client = crate::coordination::connect(&base).await.unwrap();
        let schema = format!("outbox_test_{:032x}", rand::random::<u128>());
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let mut url = url::Url::parse(&base).unwrap();
        url.query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        Some(url.into())
    }

    #[tokio::test]
    async fn concurrent_schema_bootstrap_is_safe_on_fresh_schemas() {
        // Separate fresh schemas expose catalog races that disappear on reruns.
        for _ in 0..3 {
            let Some(url) = isolated_database_url().await else {
                return;
            };
            let barrier = Arc::new(tokio::sync::Barrier::new(4));
            let mut workers = Vec::new();
            for _ in 0..4 {
                let url = url.clone();
                let barrier = barrier.clone();
                workers.push(tokio::spawn(async move {
                    let client = crate::coordination::connect(&url).await.unwrap();
                    barrier.wait().await;
                    client
                        .batch_execute(include_str!("storage/multipart.sql"))
                        .await
                        .unwrap();
                    client.batch_execute(&schema()).await.unwrap();
                    client
                        .batch_execute(include_str!(
                            "../database/migrations/001_upload_coordination.sql"
                        ))
                        .await
                        .unwrap();
                }));
            }
            for worker in workers {
                worker.await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn source_policy_uses_finalization_order_not_retry_schedule() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let _guard = DB_TEST_LOCK.lock().await;
        use crate::storage::{B2Store, StorageProvider};
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let gcs = google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig::default().anonymous(),
        );
        let base = MediaStorage::new(
            WriteMode::Gcs,
            Some(gcs),
            "fixture-bucket".into(),
            Some(
                B2Store::new(
                    "fixture-b2".into(),
                    "us-west-004".into(),
                    "fixture".into(),
                    "fixture".into(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let hash = format!("{:064x}", rand::random::<u128>());
        let owner = "b".repeat(64);
        let original = ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-bucket".into(),
            key: hash.clone(),
            version: Some("v1".into()),
        };
        let replica = ObjectRef {
            provider: StorageProvider::B2,
            bucket: "fixture-b2".into(),
            key: hash.clone(),
            version: Some("v2".into()),
        };
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &base)
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![original.clone()])
            .await
            .unwrap();
        {
            let db = outbox.database.lock().await;
            db.execute("UPDATE upload_processing_outbox SET next_attempt_at=now()+interval '1 day' WHERE payload->>'sha256'=$1", &[&hash]).await.unwrap();
        }
        outbox
            .begin_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                &base.with_mode(WriteMode::Dual).unwrap(),
            )
            .await
            .unwrap();
        outbox
            .enqueue_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                vec![original.clone(), replica.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            outbox.source_objects(&hash).await.unwrap(),
            vec![original.clone(), replica.clone()]
        );
        // A later explicit GCS finalization may reuse its old job identity. It
        // becomes the newest event, but does not drop the B2 copy already required.
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &base)
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![original.clone()])
            .await
            .unwrap();
        assert_eq!(
            outbox.source_objects(&hash).await.unwrap(),
            vec![original, replica]
        );
        let db = outbox.database.lock().await;
        db.execute(
            "DELETE FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
            &[&hash],
        )
        .await
        .unwrap();
        db.execute(
            "DELETE FROM upload_processing_intents WHERE sha256=$1",
            &[&hash],
        )
        .await
        .unwrap();
    }

    #[test]
    fn retries_back_off_and_remain_bounded() {
        assert_eq!(retry_delay(1), 10);
        assert!(retry_delay(2) > retry_delay(1));
        assert!(retry_delay(i32::MAX) <= 86400);
    }

    #[tokio::test]
    async fn resumed_intent_requires_original_active_destinations() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let gcs_only = fixture_storage(WriteMode::Gcs, None);
        let both = fixture_storage(WriteMode::Dual, Some("fixture-b2"));
        let hash = "a".repeat(64);
        let gcs_owner = "b".repeat(64);
        outbox
            .begin_upload(&hash, 3, "video/mp4", &gcs_owner, &gcs_only)
            .await
            .unwrap();
        let resumed = outbox
            .begin_or_resume_upload(&hash, 3, "video/mp4", &gcs_owner, &both)
            .await
            .unwrap();
        assert_eq!(
            resumed.mode,
            WriteMode::Gcs,
            "adding an unused B2 adapter must not strand an older GCS intent"
        );
        let dual_owner = "c".repeat(64);
        outbox
            .begin_upload(&hash, 3, "video/mp4", &dual_owner, &both)
            .await
            .unwrap();
        let changed = fixture_storage(WriteMode::Gcs, Some("other-b2"));
        for incompatible in [&gcs_only, &changed] {
            let result = outbox
                .begin_or_resume_upload(&hash, 3, "video/mp4", &dual_owner, incompatible)
                .await;
            assert!(result.err().unwrap().is::<IntentConflict>());
        }
        let pending = outbox.pending_intents(10).await.unwrap();
        let dual_intent = pending.iter().find(|i| i.owner == dual_owner).unwrap();
        assert_eq!(dual_intent.mode, "dual");
        assert_eq!(dual_intent.b2_bucket.as_deref(), Some("fixture-b2"));
        let new_owner = "d".repeat(64);
        assert_eq!(
            outbox
                .begin_or_resume_upload(
                    &hash,
                    3,
                    "video/mp4",
                    &new_owner,
                    &both.with_mode(WriteMode::Gcs).unwrap()
                )
                .await
                .unwrap()
                .mode,
            WriteMode::Gcs
        );
    }

    #[tokio::test]
    async fn session_admission_ignores_an_unused_adapter_bucket() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let hash = "a".repeat(64);
        let owner = "b".repeat(64);
        outbox
            .begin_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                &fixture_storage(WriteMode::Gcs, None),
            )
            .await
            .unwrap();
        // A legacy GCS session completes through the GCS view of a deployment
        // that has since gained a B2 adapter. GCS mode never writes that bucket.
        let with_b2 = fixture_storage(WriteMode::Dual, Some("fixture-b2"));
        outbox
            .begin_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                &with_b2.with_mode(WriteMode::Gcs).unwrap(),
            )
            .await
            .expect("an unused B2 adapter must not conflict with a pending GCS intent");
        // The session's own policy still binds: another mode conflicts.
        let result = outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &with_b2)
            .await;
        assert!(result.err().unwrap().is::<IntentConflict>());
        // A changed required bucket conflicts whether the request carries its
        // own policy or resumes the recorded one.
        let moved_gcs = MediaStorage::new(
            WriteMode::Gcs,
            Some(google_cloud_storage::client::Client::new(
                google_cloud_storage::client::ClientConfig::default().anonymous(),
            )),
            "moved-gcs".into(),
            None,
        )
        .unwrap();
        let strict = outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &moved_gcs)
            .await;
        assert!(strict.err().unwrap().is::<IntentConflict>());
        let resumed = outbox
            .begin_or_resume_upload(&hash, 3, "video/mp4", &owner, &moved_gcs)
            .await;
        assert!(resumed.err().unwrap().is::<IntentConflict>());
        let pending = outbox.pending_intents(10).await.unwrap();
        assert_eq!(pending[0].mode, "gcs");
        assert_eq!(pending[0].gcs_bucket, "fixture-gcs");
        assert_eq!(pending[0].b2_bucket, None);
    }

    #[tokio::test]
    async fn mismatched_session_receipt_is_an_integrity_error_not_a_policy_conflict() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let hash = "a".repeat(64);
        let owner = "b".repeat(64);
        let source = ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-gcs".into(),
            key: hash.clone(),
            version: Some("1".into()),
        };
        outbox
            .begin_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                &fixture_storage(WriteMode::Gcs, None),
            )
            .await
            .unwrap();
        outbox
            .enqueue_completed_session("upload-1", &hash, 3, &owner, vec![source.clone()])
            .await
            .unwrap();
        outbox
            .enqueue_completed_session("upload-1", &hash, 3, &owner, vec![source.clone()])
            .await
            .expect("a matching receipt acknowledges the replay");
        let other_hash = "d".repeat(64);
        let other_owner = "c".repeat(64);
        for (field, hash, size, owner) in [
            ("hash", other_hash.as_str(), 3, owner.as_str()),
            ("size", hash.as_str(), 4, owner.as_str()),
            ("owner", hash.as_str(), 3, other_owner.as_str()),
        ] {
            let error = outbox
                .enqueue_completed_session("upload-1", hash, size, owner, vec![source.clone()])
                .await
                .unwrap_err();
            assert!(
                !error.is::<IntentConflict>(),
                "a {field} mismatch must not ask the client to reconcile storage policy"
            );
            assert_eq!(
                error.to_string(),
                "session receipt does not match the completed upload",
                "{field} mismatch"
            );
        }
    }

    #[tokio::test]
    async fn finalization_rejects_destinations_that_do_not_match_the_intent() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        use crate::storage::StorageProvider;
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let storage = fixture_storage(WriteMode::Dual, Some("fixture-b2"));
        let hash = "a".repeat(64);
        let owner = "b".repeat(64);
        let gcs = ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-gcs".into(),
            key: hash.clone(),
            version: Some("1".into()),
        };
        let b2 = ObjectRef {
            provider: StorageProvider::B2,
            bucket: "fixture-b2".into(),
            key: hash.clone(),
            version: Some("v1".into()),
        };
        let mut wrong_bucket = b2.clone();
        wrong_bucket.bucket = "another-bucket".into();
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        for objects in [
            vec![gcs.clone()],
            vec![gcs.clone(), gcs.clone()],
            vec![b2.clone(), gcs.clone()],
            vec![gcs.clone(), wrong_bucket],
            vec![gcs.clone(), b2.clone(), b2.clone()],
        ] {
            let result = outbox
                .enqueue_upload(&hash, 3, "video/mp4", &owner, objects)
                .await;
            assert!(
                result.is_err(),
                "mismatched destination set must not complete a dual intent"
            );
            assert_eq!(outbox.pending_intents(10).await.unwrap().len(), 1);
            assert!(outbox.source_objects(&hash).await.unwrap().is_empty());
        }
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![gcs.clone(), b2.clone()])
            .await
            .unwrap();
        // A late dual attempt cannot overwrite a subsequent GCS admission's
        // pending state after the earlier upload has already completed.
        outbox
            .begin_upload(
                &hash,
                3,
                "video/mp4",
                &owner,
                &storage.with_mode(WriteMode::Gcs).unwrap(),
            )
            .await
            .unwrap();
        assert!(outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![gcs.clone(), b2])
            .await
            .is_err());
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![gcs])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn direct_retry_after_mode_switch_preserves_pending_destinations() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        use axum::{
            http::{Method, Uri},
            response::IntoResponse,
            routing::any,
            Json, Router,
        };
        use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
        use sha2::{Digest, Sha256};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let bytes = bytes::Bytes::from_static(b"pending-original");
        let hash = hex::encode(Sha256::digest(&bytes));
        let size = bytes.len() as u64;
        let b2_reads = Arc::new(AtomicUsize::new(0));
        let seen = b2_reads.clone();
        let body = bytes.clone();
        let fixture_hash = hash.clone();
        let app = Router::new().route("/*path", any(move |method: Method, uri: Uri| {
            let body = body.clone();
            let hash = fixture_hash.clone();
            let seen = seen.clone();
            async move {
                if uri.path().contains("b2-primary-writes") {return (axum::http::StatusCode::NOT_FOUND,Json(serde_json::json!({"error":{"code":404,"message":"No such object","errors":[]}}))).into_response();}

                assert!(method == Method::GET || method == Method::HEAD,
                    "already stored originals must be verified without overwriting");
                if uri.path().starts_with("/storage/") {
                    if uri.query().unwrap_or_default().contains("alt=media") {
                        return body.into_response();
                    }
                    return Json(serde_json::json!({"name":hash,"bucket":"fixture-gcs","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"fixture","size":body.len().to_string(),"contentType":"application/octet-stream"})).into_response();
                }
                if method == Method::GET { seen.fetch_add(1, Ordering::SeqCst); }
                let mut response = if method == Method::HEAD { "".into_response() } else { body.clone().into_response() };
                response.headers_mut().insert("x-amz-version-id", "fixture-v1".parse().unwrap());
                response.headers_mut().insert("content-length", body.len().to_string().parse().unwrap());
                response
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let b2 = crate::storage::B2Store::new(
            "fixture-b2".into(),
            "us-west-001".into(),
            "fixture".into(),
            "fixture".into(),
        )
        .unwrap()
        .with_endpoint(endpoint.clone());
        let gcs = GcsClient::new(
            ClientConfig {
                storage_endpoint: endpoint,
                ..Default::default()
            }
            .anonymous(),
        );
        let dual =
            MediaStorage::new(WriteMode::Dual, Some(gcs), "fixture-gcs".into(), Some(b2)).unwrap();
        dual.initialize_coordination(Some(&url)).await.unwrap();

        for (index, (original, current)) in [
            (WriteMode::Dual, WriteMode::Gcs),
            (WriteMode::Gcs, WriteMode::Dual),
        ]
        .into_iter()
        .enumerate()
        {
            let owner = format!("{:064x}", index + 1);
            let outbox =
                ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                    .await
                    .unwrap();
            outbox
                .begin_upload(
                    &hash,
                    size,
                    "application/octet-stream",
                    &owner,
                    &dual.with_mode(original).unwrap(),
                )
                .await
                .unwrap();
            drop(outbox);
            let outbox =
                ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                    .await
                    .unwrap();
            let before = b2_reads.load(Ordering::SeqCst);
            let result = crate::stream_to_gcs_with_hash(
                &dual.with_mode(current).unwrap(),
                Some(&outbox),
                "application/octet-stream",
                axum::body::Body::from(bytes.clone()),
                &owner,
                None,
            )
            .await;
            assert!(
                result.is_ok(),
                "pending {original:?} retry under {current:?} admission failed: {:?}",
                result.err()
            );
            let payload: serde_json::Value = outbox
                .database
                .lock()
                .await
                .query_one(
                    "SELECT payload FROM upload_processing_outbox WHERE payload->>'owner'=$1",
                    &[&owner],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(payload["source"]["provider"], "gcs");
            if original == WriteMode::Dual {
                assert_eq!(payload["replicas"][0]["provider"], "b2");
                assert!(b2_reads.load(Ordering::SeqCst) > before);
            } else {
                assert!(payload.get("replicas").is_none());
                assert_eq!(b2_reads.load(Ordering::SeqCst), before);
            }
            assert!(outbox.pending_intents(10).await.unwrap().is_empty());
        }
        // The stale GCS intent's completion is now the newest event for these
        // bytes, but the first owner already required a B2 copy of them.
        // Derivative writers must keep that destination rather than narrow to GCS.
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let providers: Vec<StorageProvider> = outbox
            .source_objects(&hash)
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.provider)
            .collect();
        assert_eq!(providers, vec![StorageProvider::Gcs, StorageProvider::B2]);
        server.abort();
    }

    #[tokio::test]
    async fn source_objects_keep_each_recorded_provider_at_its_newest_version() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let storage = fixture_storage(WriteMode::Gcs, Some("fixture-b2"));
        let hash = format!("{:064x}", rand::random::<u128>());
        let object = |provider, bucket: &str, version: &str| ObjectRef {
            provider,
            bucket: bucket.into(),
            key: hash.clone(),
            version: Some(version.into()),
        };
        let gcs_v1 = object(StorageProvider::Gcs, "fixture-gcs", "v1");
        let b2_v1 = object(StorageProvider::B2, "fixture-b2", "b2-v1");
        let gcs_v2 = object(StorageProvider::Gcs, "fixture-gcs", "v2");
        let b2_v2 = object(StorageProvider::B2, "fixture-b2", "b2-v2");
        let finalize = |owner: u8, mode: WriteMode, objects: Vec<ObjectRef>| {
            let outbox = outbox.clone();
            let storage = storage.with_mode(mode).unwrap();
            let hash = hash.clone();
            async move {
                let owner = format!("{owner:064x}");
                outbox
                    .begin_upload(&hash, 3, "video/mp4", &owner, &storage)
                    .await
                    .unwrap();
                outbox
                    .enqueue_upload(&hash, 3, "video/mp4", &owner, objects)
                    .await
                    .unwrap();
            }
        };
        finalize(1, WriteMode::Dual, vec![gcs_v1, b2_v1.clone()]).await;
        finalize(2, WriteMode::Gcs, vec![gcs_v2.clone()]).await;
        assert_eq!(
            outbox.source_objects(&hash).await.unwrap(),
            vec![gcs_v2.clone(), b2_v1],
            "a newer GCS-only event keeps the older B2 copy"
        );
        finalize(3, WriteMode::B2, vec![b2_v2.clone()]).await;
        assert_eq!(
            outbox.source_objects(&hash).await.unwrap(),
            vec![b2_v2, gcs_v2],
            "the newest event leads; a provider it lacks comes from its newest event"
        );
    }

    #[tokio::test]
    async fn unfinished_policy_conflict_is_explicit_and_keeps_original_mapping() {
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let outbox =
            ProcessingOutbox::connect(&url, "https://processing.invalid/finalized", "fixture")
                .await
                .unwrap();
        let gcs = google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig::default().anonymous(),
        );
        let gcs_storage =
            MediaStorage::new(WriteMode::Gcs, Some(gcs), "first-bucket".into(), None).unwrap();
        let b2_storage = MediaStorage::new(
            WriteMode::B2,
            None,
            "first-bucket".into(),
            Some(
                crate::storage::B2Store::new(
                    "second-bucket".into(),
                    "us-west-001".into(),
                    "fixture".into(),
                    "fixture".into(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let hash = "a".repeat(64);
        let owner = "b".repeat(64);
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &gcs_storage)
            .await
            .unwrap();
        let error = outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &b2_storage)
            .await
            .unwrap_err();
        assert!(error.is::<IntentConflict>());
        let pending = outbox.pending_intents(1).await.unwrap();
        assert_eq!(pending[0].mode, "gcs");
        assert_eq!(pending[0].gcs_bucket, "first-bucket");
        assert_eq!(pending[0].b2_bucket, None);
    }
    /// Run against an explicitly provided disposable database; never production.
    #[tokio::test]
    async fn postgres_intent_and_outbox_recover_duplicate_delivery() {
        let Some(database_url) = isolated_database_url().await else {
            return;
        };
        let _guard = DB_TEST_LOCK.lock().await;
        use crate::storage::StorageProvider;
        use axum::{routing::post, Json, Router};
        use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
        let accepted = Arc::new(std::sync::Mutex::new(
            std::collections::HashSet::<String>::new(),
        ));
        let seen = accepted.clone();
        let app=Router::new().route("/finalized",post(move |Json(event):Json<MediaFinalizedV1>| {
            let seen=seen.clone();
            async move {seen.lock().unwrap().insert(event.job_id.clone());Json(serde_json::json!({"schema_version":1,"durable":true,"job_id":event.job_id}))}
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/finalized", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let db = crate::coordination::connect(&database_url).await.unwrap();
        db.batch_execute(&schema()).await.unwrap();
        let outbox = ProcessingOutbox {
            database: Arc::new(Mutex::new(db)),
            consumer: Some(ProcessingClient::new_for_test(&endpoint, "fixture-token").unwrap()),
        };
        let storage = MediaStorage::new(
            WriteMode::Gcs,
            Some(GcsClient::new(ClientConfig::default().anonymous())),
            "fixture-bucket".into(),
            None,
        )
        .unwrap();
        let hash = format!("{:064x}", rand::random::<u128>());
        let owner = "b".repeat(64);
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        outbox
            .begin_upload(&hash, 3, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        assert!(outbox
            .begin_upload(&hash, 4, "video/mp4", &owner, &storage)
            .await
            .is_err());
        outbox
            .begin_upload(&hash, 3, "application/octet-stream", &owner, &storage)
            .await
            .expect("a MIME declaration change must not strand identical bytes");
        let object = ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-bucket".into(),
            key: hash.clone(),
            version: Some("1".into()),
        };
        outbox
            .enqueue_upload(&hash, 3, "video/mp4", &owner, vec![object.clone()])
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 3, "application/octet-stream", &owner, vec![object])
            .await
            .unwrap();
        {
            let db = outbox.database.lock().await;
            let payload: serde_json::Value = db
                .query_one(
                    "SELECT payload FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
                    &[&hash],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(payload["content_type"], "video/mp4");
        }
        outbox
            .begin_upload(&hash, 3, "video/quicktime", &owner, &storage)
            .await
            .expect("completed intents must also accept declaration changes");
        assert!(!outbox
            .pending_intents(1000)
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.sha256 == hash));
        let id = {
            let db = outbox.database.lock().await;
            let rows = db
                .query(
                    "SELECT job_id FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
                    &[&hash],
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            rows[0].get::<_, String>(0)
        };
        let resolved = outbox.source_objects(&hash).await.unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].key, hash);

        assert_eq!(outbox.dispatch_once(1).await.unwrap(), 1);
        // Simulate durable consumer acceptance with a lost local acknowledgement.
        {
            let db = outbox.database.lock().await;
            db.execute("UPDATE upload_processing_outbox SET delivered_at=NULL,next_attempt_at=now() WHERE job_id=$1",&[&id]).await.unwrap();
        }
        assert_eq!(outbox.dispatch_once(1).await.unwrap(), 1);
        assert_eq!(accepted.lock().unwrap().len(), 1);
        {
            let db = outbox.database.lock().await;
            db.execute(
                "DELETE FROM upload_processing_outbox WHERE job_id=$1",
                &[&id],
            )
            .await
            .unwrap();
            db.execute(
                "DELETE FROM upload_processing_intents WHERE sha256=$1 AND owner=$2",
                &[&hash, &owner],
            )
            .await
            .unwrap();
        }
        server.abort();
    }

    #[tokio::test]
    async fn postgres_recovery_streams_and_verifies_an_interrupted_original() {
        let Some(database_url) = isolated_database_url().await else {
            return;
        };
        let _guard = DB_TEST_LOCK.lock().await;
        use crate::storage::B2Store;
        use axum::{http::Method, response::IntoResponse, routing::any, Router};
        use sha2::{Digest, Sha256};
        let bytes = bytes::Bytes::from(format!("original-{:032x}", rand::random::<u128>()));
        let hash = hex::encode(Sha256::digest(&bytes));
        let size = bytes.len() as u64;
        let body = bytes.clone();
        let app = Router::new().route(
            "/*path",
            any(move |method: Method| {
                let body = body.clone();
                async move {
                    let mut response = if method == Method::HEAD {
                        "".into_response()
                    } else {
                        body.clone().into_response()
                    };
                    let headers = response.headers_mut();
                    headers.insert("x-amz-version-id", "fixture-v1".parse().unwrap());
                    headers.insert("content-length", body.len().to_string().parse().unwrap());
                    response
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let b2 = B2Store::new(
            "fixture-bucket".into(),
            "us-west-001".into(),
            "key".into(),
            "secret".into(),
        )
        .unwrap()
        .with_endpoint(endpoint.clone());
        let storage = MediaStorage::new(WriteMode::B2, None, "".into(), Some(b2.clone())).unwrap();
        // The same readable object behind a changed required bucket.
        let moved_b2 = MediaStorage::new(
            WriteMode::B2,
            None,
            "".into(),
            Some(
                B2Store::new(
                    "moved-bucket".into(),
                    "us-west-001".into(),
                    "key".into(),
                    "secret".into(),
                )
                .unwrap()
                .with_endpoint(endpoint),
            ),
        )
        .unwrap();
        // A later deployment configures a GCS bucket that B2 mode never writes.
        let gained_gcs_bucket =
            MediaStorage::new(WriteMode::B2, None, "fixture-gcs".into(), Some(b2)).unwrap();
        let db = crate::coordination::connect(&database_url).await.unwrap();
        db.batch_execute(&schema()).await.unwrap();
        let outbox = ProcessingOutbox {
            database: Arc::new(Mutex::new(db)),
            consumer: Some(
                ProcessingClient::new("https://processor.invalid/finalized", "fixture-token")
                    .unwrap(),
            ),
        };
        let owner = "b".repeat(64);
        outbox
            .begin_upload(&hash, size, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        assert_eq!(
            outbox
                .recover_intents(&storage, 1000, size - 1)
                .await
                .unwrap(),
            0
        );
        assert!(outbox
            .pending_intents(1000)
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.sha256 == hash));
        assert_eq!(
            outbox.recover_intents(&moved_b2, 1000, size).await.unwrap(),
            0,
            "a changed required bucket must never be recovered"
        );
        assert_eq!(outbox.pending_intents(1000).await.unwrap().len(), 1);
        assert_eq!(
            outbox
                .recover_intents(&gained_gcs_bucket, 1000, size)
                .await
                .unwrap(),
            1,
            "an unused adapter's bucket must not strand a recoverable intent"
        );
        let second_owner = "d".repeat(64);
        outbox
            .begin_upload(&hash, size, "video/mp4", &second_owner, &storage)
            .await
            .unwrap();
        assert_eq!(
            outbox.recover_intents(&storage, 1000, size).await.unwrap(),
            1
        );
        assert!(!outbox
            .pending_intents(1000)
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.sha256 == hash));
        let corrupt = "c".repeat(64);
        outbox
            .begin_upload(&corrupt, size, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        assert_eq!(
            outbox.recover_intents(&storage, 1000, size).await.unwrap(),
            0
        );
        assert!(outbox
            .pending_intents(1000)
            .await
            .unwrap()
            .iter()
            .any(|intent| intent.sha256 == corrupt));
        {
            let db = outbox.database.lock().await;
            db.execute(
                "DELETE FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
                &[&hash],
            )
            .await
            .unwrap();
            db.execute(
                "DELETE FROM upload_processing_intents WHERE sha256=$1 OR sha256=$2",
                &[&hash, &corrupt],
            )
            .await
            .unwrap();
        }
        server.abort();
    }
    #[tokio::test]
    async fn postgres_partial_dual_write_is_never_repaired_in_background() {
        let Some(database_url) = isolated_database_url().await else {
            return;
        };
        let _guard = DB_TEST_LOCK.lock().await;
        use crate::storage::B2Store;
        use axum::{
            extract::State,
            http::{Method, StatusCode, Uri},
            response::IntoResponse,
            routing::any,
            Json, Router,
        };
        use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
        use sha2::{Digest, Sha256};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        #[derive(Default)]
        struct Fixture {
            b2_present: AtomicBool,
            writes: AtomicUsize,
            reads: AtomicUsize,
        }
        let fixture = Arc::new(Fixture::default());
        let bytes = bytes::Bytes::from(format!("dual-original-{:032x}", rand::random::<u128>()));
        let hash = hex::encode(Sha256::digest(&bytes));
        let size = bytes.len() as u64;
        let fixture_hash = hash.clone();
        let body = bytes.clone();
        let app=Router::new().route("/*path",any(move |State(f):State<Arc<Fixture>>,method:Method,uri:Uri| {
            let body=body.clone();let hash=fixture_hash.clone();
            async move {
                if uri.path().contains("b2-primary-writes") {return (StatusCode::NOT_FOUND,Json(serde_json::json!({"error":{"code":404,"message":"No such object","errors":[]}}))).into_response();}

                if method!=Method::HEAD && method!=Method::GET {f.writes.fetch_add(1,Ordering::SeqCst);return StatusCode::INTERNAL_SERVER_ERROR.into_response();}
                if uri.path().starts_with("/storage/") {
                    if uri.query().unwrap_or_default().contains("alt=media"){f.reads.fetch_add(1,Ordering::SeqCst);return body.into_response();}
                    return Json(serde_json::json!({"name":hash,"bucket":"fixture-gcs","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"fixture","size":body.len().to_string(),"contentType":"video/mp4"})).into_response();
                }
                if !f.b2_present.load(Ordering::SeqCst){return StatusCode::NOT_FOUND.into_response();}
                if method==Method::GET {f.reads.fetch_add(1,Ordering::SeqCst);}
                let mut response=if method==Method::HEAD {"".into_response()}else{body.clone().into_response()};
                response.headers_mut().insert("x-amz-version-id","fixture-v1".parse().unwrap());
                response.headers_mut().insert("content-length",body.len().to_string().parse().unwrap());
                response
            }
        })).with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let b2 = B2Store::new(
            "fixture-b2".into(),
            "us-west-001".into(),
            "key".into(),
            "secret".into(),
        )
        .unwrap()
        .with_endpoint(endpoint.clone());
        let gcs = GcsClient::new(
            ClientConfig {
                storage_endpoint: endpoint,
                ..Default::default()
            }
            .anonymous(),
        );
        let storage =
            MediaStorage::new(WriteMode::Dual, Some(gcs), "fixture-gcs".into(), Some(b2)).unwrap();
        let db = crate::coordination::connect(&database_url).await.unwrap();
        db.batch_execute(&schema()).await.unwrap();
        let outbox = ProcessingOutbox {
            database: Arc::new(Mutex::new(db)),
            consumer: Some(
                ProcessingClient::new("https://processor.invalid/finalized", "fixture-token")
                    .unwrap(),
            ),
        };
        let owner = "b".repeat(64);
        outbox
            .begin_upload(&hash, size, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        assert_eq!(
            outbox
                .recover_intents(&storage, 1000, size * 2)
                .await
                .unwrap(),
            0
        );
        assert_eq!(fixture.writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            fixture.reads.load(Ordering::SeqCst),
            0,
            "a partial write must not repeatedly download the surviving replica"
        );
        fixture.b2_present.store(true, Ordering::SeqCst);
        assert_eq!(
            outbox
                .recover_intents(&storage, 1000, size * 2)
                .await
                .unwrap(),
            1
        );
        assert_eq!(fixture.writes.load(Ordering::SeqCst), 0);
        {
            let db = outbox.database.lock().await;
            let payload: serde_json::Value = db
                .query_one(
                    "SELECT payload FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
                    &[&hash],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(payload["source"]["provider"], "gcs");
            assert_eq!(payload["replicas"][0]["provider"], "b2");
            db.execute(
                "DELETE FROM upload_processing_outbox WHERE payload->>'sha256'=$1",
                &[&hash],
            )
            .await
            .unwrap();
            db.execute(
                "DELETE FROM upload_processing_intents WHERE sha256=$1",
                &[&hash],
            )
            .await
            .unwrap();
        }
        server.abort();
    }
}
