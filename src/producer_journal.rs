//! Durable preparation only: no parent gate or storage dispatch is available.
use super::*;
use rand::{rngs::OsRng, RngCore};
use std::collections::BTreeMap;
use tokio_postgres::GenericClient;

pub(super) const SCHEMA: &str = include_str!("../database/migrations/004_producer_journal.sql");

const FILES: [&str; 7] = [
    "stream_720p.ts",
    "stream_720p.mp4",
    "stream_480p.ts",
    "stream_480p.mp4",
    "stream_720p.m3u8",
    "stream_480p.m3u8",
    "master.m3u8",
];

#[derive(Debug, thiserror::Error)]
pub(super) enum JournalError {
    #[error("invalid producer journal request")]
    Invalid,
    #[error("producer assignment is stale")]
    Stale,
    #[error("conflicting producer artifact plan")]
    Conflict,
    #[error("producer parent gate unavailable")]
    GateUnavailable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Begin {
    pub lease: Lease,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Prepare {
    pub lease: Lease,
    pub attempt_id: String,
    pub artifacts: Vec<Artifact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inspect {
    pub attempt_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dispatch {
    pub lease: Lease,
    pub attempt_id: String,
    pub filename: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Artifact {
    filename: String,
    sha256: String,
    size: u64,
    content_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Target {
    provider: StorageProvider,
    bucket: String,
    key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PlannedArtifact {
    filename: String,
    target: Target,
    sha256: String,
    size: u64,
    content_type: String,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub(super) struct Attempt {
    attempt_id: String,
    pub(super) processing_job_id: String,
    pub(super) lease_generation: i64,
    pub(super) input_revision: i64,
    source: ObjectRef,
    pub(super) sha256: String,
    size: u64,
    content_type_hint: String,
    pipeline_version: u32,
    pub(super) phase: String,
    artifacts: Vec<PlannedArtifact>,
}

pub(super) fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_lease(lease: &Lease) -> Result<()> {
    if !is_hash(&lease.processing_job_id)
        || lease.lease_generation <= 0
        || lease.input_revision <= 0
    {
        return Err(JournalError::Invalid.into());
    }
    Ok(())
}

pub(super) async fn live_job(
    client: &tokio_postgres::Transaction<'_>,
    lease: &Lease,
) -> Result<()> {
    validate_lease(lease)?;
    if client
        .query_opt(
            "SELECT 1 FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
            &[&lease.processing_job_id],
        )
        .await?
        .is_none()
    {
        return Err(JournalError::Stale.into());
    }
    // Time predicates must be evaluated after any wait for the identity lock.
    if client.query_opt(
        "SELECT 1 FROM media_processing_jobs WHERE processing_job_id=$1 AND lease_generation=$2 AND input_revision=$3 AND state='running' AND lease_until>clock_timestamp() AND attempt_deadline>clock_timestamp()",
        &[&lease.processing_job_id,&lease.lease_generation,&lease.input_revision],
    ).await?.is_none() {
        return Err(JournalError::Stale.into());
    }
    Ok(())
}

pub(super) async fn read_attempt<C: GenericClient + Sync>(
    client: &C,
    id: &str,
) -> Result<Option<Attempt>> {
    let Some(row) = client.query_opt(
        "SELECT a.processing_job_id,a.lease_generation,a.input_revision,a.input,a.phase,COALESCE((SELECT jsonb_agg(jsonb_build_object('filename',o.filename,'target',o.target,'sha256',o.sha256,'size',o.size,'content_type',o.content_type,'metadata',o.metadata) ORDER BY o.position) FROM media_storage_operations o WHERE o.attempt_id=a.attempt_id),'[]'::jsonb) FROM media_storage_attempts a WHERE attempt_id=$1 AND subject_kind='producer'",
        &[&id],
    ).await? else { return Ok(None); };
    let input: ProcessingInput = serde_json::from_value(row.get(3))?;
    Ok(Some(Attempt {
        attempt_id: id.into(),
        processing_job_id: row.get(0),
        lease_generation: row.get(1),
        input_revision: row.get(2),
        source: input.source,
        sha256: input.sha256,
        size: input.size,
        content_type_hint: input.content_type,
        pipeline_version: input.pipeline_version,
        phase: row.get(4),
        artifacts: serde_json::from_value(row.get(5))?,
    }))
}

fn plan(attempt: &Attempt, artifacts: Vec<Artifact>) -> Result<Vec<PlannedArtifact>> {
    if artifacts.len() != FILES.len() {
        return Err(JournalError::Invalid.into());
    }
    let mut entries = BTreeMap::new();
    let mut total = 0u64;
    for artifact in artifacts {
        let mime = if artifact.filename.ends_with(".ts") {
            "video/mp2t"
        } else if artifact.filename.ends_with(".mp4") {
            "video/mp4"
        } else {
            "application/vnd.apple.mpegurl"
        };
        if !FILES.contains(&artifact.filename.as_str())
            || !is_hash(&artifact.sha256)
            || artifact.size == 0
            || artifact.size > 32 * 1024 * 1024
            || (artifact.filename.ends_with(".m3u8") && artifact.size > 64 * 1024)
            || artifact.content_type != mime
        {
            return Err(JournalError::Invalid.into());
        }
        total += artifact.size;
        let filename = artifact.filename.clone();
        if entries.insert(filename, artifact).is_some() {
            return Err(JournalError::Invalid.into());
        }
    }
    if total > 64 * 1024 * 1024 {
        return Err(JournalError::Invalid.into());
    }
    Ok(FILES
        .iter()
        .map(|filename| {
            let artifact = entries
                .remove(*filename)
                .expect("exact bounded filename set");
            PlannedArtifact {
                target: Target {
                    provider: attempt.source.provider,
                    bucket: attempt.source.bucket.clone(),
                    key: format!(
                        "{}/revisions/v1/{}/{}",
                        attempt.sha256, attempt.attempt_id, filename
                    ),
                },
                metadata: BTreeMap::from([
                    ("source-sha256".into(), attempt.sha256.clone()),
                    ("derivative".into(), "hls".into()),
                    ("hls-filename".into(), filename.to_string()),
                ]),
                filename: artifact.filename,
                sha256: artifact.sha256,
                size: artifact.size,
                content_type: artifact.content_type,
            }
        })
        .collect())
}

impl InboxStore {
    pub(super) async fn journal_begin(&self, lease: &Lease) -> Result<Attempt> {
        let mut client = self.database.lock().await;
        let tx = client.transaction().await?;
        live_job(&tx, lease).await?;
        let mut nonce = [0u8; 16];
        OsRng.fill_bytes(&mut nonce);
        let nonce = hex::encode(nonce);
        let row = tx.query_opt(
            "INSERT INTO media_storage_attempts(attempt_id,processing_job_id,lease_generation,input_revision,input) SELECT $4,processing_job_id,lease_generation,input_revision,input FROM media_processing_jobs WHERE processing_job_id=$1 AND lease_generation=$2 AND input_revision=$3 AND state='running' AND lease_until>clock_timestamp() AND attempt_deadline>clock_timestamp() ON CONFLICT(processing_job_id,lease_generation,input_revision) DO UPDATE SET attempt_id=media_storage_attempts.attempt_id RETURNING attempt_id",
            &[&lease.processing_job_id,&lease.lease_generation,&lease.input_revision,&nonce],
        ).await?.ok_or(JournalError::Stale)?;
        let id: String = row.get(0);
        let result = read_attempt(&tx, &id).await?.ok_or(JournalError::Stale)?;
        tx.commit().await?;
        Ok(result)
    }

    pub(super) async fn journal_prepare(&self, input: Prepare) -> Result<Attempt> {
        if !valid_id(&input.attempt_id) {
            return Err(JournalError::Invalid.into());
        }
        let mut client = self.database.lock().await;
        let tx = client.transaction().await?;
        live_job(&tx, &input.lease).await?;
        let attempt = read_attempt(&tx, &input.attempt_id)
            .await?
            .ok_or(JournalError::Stale)?;
        if attempt.processing_job_id != input.lease.processing_job_id
            || attempt.lease_generation != input.lease.lease_generation
            || attempt.input_revision != input.lease.input_revision
        {
            return Err(JournalError::Stale.into());
        }
        let artifacts = plan(&attempt, input.artifacts)?;
        if attempt.phase == "planned" {
            if attempt.artifacts != artifacts {
                return Err(JournalError::Conflict.into());
            }
        } else {
            for (position, artifact) in artifacts.iter().enumerate() {
                tx.execute(
                    "INSERT INTO media_storage_operations(attempt_id,position,filename,target,sha256,size,content_type,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[&input.attempt_id,&(position as i32),&artifact.filename,&serde_json::to_value(&artifact.target)?,&artifact.sha256,&(artifact.size as i64),&artifact.content_type,&serde_json::to_value(&artifact.metadata)?],
                ).await?;
            }
            if tx.execute(
                "UPDATE media_storage_attempts a SET phase='planned' FROM media_processing_jobs j WHERE a.attempt_id=$4 AND j.processing_job_id=a.processing_job_id AND j.processing_job_id=$1 AND j.lease_generation=$2 AND j.input_revision=$3 AND j.state='running' AND j.lease_until>clock_timestamp() AND j.attempt_deadline>clock_timestamp()",
                &[&input.lease.processing_job_id,&input.lease.lease_generation,&input.lease.input_revision,&input.attempt_id],
            ).await? != 1 {
                return Err(JournalError::Stale.into());
            }
        }
        let result = read_attempt(&tx, &input.attempt_id)
            .await?
            .ok_or(JournalError::Stale)?;
        tx.commit().await?;
        Ok(result)
    }

    pub(super) async fn journal_inspect(&self, id: &str) -> Result<Option<Attempt>> {
        if !valid_id(id) {
            return Err(JournalError::Invalid.into());
        }
        read_attempt(&*self.database.lock().await, id).await
    }

    pub(super) async fn journal_dispatch(&self, input: Dispatch) -> Result<()> {
        if !valid_id(&input.attempt_id) || !FILES.contains(&input.filename.as_str()) {
            return Err(JournalError::Invalid.into());
        }
        let mut client = self.database.lock().await;
        let tx = client.transaction().await?;
        live_job(&tx, &input.lease).await?;
        let attempt = read_attempt(&tx, &input.attempt_id)
            .await?
            .ok_or(JournalError::Stale)?;
        if attempt.processing_job_id != input.lease.processing_job_id
            || attempt.lease_generation != input.lease.lease_generation
            || attempt.input_revision != input.lease.input_revision
            || attempt.phase != "planned"
        {
            return Err(JournalError::Stale.into());
        }
        // A stored plan is not exclusion or permission to perform a provider write.
        Err(JournalError::GateUnavailable.into())
    }

    pub(super) async fn journal_completion_replay(
        &self,
        lease: &Lease,
        marker: &ObjectRef,
    ) -> Result<bool> {
        validate_lease(lease)?;
        validate_reference(marker).map_err(|_| InvalidCompletion)?;
        let client = self.database.lock().await;
        let row = client.query_opt(
            "SELECT state,lease_generation,input_revision,manifest,COALESCE(lease_until>clock_timestamp() AND attempt_deadline>clock_timestamp(),false) FROM media_processing_jobs WHERE processing_job_id=$1",
            &[&lease.processing_job_id],
        ).await?.ok_or(JournalError::Stale)?;
        if row.get::<_, i64>(1) != lease.lease_generation
            || row.get::<_, i64>(2) != lease.input_revision
        {
            return Err(JournalError::Stale.into());
        }
        let state: String = row.get(0);
        if state == "completed" {
            return Ok(
                row.get::<_, Option<serde_json::Value>>(3) == Some(serde_json::to_value(marker)?)
            );
        }
        if state != "running" || !row.get::<_, bool>(4) {
            return Err(JournalError::Stale.into());
        }
        Err(JournalError::GateUnavailable.into())
    }
}
