//! Durable intake and reservations; neither is publication or policy approval.
use crate::processing::MediaFinalizedV1;
use crate::storage::{ObjectRef, StorageProvider};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct InboxStore {
    pub(crate) database: Arc<Mutex<tokio_postgres::Client>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assignment {
    pub schema_version: u32,
    pub processing_job_id: String,
    pub lease_generation: i64,
    pub input_revision: i64,
    pub source: ObjectRef,
    pub destinations: Vec<ObjectRef>,
    pub sha256: String,
    pub size: u64,
    pub content_type: String,
    pub pipeline_version: u32,
    pub lease_expires_at: i64,
    pub attempt_deadline: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lease {
    processing_job_id: String,
    lease_generation: i64,
    input_revision: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessingReceipt {
    schema_version: u32,
    processing_job_id: String,
    input_revision: i64,
    lease_generation: i64,
    state: String,
    source: ObjectRef,
    destinations: Vec<ObjectRef>,
    sha256: String,
    size: u64,
    content_type: String,
    pipeline_version: u32,
    manifest: Option<ObjectRef>,
}

impl From<&Assignment> for Lease {
    fn from(value: &Assignment) -> Self {
        Self {
            processing_job_id: value.processing_job_id.clone(),
            lease_generation: value.lease_generation,
            input_revision: value.input_revision,
        }
    }
}

struct InboxSettings {
    intake_token: String,
    worker_token: String,
    producer_journal_enabled: bool,
}

impl InboxSettings {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let producer_journal_enabled = match lookup("MEDIA_PROCESSING_JOURNAL_ENABLED").as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            _ => bail!("MEDIA_PROCESSING_JOURNAL_ENABLED must be true or false"),
        };
        match lookup("MEDIA_FINALIZED_INBOX_ENABLED").as_deref() {
            None | Some("false") => {
                if producer_journal_enabled {
                    bail!("producer journal requires the finalization inbox");
                }
                return Ok(None);
            }
            Some("true") => {}
            _ => bail!("MEDIA_FINALIZED_INBOX_ENABLED must be true or false"),
        }
        let mut read_token = |name| -> Result<String> {
            let token =
                lookup(name).ok_or_else(|| anyhow!("inbox intake and worker tokens required"))?;
            if !(16..=1024).contains(&token.len())
                || !token.bytes().all(|b| (33..=126).contains(&b))
            {
                bail!("invalid inbox credential configuration");
            }
            Ok(token)
        };
        let intake_token = read_token("MEDIA_FINALIZED_INBOX_TOKEN")?;
        let worker_token = read_token("MEDIA_PROCESSING_WORKER_TOKEN")?;
        if intake_token == worker_token {
            bail!("inbox intake and worker credentials must differ");
        }
        Ok(Some(Self {
            intake_token,
            worker_token,
            producer_journal_enabled,
        }))
    }
}

#[derive(Clone)]
pub struct InboxRuntime {
    store: InboxStore,
    settings: Arc<InboxSettings>,
    deadline: std::time::Duration,
    parent_gate: Option<Arc<parent_gate::Client>>,
}

#[path = "parent_gate.rs"]
mod parent_gate;

#[path = "inbox_http.rs"]
mod http;
pub use http::{configured, router};

#[path = "producer_journal.rs"]
mod journal;

const INBOX_SCHEMA: &str = include_str!("../database/migrations/003_finalization_inbox.sql");

#[derive(Debug, thiserror::Error)]
#[error("conflicting durable finalization input")]
struct InboxConflict;

#[derive(Debug, thiserror::Error)]
#[error("invalid private completion marker")]
struct InvalidCompletion;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProcessingInput {
    source: ObjectRef,
    sha256: String,
    size: u64,
    content_type: String,
    pipeline_version: u32,
}

impl From<&MediaFinalizedV1> for ProcessingInput {
    fn from(event: &MediaFinalizedV1) -> Self {
        Self {
            source: event.source.clone(),
            sha256: event.sha256.clone(),
            size: event.size,
            content_type: event.content_type.clone(),
            pipeline_version: event.pipeline_version,
        }
    }
}

impl InboxStore {
    async fn lookup(&self, id: &str) -> Result<Option<ProcessingReceipt>> {
        if !is_hash(id) {
            bail!("invalid processing identity");
        }
        let client = self.database.lock().await;
        // One statement supplies a consistent MVCC snapshot of input, fence,
        // completion and destinations without mixing two transaction versions.
        let Some(row)=client.query_opt("SELECT input,input_revision,lease_generation,state,manifest,COALESCE((SELECT jsonb_agg(d.object_ref ORDER BY d.identity_key) FROM media_processing_destinations d WHERE d.processing_job_id=j.processing_job_id),'[]'::jsonb) FROM media_processing_jobs j WHERE processing_job_id=$1",&[&id]).await.map_err(|_|anyhow!("processing receipt lookup failed"))? else {return Ok(None);};
        let input: ProcessingInput = serde_json::from_value(row.get(0))?;
        Ok(Some(ProcessingReceipt {
            schema_version: 1,
            processing_job_id: id.into(),
            input_revision: row.get(1),
            lease_generation: row.get(2),
            state: row.get(3),
            manifest: row
                .get::<_, Option<serde_json::Value>>(4)
                .map(serde_json::from_value)
                .transpose()?,
            destinations: serde_json::from_value(row.get(5))?,
            source: input.source,
            sha256: input.sha256,
            size: input.size,
            content_type: input.content_type,
            pipeline_version: input.pipeline_version,
        }))
    }
    pub(crate) async fn from_client(client: tokio_postgres::Client) -> Result<Self> {
        client
            .batch_execute("SET statement_timeout='5s'; SET lock_timeout='3s';")
            .await
            .map_err(|_| anyhow!("inbox database limits initialization failed"))?;
        client
            .batch_execute(include_str!(
                "../database/migrations/001_upload_coordination.sql"
            ))
            .await
            .map_err(|_| anyhow!("upload session schema initialization failed"))?;
        crate::outbox::initialize_schema(&client).await?;
        client
            .batch_execute(INBOX_SCHEMA)
            .await
            .map_err(|_| anyhow!("inbox schema initialization failed"))?;
        client
            .batch_execute(journal::SCHEMA)
            .await
            .map_err(|_| anyhow!("producer journal schema initialization failed"))?;
        client
            .batch_execute(parent_gate::SCHEMA)
            .await
            .map_err(|_| anyhow!("parent gate journal schema initialization failed"))?;
        client
            .batch_execute(include_str!(
                "../database/migrations/006_original_write_authority.sql"
            ))
            .await
            .map_err(|_| anyhow!("original preparation schema initialization failed"))?;
        Ok(Self {
            database: Arc::new(Mutex::new(client)),
        })
    }

    async fn accept(&self, event: &MediaFinalizedV1, key: &str) -> Result<()> {
        let id = validate_event(event, key)?;
        let input = serde_json::to_value(ProcessingInput::from(event))?;
        let payload = serde_json::to_value(event)?;
        let mut client = self.database.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|_| anyhow!("inbox transaction unavailable"))?;
        tx.execute("INSERT INTO media_processing_jobs(processing_job_id,input) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&id,&input]).await.map_err(|_|anyhow!("inbox work persistence failed"))?;
        let stored = tx
            .query_one(
                "SELECT input FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
                &[&id],
            )
            .await
            .map_err(|_| anyhow!("inbox work lookup failed"))?;
        let accepted: ProcessingInput = serde_json::from_value(stored.get(0))?;
        // The source/pipeline work is shared across owner-specific events.
        // Preserve the first MIME hint instead of rejecting another owner's
        // valid declaration or retargeting work. Verify actual MIME at source.
        if accepted.source != event.source
            || accepted.sha256 != event.sha256
            || accepted.size != event.size
            || accepted.pipeline_version != event.pipeline_version
        {
            return Err(InboxConflict.into());
        }
        if let Some(existing) = tx
            .query_opt(
                "SELECT payload FROM media_finalization_inbox WHERE job_id=$1",
                &[&event.job_id],
            )
            .await
            .map_err(|_| anyhow!("inbox event lookup failed"))?
        {
            if existing.get::<_, serde_json::Value>(0) != payload {
                return Err(InboxConflict.into());
            }
            tx.commit()
                .await
                .map_err(|_| anyhow!("inbox replay commit failed"))?;
            return Ok(());
        }
        let mut added = 0;
        for object in std::iter::once(&event.source).chain(&event.replicas) {
            let identity = hex::encode(Sha256::digest(serde_json::to_vec(object)?));
            let value = serde_json::to_value(object)?;
            added+=tx.execute("INSERT INTO media_processing_destinations(processing_job_id,identity_key,object_ref) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",&[&id,&identity,&value]).await.map_err(|_|anyhow!("inbox destination persistence failed"))?;
        }
        let count: i64 = tx
            .query_one(
                "SELECT count(*) FROM media_processing_destinations WHERE processing_job_id=$1",
                &[&id],
            )
            .await
            .map_err(|_| anyhow!("inbox destination bound lookup failed"))?
            .get(0);
        if count > 16 {
            return Err(InboxConflict.into());
        }
        if added > 0 {
            // Keep the live lease to avoid overlapping writers; its old revision
            // can no longer renew or complete. Reclaim waits for normal expiry.
            tx.execute("UPDATE media_processing_jobs SET input_revision=input_revision+1,attempts_in_revision=0,state=CASE WHEN state IN ('completed','blocked') THEN 'pending' ELSE state END,manifest=NULL WHERE processing_job_id=$1",&[&id]).await.map_err(|_|anyhow!("inbox work revision failed"))?;
        }
        tx.execute("INSERT INTO media_finalization_inbox(job_id,processing_job_id,payload) VALUES($1,$2,$3)",&[&event.job_id,&id,&payload]).await.map_err(|_|anyhow!("inbox event persistence failed"))?;
        tx.commit()
            .await
            .map_err(|_| anyhow!("inbox durable commit failed"))?;
        Ok(())
    }

    async fn claim(&self) -> Result<Option<Assignment>> {
        let mut client = self.database.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|_| anyhow!("assignment transaction unavailable"))?;
        tx.execute("UPDATE media_processing_jobs SET state='blocked',failure_reason='attempt_budget_exhausted',lease_until=NULL WHERE processing_job_id IN (SELECT processing_job_id FROM media_processing_jobs WHERE state IN ('pending','running') AND attempts_in_revision>=3 AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 64)",&[]).await.map_err(|_|anyhow!("assignment budget update failed"))?;
        let Some(row)=tx.query_opt("SELECT processing_job_id,input,input_revision,lease_generation FROM media_processing_jobs WHERE state IN ('pending','running') AND attempts_in_revision<3 AND (lease_until IS NULL OR lease_until<=clock_timestamp()) ORDER BY created_at,processing_job_id FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await.map_err(|_|anyhow!("assignment selection failed"))? else {
            tx.commit().await.map_err(|_|anyhow!("assignment empty commit failed"))?;
            return Ok(None);
        };
        let id: String = row.get(0);
        let input: ProcessingInput = serde_json::from_value(row.get(1))?;
        let revision: i64 = row.get(2);
        let generation: i64 = row.get::<_, i64>(3) + 1;
        let timing=tx.query_one("UPDATE media_processing_jobs SET state='running',lease_generation=$2,attempts_in_revision=attempts_in_revision+1,lease_until=clock_timestamp()+interval '60 seconds',attempt_deadline=clock_timestamp()+interval '15 minutes',failure_reason=NULL WHERE processing_job_id=$1 RETURNING floor(extract(epoch FROM lease_until))::bigint,floor(extract(epoch FROM attempt_deadline))::bigint",&[&id,&generation]).await.map_err(|_|anyhow!("assignment lease persistence failed"))?;
        let objects=tx.query("SELECT object_ref FROM media_processing_destinations WHERE processing_job_id=$1 ORDER BY identity_key",&[&id]).await.map_err(|_|anyhow!("assignment destinations lookup failed"))?;
        let destinations = objects
            .iter()
            .map(|row| serde_json::from_value(row.get(0)))
            .collect::<std::result::Result<Vec<ObjectRef>, _>>()?;
        let assignment = Assignment {
            schema_version: 1,
            processing_job_id: id,
            lease_generation: generation,
            input_revision: revision,
            source: input.source,
            destinations,
            sha256: input.sha256,
            size: input.size,
            content_type: input.content_type,
            pipeline_version: input.pipeline_version,
            lease_expires_at: timing.get(0),
            attempt_deadline: timing.get(1),
        };
        tx.commit()
            .await
            .map_err(|_| anyhow!("assignment lease commit failed"))?;
        Ok(Some(assignment))
    }

    async fn renew(&self, assignment: &Lease) -> Result<bool> {
        let mut client = self.database.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|_| anyhow!("assignment renewal transaction unavailable"))?;
        // Acquire exclusion before evaluating time, including lock-only waits.
        if tx
            .query_opt(
                "SELECT 1 FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
                &[&assignment.processing_job_id],
            )
            .await
            .map_err(|_| anyhow!("assignment renewal lookup failed"))?
            .is_none()
        {
            return Ok(false);
        }
        let renewed = tx.execute("UPDATE media_processing_jobs SET lease_until=LEAST(clock_timestamp()+interval '60 seconds',attempt_deadline) WHERE processing_job_id=$1 AND lease_generation=$2 AND input_revision=$3 AND state='running' AND lease_until>clock_timestamp() AND attempt_deadline>clock_timestamp()",&[&assignment.processing_job_id,&assignment.lease_generation,&assignment.input_revision]).await.map_err(|_|anyhow!("assignment renewal failed"))?==1;
        tx.commit()
            .await
            .map_err(|_| anyhow!("assignment renewal commit failed"))?;
        Ok(renewed)
    }

    async fn complete(&self, assignment: &Lease, marker: &ObjectRef) -> Result<bool> {
        validate_reference(marker).map_err(|_| InvalidCompletion)?;
        let mut client = self.database.lock().await;
        let tx = client
            .transaction()
            .await
            .map_err(|_| anyhow!("completion transaction unavailable"))?;
        let Some(row) = tx
            .query_opt(
                "SELECT input FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
                &[&assignment.processing_job_id],
            )
            .await
            .map_err(|_| anyhow!("completion identity lookup failed"))?
        else {
            return Ok(false);
        };
        let input: ProcessingInput = serde_json::from_value(row.get(0))?;
        if !marker
            .key
            .starts_with(&format!("_private/artifact-families/v1/{}/", input.sha256))
            || !marker.key.ends_with("/complete.json")
            || marker.key.len() > 1024
            || !marker
                .key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_-.".contains(&b))
            || marker
                .key
                .split('/')
                .any(|segment| segment.is_empty() || segment == ".." || segment == ".")
        {
            return Err(InvalidCompletion.into());
        }
        let value = serde_json::to_value(marker)?;
        let updated=tx.execute("UPDATE media_processing_jobs SET state='completed',manifest=$4,lease_until=NULL WHERE processing_job_id=$1 AND lease_generation=$2 AND input_revision=$3 AND state='running' AND lease_until>clock_timestamp() AND attempt_deadline>clock_timestamp()",&[&assignment.processing_job_id,&assignment.lease_generation,&assignment.input_revision,&value]).await.map_err(|_|anyhow!("completion persistence failed"))?;
        let replay = if updated == 0 {
            tx.query_one("SELECT state='completed' AND lease_generation=$2 AND input_revision=$3 AND manifest=$4 FROM media_processing_jobs WHERE processing_job_id=$1",&[&assignment.processing_job_id,&assignment.lease_generation,&assignment.input_revision,&value]).await.map_err(|_|anyhow!("completion receipt lookup failed"))?.get::<_,Option<bool>>(0).unwrap_or(false)
        } else {
            true
        };
        tx.commit()
            .await
            .map_err(|_| anyhow!("completion commit failed"))?;
        Ok(replay)
    }
}

#[cfg(test)]
#[path = "inbox_tests.rs"]
mod database_tests;

#[cfg(test)]
#[path = "inbox_http_tests.rs"]
mod http_tests;

#[cfg(test)]
#[path = "producer_journal_tests.rs"]
mod journal_tests;

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_reference(value: &ObjectRef) -> Result<()> {
    let valid_bucket = (3..=222).contains(&value.bucket.len())
        && value
            .bucket
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    let version = value.version.as_deref().unwrap_or("");
    if !valid_bucket
        || version.is_empty()
        || version == "null"
        || version.len() > 1024
        || version.contains("://")
        || !version.bytes().all(|b| (33..=126).contains(&b))
    {
        bail!("invalid pinned storage identity");
    }
    if value.provider == StorageProvider::Gcs && !version.parse::<i64>().is_ok_and(|v| v > 0) {
        bail!("invalid GCS generation");
    }
    Ok(())
}

fn validate_event(event: &MediaFinalizedV1, idempotency_key: &str) -> Result<String> {
    if event.schema_version != 1
        || event.pipeline_version == 0
        || !is_hash(&event.sha256)
        || !is_hash(&event.job_id)
        || idempotency_key != event.job_id
        || event.size > i64::MAX as u64
        || event.content_type.is_empty()
        || event.content_type.len() > 255
        || !event.content_type.contains('/')
        || !event.content_type.bytes().all(|b| (32..=126).contains(&b))
        || event
            .owner
            .as_ref()
            .is_some_and(|owner| owner.len() != 64 || !owner.bytes().all(|b| b.is_ascii_hexdigit()))
        || event.replicas.len() > 1
    {
        bail!("invalid finalization envelope");
    }
    for object in std::iter::once(&event.source).chain(&event.replicas) {
        validate_reference(object)?;
        if object.key != event.sha256 {
            bail!("source identity does not match content");
        }
    }
    if event
        .replicas
        .iter()
        .any(|copy| copy.provider == event.source.provider)
    {
        bail!("duplicate storage provider");
    }
    let mut canonical = event.clone();
    canonical.refresh_job_id()?;
    if canonical.job_id != event.job_id {
        bail!("finalization identity mismatch");
    }
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        "divine-media-processing-v1",
        event.pipeline_version,
        &event.source,
    ))?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ObjectRef, StorageProvider};

    fn event(owner: char) -> MediaFinalizedV1 {
        MediaFinalizedV1::new(
            &"a".repeat(64),
            15,
            "video/mp4",
            &owner.to_string().repeat(64),
            ObjectRef {
                provider: StorageProvider::B2,
                bucket: "synthetic-private".into(),
                key: "a".repeat(64),
                version: Some("version-1".into()),
            },
        )
        .unwrap()
    }

    #[test]
    fn processing_identity_deduplicates_owners_but_pins_source_and_pipeline() {
        let one = event('b');
        let two = event('c');
        assert_ne!(one.job_id, two.job_id);
        let id = validate_event(&one, &one.job_id).unwrap();
        assert_eq!(id, validate_event(&two, &two.job_id).unwrap());
        let mut next = one.clone();
        next.source.version = Some("version-2".into());
        next.refresh_job_id().unwrap();
        assert_ne!(id, validate_event(&next, &next.job_id).unwrap());
        next = one;
        next.pipeline_version = 2;
        next.refresh_job_id().unwrap();
        assert_ne!(id, validate_event(&next, &next.job_id).unwrap());
    }

    #[test]
    fn processing_identity_has_a_fixed_cross_language_vector() {
        let value = event('b');
        let encoding =
            serde_json::to_string(&("divine-media-processing-v1", 1u32, &value.source)).unwrap();
        assert_eq!(encoding,concat!("[\"divine-media-processing-v1\",1,{\"provider\":\"b2\",\"bucket\":\"synthetic-private\",\"key\":\"",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "\",\"version\":\"version-1\"}]"));
        assert_eq!(
            validate_event(&value, &value.job_id).unwrap(),
            "31dd6874aa402c20b9c8ce3f9f5f2d01d5d9f95b545035239e90140130e6457b"
        );
    }

    #[test]
    fn invalid_envelopes_cannot_enter_the_database() {
        let good = event('b');
        assert!(validate_event(&good, "wrong").is_err());
        for field in [
            "schema", "key", "version", "bucket", "type", "owner", "job", "pipeline", "replica",
        ] {
            let mut value = good.clone();
            match field {
                "schema" => value.schema_version = 2,
                "key" => value.source.key = "b".repeat(64),
                "version" => value.source.version = Some("null".into()),
                "bucket" => value.source.bucket = "https://untrusted.invalid/".into(),
                "type" => value.content_type = "video/mp4\r\ninvalid".into(),
                "owner" => value.owner = Some("invalid".into()),
                "job" => value.job_id = "invalid".into(),
                "pipeline" => value.pipeline_version = 0,
                "replica" => value.replicas.push(value.source.clone()),
                _ => unreachable!(),
            }
            assert!(validate_event(&value, &value.job_id).is_err(), "{field}");
        }
    }
}

#[cfg(test)]
#[path = "original_write_tests.rs"]
mod original_write_tests;
