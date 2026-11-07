//! Durable original-upload preparation. No edge claim, provider I/O or release.
use crate::{
    inbox::InboxStore,
    resumable::{MediaResumableBackend, UploadSession},
    storage::{MediaStorage, StorageProvider, WriteMode},
    NostrEvent,
};
use anyhow::{anyhow, Result};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::Transaction;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Original upload execution unavailable; preparation is not write authority")]
    Unavailable,
    #[error(
        "Original upload preparation conflicts with its immutable identity or storage mapping"
    )]
    Conflict,
    #[error("Original upload authorization or session invalid")]
    Invalid,
}
#[derive(Clone)]
pub(crate) struct Runtime {
    store: InboxStore,
    storage: MediaStorage,
    backend: MediaResumableBackend,
}
#[derive(Debug, Serialize)]
pub(crate) struct Prepared {
    pub(crate) intent_id: String,
    pub(crate) attempt_id: String,
}
pub(crate) enum SessionPreparation {
    Prepared,
    Committed(Box<UploadSession>),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Destination {
    provider: StorageProvider,
    bucket: String,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Input {
    ingress: String,
    owner: String,
    auth_event_id: String,
    sha256: String,
    size: u64,
    content_type_hint: String,
    destinations: Vec<Destination>,
    byte_evidence: String,
    session: Option<(String, i64)>,
}
fn hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn nonce() -> String {
    let mut n = [0; 16];
    OsRng.fill_bytes(&mut n);
    hex::encode(n)
}
pub(crate) fn enabled(value: Option<&str>) -> Result<bool> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(anyhow!("Invalid MEDIA_ORIGINAL_WRITE_ENABLED")),
    }
}
fn authorize(auth: &NostrEvent, parent: &str) -> Result<()> {
    crate::validate_event(auth, "upload").map_err(|_| Error::Invalid)?;
    if !hash(&auth.pubkey)
        || !hash(parent)
        || crate::get_tag_value(&auth.tags, "x").is_some_and(|s| s.to_ascii_lowercase() != parent)
    {
        return Err(Error::Invalid.into());
    }
    Ok(())
}
fn destinations(storage: &MediaStorage) -> Result<Vec<Destination>> {
    Ok(crate::outbox::required_destinations(
        storage.mode,
        storage.gcs_bucket(),
        storage.b2_bucket(),
    )?
    .into_iter()
    .map(|(provider, bucket)| Destination {
        provider,
        bucket: bucket.into(),
    })
    .collect())
}
impl Runtime {
    pub(crate) fn new(
        store: InboxStore,
        storage: MediaStorage,
        backend: MediaResumableBackend,
    ) -> Self {
        Self {
            store,
            storage,
            backend,
        }
    }
    pub(crate) async fn configured(
        database_url: Option<&str>,
        storage: MediaStorage,
        backend: MediaResumableBackend,
    ) -> Result<Option<Self>> {
        if !enabled(
            std::env::var("MEDIA_ORIGINAL_WRITE_ENABLED")
                .ok()
                .as_deref(),
        )? {
            return Ok(None);
        }
        let url = database_url.ok_or_else(|| {
            anyhow!("COORDINATION_DATABASE_URL required for original preparation")
        })?;
        let store = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            InboxStore::from_client(crate::coordination::connect(url).await?).await
        })
        .await
        .map_err(|_| anyhow!("Original preparation initialization deadline exceeded"))??;
        Ok(Some(Self::new(store, storage, backend)))
    }
    pub(crate) async fn prepare_direct(
        &self,
        auth: &NostrEvent,
        parent: &str,
        size: u64,
        mime: &str,
    ) -> Result<Prepared> {
        authorize(auth, parent)?;
        let mut db = self.store.database.lock().await;
        let tx = db.transaction().await.map_err(|_| Error::Unavailable)?;
        let mut selected = destinations(&self.storage)?;
        // An unfinished compatibility intent pins required destinations, not identity.
        if let Some(row)=tx.query_opt("SELECT size,write_mode,gcs_bucket,b2_bucket FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND NOT completed",&[&parent,&auth.pubkey]).await.map_err(|_|Error::Unavailable)? {
            let mode:WriteMode=row.get::<_,String>(1).parse().map_err(|_|Error::Conflict)?;
            let gcs:String=row.get(2); let b2:Option<String>=row.get(3);
            selected=crate::outbox::required_destinations(mode,&gcs,b2.as_deref())?.into_iter().map(|(provider,bucket)| Destination{provider,bucket:bucket.into()}).collect();
            if row.get::<_,i64>(0)!=i64::try_from(size).map_err(|_|Error::Invalid)? || selected.iter().any(|d| match d.provider {StorageProvider::Gcs=>d.bucket!=self.storage.gcs_bucket(),StorageProvider::B2=>Some(d.bucket.as_str())!=self.storage.b2_bucket()}) {return Err(Error::Conflict.into());}
        }
        let key = hex::encode(Sha256::digest(serde_json::to_vec(&(
            "original-write-v1",
            "direct",
            &auth.id,
            parent,
        ))?));
        let input = Input {
            ingress: "direct".into(),
            owner: auth.pubkey.clone(),
            auth_event_id: auth.id.clone(),
            sha256: parent.into(),
            size,
            content_type_hint: mime.into(),
            destinations: selected,
            byte_evidence: "server_spooled_sha256".into(),
            session: None,
        };
        let result = prepare(&tx, &key, input).await?;
        // ON CONFLICT can wait on another transaction; authorization is checked
        // again after that wait before this receipt is committed.
        authorize(auth, parent)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(result)
    }
    pub(crate) async fn prepare_session(
        &self,
        id: &str,
        owner: &str,
        auth: Option<&NostrEvent>,
    ) -> Result<SessionPreparation> {
        let auth = auth.ok_or(Error::Unavailable)?;
        crate::validate_event(auth, "upload").map_err(|_| Error::Invalid)?;
        if auth.pubkey != owner || id.len() > 128 {
            return Err(Error::Invalid.into());
        }
        let mut db = self.store.database.lock().await;
        let tx = db.transaction().await.map_err(|_| Error::Unavailable)?;
        let locked: bool = tx
            .query_one(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,7301))",
                &[&id],
            )
            .await
            .map_err(|_| Error::Unavailable)?
            .get(0);
        if !locked {
            return Err(Error::Unavailable.into());
        }
        // No legacy provider fallback and no expiry cleanup: OW1 is PG-only.
        let row=tx.query_opt("SELECT revision,state FROM upload_sessions WHERE upload_id=$1 AND NOT EXISTS(SELECT 1 FROM upload_session_tombstones WHERE upload_id=$1) FOR UPDATE",&[&id]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Unavailable)?;
        let mut session: UploadSession =
            serde_json::from_value(row.get(1)).map_err(|_| Error::Unavailable)?;
        session.revision = row.get(0);
        if session.upload_id != id
            || session.owner != owner
            || session.revision <= 0
            || session.expires_at_epoch_secs <= crate::resumable::now_epoch_secs()
            || session.next_offset != session.declared_size
        {
            return Err(Error::Invalid.into());
        }
        authorize(auth, &session.final_sha256)?;
        if session.finalized_object.is_some() {
            if let Some(row)=tx.query_opt("SELECT o.payload FROM upload_processing_session_receipts r JOIN upload_processing_outbox o ON o.job_id=r.job_id WHERE r.upload_id=$1",&[&id]).await.map_err(|_|Error::Unavailable)? {
                crate::outbox::check_session_receipt(row.get(0),&session.final_sha256,session.declared_size,owner).map_err(|_|Error::Unavailable)?;
                tx.commit().await.map_err(|_|Error::Unavailable)?;
                return Ok(SessionPreparation::Committed(Box::new(session)));
            }
            return Err(Error::Unavailable.into());
        }
        let storage = self
            .backend
            .finalization_storage(&session)
            .map_err(|_| Error::Unavailable)?;
        if session.session_url != "staged-v1" {
            // Legacy session manifests did not pin a bucket. A known durable
            // finalization intent must supply it; current config alone cannot.
            let row=tx.query_opt("SELECT size,write_mode,gcs_bucket FROM upload_processing_intents WHERE sha256=$1 AND owner=$2 AND NOT completed",&[&session.final_sha256,&session.owner]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Unavailable)?;
            if row.get::<_, i64>(0)
                != i64::try_from(session.declared_size).map_err(|_| Error::Invalid)?
                || row.get::<_, String>(1) != "gcs"
                || row.get::<_, String>(2) != storage.gcs_bucket()
            {
                return Err(Error::Conflict.into());
            }
        }
        let key = hex::encode(Sha256::digest(serde_json::to_vec(&(
            "original-write-v1",
            "session",
            id,
            session.revision,
        ))?));
        let input = Input {
            ingress: if session.session_url == "staged-v1" {
                "resumable_staged"
            } else {
                "resumable_legacy"
            }
            .into(),
            owner: owner.into(),
            auth_event_id: auth.id.clone(),
            sha256: session.final_sha256.clone(),
            size: session.declared_size,
            content_type_hint: session.content_type.clone(),
            destinations: destinations(&storage)?,
            byte_evidence: "session_declared_not_verified".into(),
            session: Some((id.into(), session.revision)),
        };
        prepare(&tx, &key, input).await?;
        authorize(auth, &session.final_sha256)?;
        if session.expires_at_epoch_secs <= crate::resumable::now_epoch_secs() {
            return Err(Error::Invalid.into());
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(SessionPreparation::Prepared)
    }
}
async fn prepare(tx: &Transaction<'_>, key: &str, input: Input) -> Result<Prepared> {
    if input.content_type_hint.len() > 1024
        || !hash(&input.owner)
        || !hash(&input.sha256)
        || input.size > i64::MAX as u64
    {
        return Err(Error::Invalid.into());
    }
    let value = serde_json::to_value(&input)?;
    let candidate = nonce();
    let row=tx.query_one("INSERT INTO upload_write_intents(intent_id,request_key,input) VALUES($1,$2,$3) ON CONFLICT(request_key) DO UPDATE SET request_key=upload_write_intents.request_key RETURNING intent_id,input",&[&candidate,&key,&value]).await.map_err(|_|Error::Unavailable)?;
    let intent_id: String = row.get(0);
    let saved: Input = serde_json::from_value(row.get(1)).map_err(|_| Error::Unavailable)?;
    // Fresh valid auth and MIME hints may differ on retry; accepted authority/mapping may not.
    if saved.owner != input.owner
        || saved.sha256 != input.sha256
        || saved.size != input.size
        || saved.destinations != input.destinations
        || saved.ingress != input.ingress
        || saved.session != input.session
        || saved.byte_evidence != input.byte_evidence
    {
        return Err(Error::Conflict.into());
    }
    let candidate = nonce();
    let row=tx.query_one("INSERT INTO media_storage_attempts(attempt_id,subject_kind,upload_write_intent_id,input,phase) VALUES($1,'original',$2,$3,'planned') ON CONFLICT(upload_write_intent_id) WHERE subject_kind='original' DO UPDATE SET attempt_id=media_storage_attempts.attempt_id RETURNING attempt_id",&[&candidate,&intent_id,&row.get::<_,serde_json::Value>(1)]).await.map_err(|_|Error::Unavailable)?;
    let attempt_id: String = row.get(0);
    for (i, d) in saved.destinations.iter().enumerate() {
        for (offset, key, optional) in [
            (0, saved.sha256.clone(), false),
            (2, format!("{}.jpg", saved.sha256), true),
        ] {
            let position = (i + offset) as i32;
            let target = serde_json::json!({"provider":d.provider,"bucket":d.bucket,"key":key});
            tx.execute("INSERT INTO upload_write_operations(attempt_id,position,target,optional) VALUES($1,$2,$3,$4) ON CONFLICT(attempt_id,position) DO NOTHING",&[&attempt_id,&position,&target,&optional]).await.map_err(|_|Error::Unavailable)?;
        }
    }
    Ok(Prepared {
        intent_id,
        attempt_id,
    })
}

pub(crate) fn error_response(error: &anyhow::Error) -> Option<axum::response::Response> {
    use axum::http::{header, StatusCode};
    use axum::response::IntoResponse;
    let err = error.downcast_ref::<Error>()?;
    let code = match err {
        Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Error::Conflict => StatusCode::CONFLICT,
        Error::Invalid => StatusCode::UNAUTHORIZED,
    };
    Some(
        (
            code,
            [(header::CACHE_CONTROL, "private, no-store")],
            axum::Json(serde_json::json!({"error":err.to_string()})),
        )
            .into_response(),
    )
}
