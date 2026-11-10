//! Durable small-chunk uploads with provider-pinned final writes.
use super::*;
use crate::storage::{MediaStorage, ObjectRef, WriteMode};
use anyhow::bail;
use std::sync::Arc;

#[derive(Clone)]
pub struct ResumableRuntime {
    pub backend: MediaResumableBackend,
    pub store: ProviderSessionStore,
}
impl ResumableRuntime {
    pub async fn new(
        storage: MediaStorage,
        gcs: Option<GcsClient>,
        bucket: String,
        database_url: Option<&str>,
    ) -> Result<Self> {
        let legacy = gcs.map(|client| GcsSessionStore::new(client, bucket.clone()));
        let backend_gcs = legacy
            .as_ref()
            .map(|store| GcsResumableBackend::new(store.client.clone(), bucket));
        let database = match database_url {
            Some(url) => {
                let client = crate::coordination::connect(url).await?;
                client
                    .batch_execute(include_str!(
                        "../database/migrations/001_upload_coordination.sql"
                    ))
                    .await
                    .map_err(|_| anyhow!("Failed to initialize upload coordination"))?;
                Some(Arc::new(client))
            }
            None => None,
        };
        if matches!(storage.mode, WriteMode::B2 | WriteMode::Dual) && database.is_none() {
            bail!("COORDINATION_DATABASE_URL is required for B2 resumable uploads");
        }
        Ok(Self {
            backend: MediaResumableBackend {
                storage,
                legacy: backend_gcs,
                database: database.clone(),
                database_url: database_url.map(str::to_owned),
            },
            store: ProviderSessionStore {
                database,
                database_url: database_url.map(str::to_owned),
                legacy,
            },
        })
    }
    /// Bounded cleanup of expired durable sessions; does not enumerate legacy GCS manifests.
    pub async fn sweep_expired(
        &self,
        limit: i64,
        outbox: Option<&crate::outbox::ProcessingOutbox>,
    ) -> Result<u64> {
        let Some(db) = &self.store.database else {
            return Ok(0);
        };
        let now = now_epoch_secs() as i64;
        let rows=db.query("SELECT upload_id FROM upload_sessions WHERE (state->>'expires_at_epoch_secs')::bigint <= $1 ORDER BY cleanup_checked_at,upload_id LIMIT $2",&[&now,&limit.clamp(1,1000)]).await.map_err(|_|anyhow!("Expired session lookup failed"))?;
        let mut count = 0;
        let mut errors = 0;
        for row in rows {
            let id: String = row.get(0);
            // Persist scheduling progress before attempting cleanup, including busy
            // and uncertain sessions, so a bounded page cannot starve later work.
            if db.execute("UPDATE upload_sessions SET cleanup_checked_at=clock_timestamp() WHERE upload_id=$1",&[&id]).await.is_err() {errors+=1;continue;}
            let attempt: Result<bool> = async {
                let _lock = self.store.lock(&id).await?;
                let Some(session) = self.store.load(&id).await? else {
                    return Ok(false);
                };
                if session.expires_at_epoch_secs > now_epoch_secs() {
                    return Ok(false);
                }
                if session.session_url == "staged-v1" {
                    self.backend.delete_object(&session.temp_object).await?;
                } else if session.finalized_object.is_none() {
                    self.backend.cancel_session(&session).await?;
                    self.backend.delete_object(&session.temp_object).await?;
                }
                // Before the session, so a failure leaves it for the next pass
                // rather than orphaning a receipt no sweep will find again.
                if let Some(outbox) = outbox {
                    outbox.forget_session_receipt(&id).await?;
                }
                self.store.delete(&id).await?;
                db.execute(
                    "DELETE FROM upload_final_objects WHERE temp_key=$1",
                    &[&session.temp_object],
                )
                .await
                .map_err(|_| anyhow!("Final reference cleanup failed"))?;
                Ok(true)
            }
            .await;
            match attempt {
                Ok(true) => count += 1,
                Ok(false) => {}
                Err(_) => errors += 1,
            }
        }
        if errors > 0 {
            bail!(
                "Expired session cleanup had {} errors; {} sessions cleaned",
                errors,
                count
            );
        }
        Ok(count)
    }
    #[cfg(test)]
    pub fn legacy_for_test() -> Self {
        let client =
            GcsClient::new(google_cloud_storage::client::ClientConfig::default().anonymous());
        Self {
            backend: MediaResumableBackend {
                storage: MediaStorage::new(
                    WriteMode::Gcs,
                    Some(client.clone()),
                    "test-bucket".into(),
                    None,
                )
                .unwrap(),
                legacy: Some(GcsResumableBackend::new(client.clone(), "test-bucket")),
                database: None,
                database_url: None,
            },
            store: ProviderSessionStore {
                database: None,
                database_url: None,
                legacy: Some(GcsSessionStore::new(client, "test-bucket")),
            },
        }
    }
}
#[derive(Clone)]
pub struct ProviderSessionStore {
    database: Option<Arc<tokio_postgres::Client>>,
    database_url: Option<String>,
    legacy: Option<GcsSessionStore>,
}
#[async_trait]
impl SessionStore for ProviderSessionStore {
    async fn lock(&self, id: &str) -> Result<Option<Box<dyn Send>>> {
        if let Some(url) = &self.database_url {
            let client = crate::coordination::connect(url).await?;
            // A dedicated connection holds the session advisory lock; disconnect releases it.
            let locked: bool = client
                .query_one(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 7301))",
                    &[&id],
                )
                .await
                .map_err(|_| anyhow!("Upload lock unavailable"))?
                .get(0);
            if !locked {
                return Err(SessionBusy.into());
            }
            Ok(Some(Box::new(client)))
        } else {
            Ok(None)
        }
    }
    async fn load(&self, id: &str) -> Result<Option<UploadSession>> {
        if let Some(db) = &self.database {
            if db
                .query_opt(
                    "SELECT upload_id FROM upload_session_tombstones WHERE upload_id=$1",
                    &[&id],
                )
                .await
                .map_err(|_| anyhow!("Session deletion lookup failed"))?
                .is_some()
            {
                return Ok(None);
            }
            if let Some(row) = db
                .query_opt(
                    "SELECT revision,state FROM upload_sessions WHERE upload_id=$1",
                    &[&id],
                )
                .await
                .map_err(|_| anyhow!("Session lookup failed"))?
            {
                let mut state: UploadSession = serde_json::from_value(row.get(1))?;
                state.revision = row.get(0);
                return Ok(Some(state));
            }
        }
        match &self.legacy {
            Some(store) => store.load(id).await,
            None => Ok(None),
        }
    }
    async fn save(&self, session: &UploadSession) -> Result<()> {
        if primary_session(&session.temp_object) {
            return self
                .legacy
                .as_ref()
                .ok_or_else(|| anyhow!("GCS session storage unavailable"))?
                .save(session)
                .await;
        }
        if let Some(db) = &self.database {
            let next = session
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow!("Session revision exhausted"))?;
            let state = serde_json::to_value(session)?;
            let changed=if session.revision==0 {
                db.execute("INSERT INTO upload_sessions(upload_id,revision,state) SELECT $1,$2,$3 WHERE NOT EXISTS(SELECT 1 FROM upload_session_tombstones WHERE upload_id=$1) ON CONFLICT(upload_id) DO NOTHING",&[&session.upload_id,&next,&state]).await
            }else{
                db.execute("UPDATE upload_sessions SET revision=$2,state=$3 WHERE upload_id=$1 AND revision=$4 AND NOT EXISTS(SELECT 1 FROM upload_session_tombstones WHERE upload_id=$1)",&[&session.upload_id,&next,&state,&session.revision]).await
            }.map_err(|_|anyhow!("Session persistence failed"))?;
            if changed != 1 {
                bail!("Session changed concurrently; retry");
            }
            Ok(())
        } else {
            self.legacy
                .as_ref()
                .ok_or_else(|| anyhow!("Session storage unavailable"))?
                .save(session)
                .await
        }
    }
    async fn delete(&self, id: &str) -> Result<()> {
        if let Some(db) = &self.database {
            // Existing PostgreSQL sessions never fall through to the GCS path.
            let known=db.query_opt("SELECT upload_id FROM upload_sessions WHERE upload_id=$1 UNION ALL SELECT upload_id FROM upload_session_tombstones WHERE upload_id=$1 LIMIT 1",&[&id]).await.map_err(|_|anyhow!("Session deletion lookup failed"))?.is_some();
            if known {
                db.execute("WITH removed AS (DELETE FROM upload_sessions WHERE upload_id=$1 RETURNING upload_id) INSERT INTO upload_session_tombstones(upload_id) VALUES($1) ON CONFLICT(upload_id) DO NOTHING",&[&id]).await.map_err(|_|anyhow!("Session cleanup failed"))?;
                return Ok(());
            }
        }
        if let Some(store) = &self.legacy {
            store.delete(id).await?;
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct MediaResumableBackend {
    storage: MediaStorage,
    legacy: Option<GcsResumableBackend>,
    database: Option<Arc<tokio_postgres::Client>>,
    database_url: Option<String>,
}
impl MediaResumableBackend {
    pub(crate) fn finalization_storage(&self, session: &UploadSession) -> Result<MediaStorage> {
        if primary_session(&session.temp_object) {
            let parts: Vec<_> = session.temp_object.split('/').collect();
            if parts.len() != 6
                || parts[2] != hex::encode(self.storage.gcs_bucket())
                || parts[3] != hex::encode(self.storage.b2_bucket().unwrap_or(""))
            {
                bail!("Upload session bucket mapping changed");
            }
            self.storage.with_mode(WriteMode::B2Primary)
        } else if session.session_url == "staged-v1" {
            self.staged_store(&session.temp_object)
        } else {
            self.storage.with_mode(WriteMode::Gcs)
        }
    }
    fn legacy(&self) -> Result<&GcsResumableBackend> {
        self.legacy
            .as_ref()
            .ok_or_else(|| anyhow!("Legacy GCS session requires GCS compatibility"))
    }
    fn database(&self) -> Result<&tokio_postgres::Client> {
        self.database
            .as_deref()
            .ok_or_else(|| anyhow!("Upload coordination unavailable"))
    }
    fn staged_store(&self, key: &str) -> Result<MediaStorage> {
        let parts: Vec<_> = key.split('/').collect();
        if parts.len() != 6 || parts[0] != "__resumable" || parts[1] != "staged" {
            bail!("Invalid staged upload identity");
        }
        let mode: WriteMode = parts[2].parse()?;
        let expected_gcs = hex::encode(self.storage.gcs_bucket());
        let expected_b2 = hex::encode(self.storage.b2_bucket().unwrap_or(""));
        if (mode != WriteMode::B2 && parts[3] != expected_gcs)
            || (mode != WriteMode::Gcs && parts[4] != expected_b2)
        {
            bail!("Upload session bucket mapping changed");
        }
        self.storage.with_mode(mode)
    }
    async fn assemble_file(&self, key: &str) -> Result<tempfile::NamedTempFile> {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new()?;
        let mut offset = 0u64;
        for row in self.database()?.query("SELECT start_offset,end_offset,sha256,objects FROM upload_chunks WHERE temp_key=$1 ORDER BY start_offset",&[&key]).await.map_err(|_|anyhow!("Upload chunk lookup failed"))? {
            let start:i64=row.get(0);let end:i64=row.get(1);let expected:String=row.get(2);let refs:Vec<ObjectRef>=serde_json::from_value(row.get(3))?;
            if start<0 || end<start || start as u64!=offset {bail!("Upload chunks are not contiguous");}
            crate::resources::check_size(end as u64 + 1)?;
            let reference=refs.first().ok_or_else(||anyhow!("Upload chunk has no storage reference"))?;
            let bytes=self.storage.read_ref(reference).await?;
            if bytes.len() as i64!=end-start+1 || hex::encode(Sha256::digest(&bytes))!=expected {bail!("Upload chunk integrity mismatch");}
            let next_offset=offset.checked_add(bytes.len() as u64).ok_or_else(||anyhow!("Upload size overflow"))?;
            crate::resources::check_size(next_offset)?;
            file.write_all(&bytes)?;
            offset=next_offset;
        }
        file.flush()?;
        file.as_file().sync_all()?;
        Ok(file)
    }
}

#[async_trait]
impl ResumableBackend for MediaResumableBackend {
    async fn historical_processing(
        &self,
        session: &UploadSession,
    ) -> Result<Option<crate::outbox::ProcessingOutbox>> {
        // Old staged B2/dual sessions were admitted under the historical outbox
        // contract. A new primary admission mode cannot downgrade that contract.
        if self.storage.mode == WriteMode::B2Primary && session.session_url == "staged-v1" {
            let url = self.database_url.as_deref().ok_or_else(|| {
                anyhow!("Historical upload session requires its existing database")
            })?;
            return crate::outbox::history_for_session(url).await.map(Some);
        }
        Ok(None)
    }
    async fn is_committed_retry(
        &self,
        session: &UploadSession,
        range: &ContentRange,
        chunk: &Bytes,
    ) -> Result<bool> {
        if session.session_url != "staged-v1" {
            return Ok(false);
        }
        if let Some(row)=self.database()?.query_opt("SELECT end_offset,sha256,objects FROM upload_chunks WHERE temp_key=$1 AND start_offset=$2",&[&session.temp_object,&(range.start as i64)]).await.map_err(|_|anyhow!("Chunk retry lookup failed"))? {
            let end:i64=row.get(0);let digest:String=row.get(1);let refs:Vec<ObjectRef>=serde_json::from_value(row.get(2))?;
            return Ok(end as u64==range.end && chunk.len() as u64==range.len() && digest==hex::encode(Sha256::digest(chunk)) && !refs.is_empty());
        }
        Ok(false)
    }

    async fn begin_finalization(
        &self,
        session: &UploadSession,
        outbox: &crate::outbox::ProcessingOutbox,
    ) -> Result<()> {
        let store = self.finalization_storage(session)?;
        outbox
            .begin_upload(
                &session.final_sha256,
                session.declared_size,
                &session.content_type,
                &session.owner,
                &store,
            )
            .await
    }
    async fn final_objects(&self, session: &UploadSession) -> Result<Vec<ObjectRef>> {
        if primary_session(&session.temp_object) {
            return self
                .finalization_storage(session)?
                .primary_receipts(&session.final_sha256, session.declared_size)
                .await;
        }
        if session.session_url == "staged-v1" {
            let row = self
                .database()?
                .query_opt(
                    "SELECT objects FROM upload_final_objects WHERE temp_key=$1",
                    &[&session.temp_object],
                )
                .await
                .map_err(|_| anyhow!("Final object lookup failed"))?
                .ok_or_else(|| anyhow!("Final object references missing"))?;
            Ok(serde_json::from_value(row.get(0))?)
        } else {
            Ok(vec![
                self.storage
                    .with_mode(WriteMode::Gcs)?
                    .stat(&session.final_sha256)
                    .await?
                    .ok_or_else(|| anyhow!("Final GCS object missing"))?
                    .reference,
            ])
        }
    }
    async fn finalize(&self, session: &UploadSession) -> Result<bool> {
        if primary_session(&session.temp_object) {
            use std::io::Write;
            let mut file = tempfile::NamedTempFile::new()?;
            let mut stream = self.legacy()?.stream_object(&session.temp_object).await?;
            let mut size = 0u64;
            let mut hash = Sha256::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                size = size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| anyhow!("Upload size overflow"))?;
                crate::resources::check_size(size)?;
                if size > session.declared_size {
                    bail!("Upload staging size changed");
                }
                hash.update(&chunk);
                file.write_all(&chunk)?;
            }
            if size != session.declared_size || hex::encode(hash.finalize()) != session.final_sha256
            {
                bail!("Upload staging integrity changed");
            }
            file.flush()?;
            let result = self
                .finalization_storage(session)?
                .put_file(
                    &session.final_sha256,
                    file.path(),
                    &session.content_type,
                    Some(&session.owner),
                )
                .await?;
            return Ok(result.newly_stored);
        }
        if session.session_url != "staged-v1" {
            let exists = self.legacy()?.object_exists(&session.final_sha256).await?;
            if !exists {
                self.legacy()?
                    .copy_to_final(
                        &session.temp_object,
                        &session.final_sha256,
                        &session.content_type,
                        &session.owner,
                    )
                    .await?;
            }
            return Ok(!exists);
        }
        let file = self.assemble_file(&session.temp_object).await?;
        let result = self
            .staged_store(&session.temp_object)?
            .put_file(
                &session.final_sha256,
                file.path(),
                &session.content_type,
                Some(&session.owner),
            )
            .await?;
        let references = serde_json::to_value(&result.objects)?;
        self.database()?.execute("INSERT INTO upload_final_objects(temp_key,objects) VALUES($1,$2) ON CONFLICT(temp_key) DO UPDATE SET objects=EXCLUDED.objects",&[&session.temp_object,&references]).await.map_err(|_|anyhow!("Failed to persist final object references"))?;
        Ok(result.newly_stored)
    }

    async fn create_session(
        &self,
        id: &str,
        owner: &str,
        mime: &str,
        size: u64,
    ) -> Result<BackendCreateSession> {
        if self.storage.mode == WriteMode::Gcs {
            return self.legacy()?.create_session(id, owner, mime, size).await;
        }
        if self.storage.mode == WriteMode::B2Primary {
            let key = format!(
                "__resumable/b2_primary/{}/{}/{}/blob",
                hex::encode(self.storage.gcs_bucket()),
                hex::encode(self.storage.b2_bucket().unwrap_or("")),
                id
            );
            return self.legacy()?.create_session_at(id, owner, mime, key).await;
        }
        if size > i64::MAX as u64 {
            bail!("Upload exceeds supported offset range");
        }
        let mode = match self.storage.mode {
            WriteMode::Gcs => "gcs",
            WriteMode::B2 => "b2",
            WriteMode::Dual => "dual",
            WriteMode::B2Primary => "b2_primary",
        };
        let key = format!(
            "__resumable/staged/{}/{}/{}/{}",
            mode,
            hex::encode(self.storage.gcs_bucket()),
            hex::encode(self.storage.b2_bucket().unwrap_or("")),
            id
        );
        Ok(BackendCreateSession {
            session_url: "staged-v1".into(),
            temp_object: key,
        })
    }
    async fn upload_chunk(
        &self,
        session: &UploadSession,
        range: &ContentRange,
        chunk: Bytes,
    ) -> Result<BackendUploadState> {
        if session.session_url != "staged-v1" {
            return self.legacy()?.upload_chunk(session, range, chunk).await;
        }
        let store = self.staged_store(&session.temp_object)?;
        let digest = hex::encode(Sha256::digest(&chunk));
        let key = format!(
            "{}/chunks/{}-{}-{}",
            session.temp_object, range.start, range.end, digest
        );
        let start = range.start as i64;
        let end = range.end as i64;
        // Reserve identity before I/O so cleanup/retry knows the deterministic key even after a lost response.
        let reserved=self.database()?.execute("INSERT INTO upload_chunks(temp_key,start_offset,end_offset,sha256,objects) VALUES($1,$2,$3,$4,'[]'::jsonb) ON CONFLICT(temp_key,start_offset) DO UPDATE SET sha256=upload_chunks.sha256 WHERE upload_chunks.end_offset=EXCLUDED.end_offset AND upload_chunks.sha256=EXCLUDED.sha256",&[&session.temp_object,&start,&end,&digest]).await.map_err(|_|anyhow!("Failed to reserve upload chunk"))?;
        if reserved != 1 {
            bail!("Conflicting bytes already committed at upload offset");
        }
        let result = store
            .put(
                &key,
                chunk,
                "application/octet-stream",
                Some(&session.owner),
            )
            .await?;
        let refs = serde_json::to_value(result.objects)?;
        self.database()?
            .execute(
                "UPDATE upload_chunks SET objects=$3 WHERE temp_key=$1 AND start_offset=$2",
                &[&session.temp_object, &start, &refs],
            )
            .await
            .map_err(|_| anyhow!("Failed to persist upload chunk"))?;
        Ok(if range.end + 1 == range.total {
            BackendUploadState::Completed
        } else {
            BackendUploadState::InProgress
        })
    }
    async fn cancel_session(&self, session: &UploadSession) -> Result<()> {
        if session.session_url == "staged-v1" {
            self.delete_object(&session.temp_object).await
        } else {
            self.legacy()?.cancel_session(session).await
        }
    }
    async fn stream_object(&self, key: &str) -> Result<BoxStream<'static, Result<Bytes>>> {
        if key.starts_with("__resumable/staged/") {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = self.assemble_file(key).await?;
            file.as_file_mut().seek(SeekFrom::Start(0))?;
            Ok(Box::pin(futures::stream::try_unfold(
                file,
                |mut file| async move {
                    let mut buffer = vec![0; 1024 * 1024];
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        Ok(None)
                    } else {
                        buffer.truncate(count);
                        Ok(Some((Bytes::from(buffer), file)))
                    }
                },
            )))
        } else {
            self.legacy()?.stream_object(key).await
        }
    }
    // Always promote staged uploads through put: it repairs a partially completed dual write.
    async fn object_exists(&self, _key: &str) -> Result<bool> {
        Ok(false)
    }
    async fn copy_to_final(&self, source: &str, dest: &str, mime: &str, owner: &str) -> Result<()> {
        if source.starts_with("__resumable/staged/") {
            let file = self.assemble_file(source).await?;
            self.staged_store(source)?
                .put_file(dest, file.path(), mime, Some(owner))
                .await?;
            Ok(())
        } else {
            self.legacy()?
                .copy_to_final(source, dest, mime, owner)
                .await
        }
    }
    async fn delete_object(&self, key: &str) -> Result<()> {
        if !key.starts_with("__resumable/staged/") {
            return self.legacy()?.delete_object(key).await;
        }
        let rows = self
            .database()?
            .query(
                "SELECT objects FROM upload_chunks WHERE temp_key=$1",
                &[&key],
            )
            .await
            .map_err(|_| anyhow!("Chunk cleanup lookup failed"))?;
        let mut references = Vec::new();
        for row in rows {
            let refs: Vec<ObjectRef> = serde_json::from_value(row.get(0))?;
            // A reservation is persisted before the PUT. No recorded version means
            // that a timed-out remote write may still commit; a HEAD 404 cannot
            // establish that it is safe to forget this operation.
            if refs.is_empty() {
                bail!("Chunk upload outcome is unresolved; retain session for operator reconciliation");
            }
            references.extend(refs);
        }
        for reference in references {
            self.storage.delete_ref(&reference).await?;
        }

        self.database()?
            .execute("DELETE FROM upload_chunks WHERE temp_key=$1", &[&key])
            .await
            .map_err(|_| anyhow!("Chunk cleanup failed"))?;
        Ok(())
    }
}

#[cfg(test)]
static SWEEP_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

#[cfg(test)]
mod tests {
    use super::*;
    async fn database_store() -> ProviderSessionStore {
        let url = std::env::var("TEST_COORDINATION_DATABASE_URL")
            .expect("TEST_COORDINATION_DATABASE_URL required");
        let db = crate::coordination::connect(&url).await.unwrap();
        db.batch_execute(include_str!(
            "../database/migrations/001_upload_coordination.sql"
        ))
        .await
        .unwrap();
        ProviderSessionStore {
            database: Some(Arc::new(db)),
            database_url: Some(url),
            legacy: None,
        }
    }
    fn session() -> UploadSession {
        UploadSession {
            revision: 0,
            chunk_size: 1024 * 1024,
            upload_id: format!("test_{}", random_hex(12)),
            owner: "a".repeat(64),
            final_sha256: "b".repeat(64),
            declared_size: 42,
            content_type: "video/mp4".into(),
            file_name: None,
            expires_at_epoch_secs: now_epoch_secs() + 86400,
            next_offset: 0,
            session_url: "staged-v1".into(),
            session_token: "token".into(),
            temp_object: "test-temp".into(),
            storage_complete: false,
            finalized_object: None,
        }
    }
    #[tokio::test]
    async fn postgres_session_cas_rejects_stale_revision() {
        if std::env::var("TEST_COORDINATION_DATABASE_URL").is_err() {
            eprintln!("Skipping PostgreSQL integration: TEST_COORDINATION_DATABASE_URL unset");
            return;
        }
        let store = database_store().await;
        let original = session();
        store.save(&original).await.unwrap();
        let mut loaded = store.load(&original.upload_id).await.unwrap().unwrap();
        assert_eq!(loaded.revision, 1);
        assert_eq!(loaded.chunk_size, 1024 * 1024);
        loaded.next_offset = 10;
        store.save(&loaded).await.unwrap();
        assert!(store.save(&loaded).await.is_err());
        assert_eq!(
            store
                .load(&original.upload_id)
                .await
                .unwrap()
                .unwrap()
                .next_offset,
            10
        );
        store.delete(&original.upload_id).await.unwrap();
        assert!(store.load(&original.upload_id).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn sweep_forgets_the_completion_receipt_of_an_expired_session() {
        use crate::outbox::tests::{fixture_storage, isolated_database_url};
        use crate::storage::StorageProvider;
        // A fresh schema, so the sweep sees only this test's sessions.
        let Some(url) = isolated_database_url().await else {
            return;
        };
        let storage = fixture_storage(WriteMode::Gcs, None);
        let runtime = ResumableRuntime::new(storage.clone(), None, String::new(), Some(&url))
            .await
            .unwrap();
        let outbox = crate::outbox::ProcessingOutbox::connect(
            &url,
            "https://processing.invalid/finalized",
            "fixture",
        )
        .await
        .unwrap();
        let mut completed = session();
        completed.final_sha256 = random_hex(32);
        completed.next_offset = completed.declared_size;
        completed.storage_complete = true;
        completed.finalized_object = Some(completed.final_sha256.clone());
        // Not staged: a finalized session's sweep then needs no provider I/O.
        completed.session_url = "https://upload.invalid/session".into();
        completed.expires_at_epoch_secs = 0;
        runtime.store.save(&completed).await.unwrap();
        let (hash, size, owner) = (
            completed.final_sha256.clone(),
            completed.declared_size,
            completed.owner.clone(),
        );
        outbox
            .begin_upload(&hash, size, "video/mp4", &owner, &storage)
            .await
            .unwrap();
        outbox
            .enqueue_completed_session(
                &completed.upload_id,
                &hash,
                size,
                &owner,
                vec![ObjectRef {
                    provider: StorageProvider::Gcs,
                    bucket: "fixture-gcs".into(),
                    key: hash.clone(),
                    version: Some("1".into()),
                }],
            )
            .await
            .unwrap();

        assert_eq!(runtime.sweep_expired(100, Some(&outbox)).await.unwrap(), 1);
        assert!(runtime
            .store
            .load(&completed.upload_id)
            .await
            .unwrap()
            .is_none());
        assert!(
            !outbox
                .has_session_receipt(&completed.upload_id, &hash, size, &owner)
                .await
                .unwrap(),
            "an expired session can never be replayed, so its receipt must go with it"
        );
    }
    #[tokio::test]
    async fn postgres_session_lock_excludes_another_connection() {
        if std::env::var("TEST_COORDINATION_DATABASE_URL").is_err() {
            eprintln!("Skipping PostgreSQL integration: TEST_COORDINATION_DATABASE_URL unset");
            return;
        }
        let store = database_store().await;
        let id = format!("test_lock_{}", random_hex(12));
        let guard = store.lock(&id).await.unwrap();
        let busy = store
            .lock(&id)
            .await
            .err()
            .expect("lock must remain exclusive");
        assert!(busy.is::<SessionBusy>());
        assert_eq!(
            super::internal_error(busy).status_code(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        drop(guard);
    }
    #[tokio::test]
    async fn migrated_legacy_session_does_not_reappear_after_deletion() {
        if std::env::var("TEST_COORDINATION_DATABASE_URL").is_err() {
            return;
        }
        use axum::{routing::any, Json, Router};
        use std::{
            future::IntoFuture,
            sync::atomic::{AtomicUsize, Ordering},
        };
        let mut store = database_store().await;
        let original = session();
        let payload = serde_json::to_value(&original).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            axum::serve(
                listener,
                Router::new().route(
                    "/*path",
                    any(move || {
                        let payload = payload.clone();
                        let observed = observed.clone();
                        async move {
                            observed.fetch_add(1, Ordering::SeqCst);
                            Json(payload)
                        }
                    }),
                ),
            )
            .into_future(),
        );
        let gcs = GcsClient::new(
            google_cloud_storage::client::ClientConfig {
                storage_endpoint: endpoint,
                ..Default::default()
            }
            .anonymous(),
        );
        store.legacy = Some(GcsSessionStore::new(gcs, "legacy"));
        let loaded = store.load(&original.upload_id).await.unwrap().unwrap();
        store.save(&loaded).await.unwrap();
        store.delete(&original.upload_id).await.unwrap();
        assert!(
            store.load(&original.upload_id).await.unwrap().is_none(),
            "deleted local session must not fall back to its old manifest"
        );
        assert!(
            store.save(&original).await.is_err(),
            "stale initial state cannot resurrect deletion"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        task.abort();
    }

    #[tokio::test]
    async fn uncertain_cleanup_does_not_block_other_expired_sessions_or_later_pages() {
        let _sweep_guard = SWEEP_TEST_LOCK.lock().await;
        let Ok(url) = std::env::var("TEST_COORDINATION_DATABASE_URL") else {
            return;
        };
        let storage = MediaStorage::new(
            WriteMode::B2,
            None,
            String::new(),
            Some(
                crate::storage::B2Store::new(
                    "fixture".into(),
                    "us-west-001".into(),
                    "key".into(),
                    "secret".into(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let runtime = ResumableRuntime::new(storage, None, String::new(), Some(&url))
            .await
            .unwrap();
        let db = runtime.store.database.as_ref().unwrap();
        let mut uncertain = session();
        uncertain.expires_at_epoch_secs = 0;
        uncertain.temp_object = format!(
            "__resumable/staged/b2//66697874757265/{}",
            uncertain.upload_id
        );
        runtime.store.save(&uncertain).await.unwrap();
        db.execute("INSERT INTO upload_chunks(temp_key,start_offset,end_offset,sha256,objects) VALUES($1,0,7,$2,'[]'::jsonb)",&[&uncertain.temp_object,&"f".repeat(64)]).await.unwrap();
        let mut first = session();
        first.expires_at_epoch_secs = 0;
        first.temp_object = format!("__resumable/staged/b2//66697874757265/{}", first.upload_id);
        runtime.store.save(&first).await.unwrap();
        let first_pass = runtime.sweep_expired(100, None).await;
        let first_gone = runtime
            .store
            .load(&first.upload_id)
            .await
            .unwrap()
            .is_none();
        let mut later = Vec::new();
        for _ in 0..3 {
            let mut value = session();
            value.expires_at_epoch_secs = 0;
            value.temp_object =
                format!("__resumable/staged/b2//66697874757265/{}", value.upload_id);
            runtime.store.save(&value).await.unwrap();
            later.push(value);
        }
        for _ in 0..5 {
            let _ = runtime.sweep_expired(1, None).await;
        }
        let mut remaining = 0;
        for value in &later {
            remaining += usize::from(
                runtime
                    .store
                    .load(&value.upload_id)
                    .await
                    .unwrap()
                    .is_some(),
            );
        }
        let retained = runtime
            .store
            .load(&uncertain.upload_id)
            .await
            .unwrap()
            .is_some();
        // Remove synthetic reservations even if the old implementation starves cleanup.
        db.execute(
            "DELETE FROM upload_chunks WHERE temp_key=$1",
            &[&uncertain.temp_object],
        )
        .await
        .unwrap();
        runtime.store.delete(&uncertain.upload_id).await.unwrap();
        runtime.store.delete(&first.upload_id).await.unwrap();
        for value in later {
            runtime.store.delete(&value.upload_id).await.unwrap();
        }
        assert!(
            first_pass.is_err(),
            "uncertain outcomes must still alert after the pass"
        );
        assert!(
            first_gone,
            "an uncertain session must not prevent another session's cleanup in the same pass"
        );
        assert_eq!(
            remaining, 0,
            "bounded passes must progress beyond persistent uncertain sessions"
        );
        assert!(retained);
    }

    #[test]
    fn legacy_session_missing_revision_and_chunk_size_deserializes() {
        let mut value = serde_json::to_value(session()).unwrap();
        value.as_object_mut().unwrap().remove("revision");
        value.as_object_mut().unwrap().remove("chunk_size");
        let decoded: UploadSession = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.revision, 0);
        assert_eq!(decoded.chunk_size, 0);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::storage::B2Store;
    use axum::{
        body::Bytes as BodyBytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use std::{collections::HashMap, future::IntoFuture, sync::Mutex};
    #[derive(Clone)]
    struct Blob {
        bytes: Bytes,
        mime: String,
        owner: String,
    }
    type Objects = Arc<Mutex<HashMap<String, Blob>>>;
    async fn origin(
        State(objects): State<Objects>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: BodyBytes,
    ) -> axum::response::Response {
        let key = uri.path().to_string();
        if method == Method::PUT {
            objects.lock().unwrap().insert(
                key,
                Blob {
                    bytes: body,
                    mime: headers
                        .get("content-type")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .into(),
                    owner: headers
                        .get("x-amz-meta-owner")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .into(),
                },
            );
            return ([("x-amz-version-id", "version-1")], "").into_response();
        }
        if method == Method::DELETE {
            objects.lock().unwrap().remove(&key);
            return StatusCode::NO_CONTENT.into_response();
        }
        let Some(blob) = objects.lock().unwrap().get(&key).cloned() else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let headers = [
            ("x-amz-version-id", "version-1".to_string()),
            ("content-length", blob.bytes.len().to_string()),
            ("content-type", blob.mime),
            ("x-amz-meta-owner", blob.owner),
        ];
        if method == Method::HEAD {
            (headers, "").into_response()
        } else {
            (headers, blob.bytes).into_response()
        }
    }
    #[tokio::test]
    async fn b2_resumable_restart_switch_retry_completion_abort_and_expiry() {
        let _sweep_guard = SWEEP_TEST_LOCK.lock().await;
        if std::env::var("TEST_COORDINATION_DATABASE_URL").is_err() {
            eprintln!("Skipping PostgreSQL integration: TEST_COORDINATION_DATABASE_URL unset");
            return;
        }
        let database_url = std::env::var("TEST_COORDINATION_DATABASE_URL").unwrap();
        let objects: Objects = Arc::new(Mutex::new(HashMap::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/*path", any(origin))
                    .with_state(objects.clone())
                    .layer(axum::extract::DefaultBodyLimit::disable()),
            )
            .into_future(),
        );
        let b2 = B2Store::new(
            "test-bucket".into(),
            "us-west-001".into(),
            "key".into(),
            "secret".into(),
        )
        .unwrap()
        .with_endpoint(endpoint);
        let storage = MediaStorage::new(
            WriteMode::B2,
            None,
            "legacy-bucket".into(),
            Some(b2.clone()),
        )
        .unwrap();
        let runtime =
            ResumableRuntime::new(storage, None, "legacy-bucket".into(), Some(&database_url))
                .await
                .unwrap();
        let manager = ResumableManager::new(
            runtime.backend.clone(),
            runtime.store.clone(),
            "https://upload.example",
            "https://media.example",
            1024 * 1024,
            86400,
        );
        let bytes = Bytes::from(vec![42; 1024 * 1024 + 8]);
        let hash = hex::encode(Sha256::digest(&bytes));
        let init = manager
            .init_session(
                "owner",
                ResumableUploadInitRequest {
                    sha256: hash.clone(),
                    size: bytes.len() as u64,
                    content_type: "video/mp4".into(),
                    file_name: None,
                },
            )
            .await
            .unwrap();
        let authorization = init.required_headers.get("Authorization").unwrap();
        let range = format!("bytes 0-1048575/{}", bytes.len());
        manager
            .upload_chunk(
                &init.upload_id,
                Some(authorization),
                &range,
                bytes.slice(..1048576),
            )
            .await
            .unwrap();
        drop(manager);
        let gcs = GcsClient::new(google_cloud_storage::client::ClientConfig::default().anonymous());
        let switched = MediaStorage::new(
            WriteMode::Gcs,
            Some(gcs.clone()),
            "legacy-bucket".into(),
            Some(b2.clone()),
        )
        .unwrap();
        let runtime = ResumableRuntime::new(
            switched,
            Some(gcs),
            "legacy-bucket".into(),
            Some(&database_url),
        )
        .await
        .unwrap();
        let manager = ResumableManager::new(
            runtime.backend.clone(),
            runtime.store.clone(),
            "https://upload.example",
            "https://media.example",
            8 * 1024 * 1024,
            86400,
        );
        let status = manager
            .head_session(&init.upload_id, Some(authorization))
            .await
            .unwrap();
        assert_eq!(status.next_offset, 1048576);
        assert_eq!(status.chunk_size, 1048576);
        manager
            .upload_chunk(
                &init.upload_id,
                Some(authorization),
                &range,
                bytes.slice(..1048576),
            )
            .await
            .unwrap();
        assert!(manager
            .upload_chunk(
                &init.upload_id,
                Some(authorization),
                &range,
                Bytes::from(vec![41; 1048576])
            )
            .await
            .is_err());
        manager
            .upload_chunk(
                &init.upload_id,
                Some(authorization),
                &format!("bytes 1048576-1048583/{}", bytes.len()),
                bytes.slice(1048576..),
            )
            .await
            .unwrap();
        let complete = manager
            .complete_session(&init.upload_id, "owner")
            .await
            .unwrap();
        assert!(complete.newly_stored);
        let session = runtime.store.load(&init.upload_id).await.unwrap().unwrap();
        assert_eq!(
            runtime.backend.final_objects(&session).await.unwrap().len(),
            1
        );
        assert_eq!(
            objects
                .lock()
                .unwrap()
                .get(&format!("/test-bucket/{hash}"))
                .unwrap()
                .bytes,
            bytes
        );
        assert!(
            !manager
                .complete_session(&init.upload_id, "owner")
                .await
                .unwrap()
                .newly_stored
        );
        // Expiry removes session coordination without removing canonical media.
        let mut expired = runtime.store.load(&init.upload_id).await.unwrap().unwrap();
        expired.expires_at_epoch_secs = now_epoch_secs() - 1;
        runtime.store.save(&expired).await.unwrap();
        assert_eq!(runtime.sweep_expired(100, None).await.unwrap(), 1);
        let count: i64 = runtime
            .store
            .database
            .as_ref()
            .unwrap()
            .query_one(
                "SELECT count(*) FROM upload_sessions WHERE upload_id=$1",
                &[&init.upload_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0);
        let b2_storage =
            MediaStorage::new(WriteMode::B2, None, "legacy-bucket".into(), Some(b2)).unwrap();
        let b2_runtime = ResumableRuntime::new(
            b2_storage,
            None,
            "legacy-bucket".into(),
            Some(&database_url),
        )
        .await
        .unwrap();
        let manager = ResumableManager::new(
            b2_runtime.backend.clone(),
            b2_runtime.store.clone(),
            "https://upload.example",
            "https://media.example",
            1024 * 1024,
            86400,
        );
        let aborted = manager
            .init_session(
                "owner",
                ResumableUploadInitRequest {
                    sha256: hash.clone(),
                    size: bytes.len() as u64,
                    content_type: "video/mp4".into(),
                    file_name: None,
                },
            )
            .await
            .unwrap();
        let auth = aborted.required_headers.get("Authorization").unwrap();
        manager
            .upload_chunk(
                &aborted.upload_id,
                Some(auth),
                &range,
                bytes.slice(..1048576),
            )
            .await
            .unwrap();
        manager
            .abort_session(&aborted.upload_id, Some(auth))
            .await
            .unwrap();
        assert!(b2_runtime
            .store
            .load(&aborted.upload_id)
            .await
            .unwrap()
            .is_none());
        assert!(objects
            .lock()
            .unwrap()
            .keys()
            .all(|key| !key.contains(&aborted.upload_id)));
        let expired = manager
            .init_session(
                "owner",
                ResumableUploadInitRequest {
                    sha256: hash.clone(),
                    size: bytes.len() as u64,
                    content_type: "video/mp4".into(),
                    file_name: None,
                },
            )
            .await
            .unwrap();
        manager
            .upload_chunk(
                &expired.upload_id,
                expired
                    .required_headers
                    .get("Authorization")
                    .map(String::as_str),
                &range,
                bytes.slice(..1048576),
            )
            .await
            .unwrap();
        let mut record = b2_runtime
            .store
            .load(&expired.upload_id)
            .await
            .unwrap()
            .unwrap();
        record.expires_at_epoch_secs = now_epoch_secs() - 1;
        b2_runtime.store.save(&record).await.unwrap();
        assert_eq!(b2_runtime.sweep_expired(100, None).await.unwrap(), 1);
        assert!(objects
            .lock()
            .unwrap()
            .keys()
            .all(|key| !key.contains(&expired.upload_id)));
        assert!(objects
            .lock()
            .unwrap()
            .contains_key(&format!("/test-bucket/{hash}")));
        let existing_hash = hex::encode(Sha256::digest(b"original"));
        let wrong_owner = random_hex(32);
        b2_runtime
            .backend
            .storage
            .put(
                &existing_hash,
                Bytes::from_static(b"original"),
                "video/mp4",
                Some(&"a".repeat(64)),
            )
            .await
            .unwrap();
        let outbox = crate::outbox::ProcessingOutbox::connect(
            &database_url,
            "https://processor.example/finalized",
            "fixture-token",
        )
        .await
        .unwrap();
        let manager = manager.with_outbox(Some(outbox.clone()));
        let corrupt = manager
            .init_session(
                &wrong_owner,
                ResumableUploadInitRequest {
                    sha256: existing_hash.clone(),
                    size: 8,
                    content_type: "video/mp4".into(),
                    file_name: None,
                },
            )
            .await
            .unwrap();
        manager
            .upload_chunk(
                &corrupt.upload_id,
                corrupt
                    .required_headers
                    .get("Authorization")
                    .map(String::as_str),
                "bytes 0-7/8",
                Bytes::from_static(b"bad-hash"),
            )
            .await
            .unwrap();
        assert!(matches!(
            manager
                .complete_session(&corrupt.upload_id, &wrong_owner)
                .await,
            Err(ResumableError::UnprocessableEntity(_))
        ));
        assert!(objects
            .lock()
            .unwrap()
            .keys()
            .all(|key| !key.contains(&corrupt.upload_id)));
        let db = b2_runtime.store.database.as_ref().unwrap();
        let intent_count: i64 = db
            .query_one(
                "SELECT count(*) FROM upload_processing_intents WHERE sha256=$1 AND owner=$2",
                &[&existing_hash, &wrong_owner],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            intent_count, 0,
            "rejected bytes must not create recoverable ownership intents"
        );
        let event_count:i64=db.query_one("SELECT count(*) FROM upload_processing_outbox WHERE payload->>'sha256'=$1 AND payload->>'owner'=$2",&[&existing_hash,&wrong_owner]).await.unwrap().get(0);
        assert_eq!(event_count, 0, "rejected bytes must not enqueue processing");
        let uncertain = manager
            .init_session(
                "owner",
                ResumableUploadInitRequest {
                    sha256: "f".repeat(64),
                    size: 8,
                    content_type: "video/mp4".into(),
                    file_name: None,
                },
            )
            .await
            .unwrap();
        let mut uncertain_record = b2_runtime
            .store
            .load(&uncertain.upload_id)
            .await
            .unwrap()
            .unwrap();
        db.execute("INSERT INTO upload_chunks(temp_key,start_offset,end_offset,sha256,objects) VALUES($1,0,7,$2,'[]'::jsonb)",&[&uncertain_record.temp_object,&"f".repeat(64)]).await.unwrap();
        let aborted_unknown = manager
            .abort_session(
                &uncertain.upload_id,
                uncertain
                    .required_headers
                    .get("Authorization")
                    .map(String::as_str),
            )
            .await;
        assert!(
            aborted_unknown.is_err(),
            "abort must retain unresolved writes"
        );
        assert!(b2_runtime
            .store
            .load(&uncertain.upload_id)
            .await
            .unwrap()
            .is_some());
        uncertain_record.expires_at_epoch_secs = now_epoch_secs() - 1;
        b2_runtime.store.save(&uncertain_record).await.unwrap();
        // The fixture returns 404 for its absent chunk. Absence at this instant cannot exclude a late PUT.
        let cleanup = b2_runtime.sweep_expired(100, None).await;
        let retained = b2_runtime.store.load(&uncertain.upload_id).await.unwrap();
        let reservations: i64 = db
            .query_one(
                "SELECT count(*) FROM upload_chunks WHERE temp_key=$1",
                &[&uncertain_record.temp_object],
            )
            .await
            .unwrap()
            .get(0);
        // Remove only this synthetic fixture after collecting evidence, even when the assertion fails.
        db.execute(
            "DELETE FROM upload_chunks WHERE temp_key=$1",
            &[&uncertain_record.temp_object],
        )
        .await
        .unwrap();
        b2_runtime.store.delete(&uncertain.upload_id).await.unwrap();
        assert!(
            cleanup.is_err(),
            "unknown provider outcome must fail cleanup closed"
        );
        assert!(
            retained.is_some(),
            "unknown provider outcome must retain the session"
        );
        assert_eq!(
            reservations, 1,
            "unknown provider outcome must retain its durable reservation"
        );
        task.abort();
    }
}
