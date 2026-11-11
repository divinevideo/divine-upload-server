use super::*;
#[tokio::test]
async fn original_write_schema_has_a_pre_intake_subject() {
    let Some((one, _two)) = database_tests::stores().await else {
        return;
    };
    let db = one.database.lock().await;
    let exists:bool=db.query_one("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='media_storage_attempts' AND column_name='upload_write_intent_id')",&[]).await.unwrap().get(0);
    assert!(
        exists,
        "original upload authority must not require a processing-job row"
    );
}

fn auth() -> crate::NostrEvent {
    use k256::schnorr::{signature::hazmat::PrehashSigner, SigningKey};
    let key = SigningKey::from_bytes(&[17u8; 32]).unwrap();
    let mut event = crate::NostrEvent {
        id: String::new(),
        pubkey: hex::encode(key.verifying_key().to_bytes()),
        created_at: 1,
        kind: 24242,
        tags: vec![vec!["t".into(), "upload".into()]],
        content: String::new(),
        sig: String::new(),
    };
    event.id = crate::compute_event_id(&event).unwrap();
    event.sig = hex::encode(
        key.sign_prehash(&hex::decode(&event.id).unwrap())
            .unwrap()
            .to_bytes(),
    );
    event
}
fn runtime(store: InboxStore) -> crate::original_write::Runtime {
    let r = crate::resumable::ResumableRuntime::legacy_for_test();
    let storage = crate::storage::MediaStorage::new(
        crate::storage::WriteMode::Gcs,
        Some(google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig::default().anonymous(),
        )),
        "test-bucket".into(),
        None,
    )
    .unwrap();
    crate::original_write::Runtime::new(store, storage, r.backend)
}
#[tokio::test]
async fn original_write_concurrent_preparation_persists_one_non_job_identity() {
    let Some((one, two)) = database_tests::stores().await else {
        return;
    };
    let a = runtime(one.clone());
    let b = runtime(two.clone());
    let event = auth();
    let hash = "a".repeat(64);
    let (x, y) = tokio::join!(
        a.prepare_direct(&event, &hash, 12, "video/mp4"),
        b.prepare_direct(&event, &hash, 12, "video/mp4")
    );
    let x = x.expect("authenticated upload must have a durable pre-intake subject");
    let y = y.unwrap();
    assert_eq!(x.intent_id, y.intent_id);
    assert_eq!(x.attempt_id, y.attempt_id);
    let db = one.database.lock().await;
    let row=db.query_one("SELECT subject_kind,processing_job_id,lease_generation,input_revision,gate_claim_request FROM media_storage_attempts WHERE attempt_id=$1",&[&x.attempt_id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "original");
    assert!(row.get::<_, Option<String>>(1).is_none());
    assert!(row.get::<_, Option<i64>>(2).is_none());
    assert!(row.get::<_, Option<i64>>(3).is_none());
    assert!(row.get::<_, Option<serde_json::Value>>(4).is_none());
    assert_eq!(
        db.query_one("SELECT count(*) FROM media_processing_jobs", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_write_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

async fn origin() -> (
    String,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use axum::{extract::State, http::StatusCode, routing::any, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    async fn hit(State(c): State<Arc<AtomicUsize>>) -> StatusCode {
        c.fetch_add(1, Ordering::SeqCst);
        StatusCode::NOT_FOUND
    }
    let counter = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let c = counter.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/*path", any(hit)).with_state(c),
        )
        .await
        .unwrap()
    });
    (endpoint, counter, task)
}
fn auth_header(event: &crate::NostrEvent) -> String {
    use base64::Engine;
    let v = serde_json::json!({"id":event.id,"pubkey":event.pubkey,"created_at":event.created_at,"kind":event.kind,"tags":event.tags,"content":event.content,"sig":event.sig});
    format!(
        "Nostr {}",
        base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&v).unwrap())
    )
}
fn gcs_storage(endpoint: String) -> crate::storage::MediaStorage {
    let cfg = google_cloud_storage::client::ClientConfig {
        storage_endpoint: endpoint,
        ..Default::default()
    }
    .anonymous();
    crate::storage::MediaStorage::new(
        crate::storage::WriteMode::Gcs,
        Some(google_cloud_storage::client::Client::new(cfg)),
        "test-bucket".into(),
        None,
    )
    .unwrap()
}
#[tokio::test]
async fn original_write_http_direct_is_prepared_then_503_before_provider_io() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::sync::{atomic::Ordering, Arc};
    use tower::ServiceExt;
    let Some((one, _)) = database_tests::stores().await else {
        return;
    };
    let (endpoint, calls, server) = origin().await;
    let storage = gcs_storage(endpoint);
    let mut state = crate::tests::test_state(None, 1);
    let st = Arc::get_mut(&mut state).unwrap();
    st.media_storage = storage.clone();
    st.original_write = Some(crate::original_write::Runtime::new(
        one.clone(),
        storage,
        st.resumable_runtime.backend.clone(),
    ));
    let response = crate::build_router(state)
        .oneshot(
            Request::put("/upload")
                .header("authorization", auth_header(&auth()))
                .header("content-type", "video/mp4")
                .body(Body::from("prepared body"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        one.database
            .lock()
            .await
            .query_one("SELECT count(*) FROM upload_write_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let input: serde_json::Value = one
        .database
        .lock()
        .await
        .query_one("SELECT input FROM upload_write_intents", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        input["sha256"],
        hex::encode(Sha256::digest(b"prepared body"))
    );
    assert_eq!(input["size"], 13);
    server.abort();
}

fn session(id: &str, staged: bool) -> crate::resumable::UploadSession {
    crate::resumable::UploadSession {
        processing_outbox_required: None,
        revision: 7,
        chunk_size: 12,
        upload_id: id.into(),
        owner: auth().pubkey,
        final_sha256: "b".repeat(64),
        declared_size: 12,
        content_type: "video/mp4".into(),
        file_name: None,
        expires_at_epoch_secs: crate::resumable::now_epoch_secs() + 3600,
        next_offset: 12,
        session_url: if staged {
            "staged-v1"
        } else {
            "https://synthetic.invalid/session"
        }
        .into(),
        session_token: "synthetic-token".into(),
        temp_object: if staged {
            format!(
                "__resumable/staged/gcs/{}//{id}",
                hex::encode("test-bucket")
            )
        } else {
            format!("__resumable/{id}")
        },
        storage_complete: false,
        finalized_object: None,
    }
}
async fn save_session(one: &InboxStore, s: &crate::resumable::UploadSession) {
    one.database
        .lock()
        .await
        .execute(
            "INSERT INTO upload_sessions(upload_id,revision,state) VALUES($1,$2,$3)",
            &[&s.upload_id, &s.revision, &serde_json::to_value(s).unwrap()],
        )
        .await
        .unwrap();
}
async fn state_with_runtime(one: InboxStore, endpoint: String) -> std::sync::Arc<crate::AppState> {
    let storage = gcs_storage(endpoint);
    let mut state = crate::tests::test_state(None, 1);
    let st = std::sync::Arc::get_mut(&mut state).unwrap();
    st.media_storage = storage.clone();
    st.original_write = Some(crate::original_write::Runtime::new(
        one,
        storage,
        st.resumable_runtime.backend.clone(),
    ));
    state
}
#[tokio::test]
async fn original_write_both_resumable_paths_prepare_without_provider_reads_or_promotion() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;
    for staged in [true, false] {
        let Some((one, _)) = database_tests::stores().await else {
            return;
        };
        let s = session(if staged { "up_staged" } else { "up_legacy" }, staged);
        save_session(&one, &s).await;
        if !staged {
            one.database.lock().await.execute("INSERT INTO upload_processing_intents(sha256,owner,size,content_type,write_mode,gcs_bucket) VALUES($1,$2,12,'video/mp4','gcs','test-bucket')",&[&s.final_sha256,&s.owner]).await.unwrap();
        }
        let (endpoint, calls, server) = origin().await;
        let state = state_with_runtime(one.clone(), endpoint).await;
        let response = crate::build_router(state)
            .oneshot(
                Request::post(format!("/upload/{}/complete", s.upload_id))
                    .header("authorization", auth_header(&auth()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["cache-control"], "private, no-store");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let db = one.database.lock().await;
        let input: serde_json::Value = db
            .query_one("SELECT input FROM upload_write_intents", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(input["byte_evidence"], "session_declared_not_verified");
        assert_eq!(
            input["ingress"],
            if staged {
                "resumable_staged"
            } else {
                "resumable_legacy"
            }
        );
        assert_eq!(
            db.query_one(
                "SELECT revision FROM upload_sessions WHERE upload_id=$1",
                &[&s.upload_id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            7
        );
        assert_eq!(
            db.query_one("SELECT count(*) FROM upload_final_objects", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        server.abort();
    }
}
#[tokio::test]
async fn original_write_session_receipt_replay_is_pg_only() {
    check_session_receipt_replay(true).await;
    check_session_receipt_replay(false).await;
}

async fn check_session_receipt_replay(final_session_saved: bool) {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;
    let Some((one, _)) = database_tests::stores().await else {
        return;
    };
    let mut s = session("up_complete", false);
    if final_session_saved {
        s.finalized_object = Some(s.final_sha256.clone());
    }
    s.storage_complete = true;
    save_session(&one, &s).await;
    let event = crate::processing::MediaFinalizedV1::new(
        &s.final_sha256,
        12,
        "video/mp4",
        &s.owner,
        crate::storage::ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "test-bucket".into(),
            key: s.final_sha256.clone(),
            version: Some("123".into()),
        },
    )
    .unwrap();
    {
        let db = one.database.lock().await;
        db.execute(
            "INSERT INTO upload_processing_outbox(job_id,payload) VALUES($1,$2)",
            &[&event.job_id, &serde_json::to_value(&event).unwrap()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO upload_processing_session_receipts(upload_id,job_id) VALUES($1,$2)",
            &[&s.upload_id, &event.job_id],
        )
        .await
        .unwrap();
    }
    let (endpoint, calls, server) = origin().await;
    let state = state_with_runtime(one.clone(), endpoint).await;
    let response = crate::build_router(state)
        .oneshot(
            Request::post("/upload/up_complete/complete")
                .header("authorization", auth_header(&auth()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        one.database
            .lock()
            .await
            .query_one("SELECT count(*) FROM upload_write_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    server.abort();
}
#[tokio::test]
async fn original_write_untrusted_migrate_and_unknown_legacy_mapping_do_no_io() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;
    let Some((one, _)) = database_tests::stores().await else {
        return;
    };
    let s = session("up_unmapped", false);
    save_session(&one, &s).await;
    let (endpoint, calls, server) = origin().await;
    let state = state_with_runtime(one.clone(), endpoint.clone()).await;
    let router = crate::build_router(state);
    let response = router
        .clone()
        .oneshot(
            Request::post("/upload/up_unmapped/complete")
                .header("authorization", auth_header(&auth()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response=router.oneshot(Request::post("/migrate").header("content-type","application/json").body(Body::from(serde_json::to_vec(&serde_json::json!({"source_url":endpoint,"owner":auth().pubkey,"expected_hash":"b".repeat(64)})).unwrap())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        one.database
            .lock()
            .await
            .query_one("SELECT count(*) FROM upload_write_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    server.abort();
}
#[tokio::test]
async fn original_write_owner_mapping_and_producer_subjects_are_isolated() {
    let Some((one, _)) = database_tests::stores().await else {
        return;
    };
    let a = runtime(one.clone());
    let event = auth();
    let parent = "a".repeat(64);
    let receipt = a
        .prepare_direct(&event, &parent, 12, "video/mp4")
        .await
        .unwrap();
    let retry = a
        .prepare_direct(&event, &parent, 12, "other/hint")
        .await
        .unwrap();
    assert_eq!(receipt.intent_id, retry.intent_id);
    assert!(a
        .prepare_direct(&event, &parent, 13, "video/mp4")
        .await
        .is_err());
    let other = crate::storage::MediaStorage::new(
        crate::storage::WriteMode::Gcs,
        Some(google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig::default().anonymous(),
        )),
        "changed-bucket".into(),
        None,
    )
    .unwrap();
    let b = crate::original_write::Runtime::new(
        one.clone(),
        other,
        crate::resumable::ResumableRuntime::legacy_for_test().backend,
    );
    assert!(b
        .prepare_direct(&event, &parent, 12, "video/mp4")
        .await
        .is_err());
    let mut forged = auth();
    forged.pubkey = "c".repeat(64);
    assert!(a
        .prepare_direct(&forged, &parent, 12, "video/mp4")
        .await
        .is_err());
    let db = one.database.lock().await;
    assert!(journal::read_attempt(&*db, &receipt.attempt_id)
        .await
        .unwrap()
        .is_none());
    let input: serde_json::Value = db
        .query_one(
            "SELECT input FROM upload_write_intents WHERE intent_id=$1",
            &[&receipt.intent_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(input["owner"], event.pubkey);
    assert_eq!(input["destinations"][0]["bucket"], "test-bucket");
    assert_eq!(input["content_type_hint"], "video/mp4");
    assert!(db
        .execute(
            "UPDATE media_storage_attempts SET lease_generation=1 WHERE attempt_id=$1",
            &[&receipt.attempt_id]
        )
        .await
        .is_err());
    assert!(db.execute("INSERT INTO media_storage_operations(attempt_id,position,filename,target,sha256,size,content_type,metadata) VALUES($1,0,'master.m3u8','{}',$2,1,'application/vnd.apple.mpegurl','{}')",&[&receipt.attempt_id,&parent]).await.is_err(),"an original subject must not accept producer artifacts");
}
#[test]
fn original_write_opt_in_is_strict_and_default_off() {
    assert!(!crate::original_write::enabled(None).unwrap());
    assert!(!crate::original_write::enabled(Some("false")).unwrap());
    assert!(crate::original_write::enabled(Some("true")).unwrap());
    assert!(crate::original_write::enabled(Some("1")).is_err());
}

#[tokio::test]
async fn original_write_auth_is_rechecked_after_actual_row_lock_wait() {
    use k256::schnorr::{signature::hazmat::PrehashSigner, SigningKey};
    let Some((one, two)) = database_tests::stores().await else {
        return;
    };
    let runtime = runtime(one.clone());
    let parent = "a".repeat(64);
    let mut event = auth();
    let expires = crate::resumable::now_epoch_secs() + 2;
    event
        .tags
        .push(vec!["expiration".into(), expires.to_string()]);
    event.id = crate::compute_event_id(&event).unwrap();
    event.sig = hex::encode(
        SigningKey::from_bytes(&[17; 32])
            .unwrap()
            .sign_prehash(&hex::decode(&event.id).unwrap())
            .unwrap()
            .to_bytes(),
    );
    let receipt = runtime
        .prepare_direct(&event, &parent, 12, "video/mp4")
        .await
        .unwrap();
    let waiter_pid: i32 = one
        .database
        .lock()
        .await
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let mut db = two.database.lock().await;
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM upload_write_intents WHERE intent_id=$1 FOR UPDATE",
        &[&receipt.intent_id],
    )
    .await
    .unwrap();
    let waiting = tokio::spawn(async move {
        runtime
            .prepare_direct(&event, &parent, 12, "video/mp4")
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2),async{loop{let blocked:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",&[&waiter_pid]).await.unwrap().get(0);if blocked{break;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.unwrap();
    while crate::resumable::now_epoch_secs() <= expires {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    tx.commit().await.unwrap();
    assert!(
        waiting.await.unwrap().is_err(),
        "expired auth cannot become a fresh prepared receipt after waiting"
    );
}

#[tokio::test]
async fn original_write_worker_http_cannot_inspect_original_payload() {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let Some((one, _)) = database_tests::stores().await else {
        return;
    };
    let id = runtime(one.clone())
        .prepare_direct(&auth(), &"a".repeat(64), 12, "video/mp4")
        .await
        .unwrap()
        .attempt_id;
    let settings = InboxSettings::from_lookup(|key| match key {
        "MEDIA_FINALIZED_INBOX_ENABLED" | "MEDIA_PROCESSING_JOURNAL_ENABLED" => Some("true".into()),
        "MEDIA_FINALIZED_INBOX_TOKEN" => Some("original-intake-token".into()),
        "MEDIA_PROCESSING_WORKER_TOKEN" => Some("original-worker-token".into()),
        _ => None,
    })
    .unwrap()
    .unwrap();
    let rt = InboxRuntime {
        store: one,
        settings: Arc::new(settings),
        deadline: std::time::Duration::from_secs(10),
        parent_gate: None,
    };
    let response = router(Some(rt))
        .oneshot(
            Request::post("/internal/media-processing/attempts/inspect")
                .header("authorization", "Bearer original-worker-token")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({"attempt_id":id})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 32768).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"attempt":null})
    );
}
#[tokio::test]
async fn original_write_records_each_mode_without_outbox_dispatch_or_provider_io() {
    use std::sync::atomic::Ordering;
    for mode in [
        crate::storage::WriteMode::Gcs,
        crate::storage::WriteMode::B2,
        crate::storage::WriteMode::Dual,
        crate::storage::WriteMode::B2Primary,
    ] {
        let Some((one, _)) = database_tests::stores().await else {
            return;
        };
        let (endpoint, calls, server) = origin().await;
        let gcs = google_cloud_storage::client::Client::new(
            google_cloud_storage::client::ClientConfig {
                storage_endpoint: endpoint.clone(),
                ..Default::default()
            }
            .anonymous(),
        );
        let b2 = crate::storage::B2Store::new(
            "synthetic".into(),
            "us-west-001".into(),
            "synthetic".into(),
            "synthetic".into(),
        )
        .unwrap()
        .with_endpoint(endpoint);
        let storage =
            crate::storage::MediaStorage::new(mode, Some(gcs), "test-bucket".into(), Some(b2))
                .unwrap();
        let rt = crate::original_write::Runtime::new(
            one.clone(),
            storage,
            crate::resumable::ResumableRuntime::legacy_for_test().backend,
        );
        let result = rt
            .prepare_direct(&auth(), &"a".repeat(64), 40 * 1024 * 1024, "video/mp4")
            .await
            .unwrap();
        let count:i64=one.database.lock().await.query_one("SELECT count(*) FROM upload_write_operations WHERE attempt_id=$1 AND state='prepared'",&[&result.attempt_id]).await.unwrap().get(0);
        assert_eq!(count, if mode.requires_both() { 4 } else { 2 });
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.abort();
    }
}
