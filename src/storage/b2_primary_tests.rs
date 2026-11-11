use super::*;
use axum::{
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
struct Copy {
    bytes: Bytes,
    owner: Option<String>,
    mime: String,
}

#[tokio::test]
async fn declared_processing_cleanup_requires_its_database_binding() {
    use crate::resumable::{
        ResumableBackend, ResumableManager, ResumableRuntime, ResumableUploadInitRequest,
        SessionStore,
    };
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend,
        runtime.store.clone(),
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    )
    .with_outbox(Some(outbox));
    let init = manager
        .init_session(
            &"e".repeat(64),
            ResumableUploadInitRequest {
                sha256: hex::encode(Sha256::digest(b"original")),
                size: 8,
                content_type: "application/octet-stream".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    let mut session = runtime.store.load(&init.upload_id).await.unwrap().unwrap();
    let missing = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await
    .unwrap();
    assert!(missing.backend.historical_cleanup(&session).await.is_err());
    session.processing_outbox_required = Some(false);
    assert!(missing
        .backend
        .historical_cleanup(&session)
        .await
        .unwrap()
        .is_none());
    assert!(f.objects.lock().unwrap().writes.is_empty());
}

#[tokio::test]
async fn expired_unknown_native_sessions_without_receipts_are_cleaned() {
    use crate::resumable::{
        ResumableError, ResumableManager, ResumableRuntime, ResumableUploadInitRequest,
        SessionStore,
    };
    for action in ["head", "complete", "sweep"] {
        for schema in [false, true] {
            let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                return;
            };
            if schema {
                crate::outbox::ProcessingOutbox::connect(
                    &url,
                    "https://processor.invalid/finalized",
                    "fixture-token",
                )
                .await
                .unwrap();
            }
            let f = Fixture::new().await;
            let runtime = ResumableRuntime::new(
                f.storage.with_mode(WriteMode::Gcs).unwrap(),
                f.storage.gcs.clone(),
                "fixture-gcs".into(),
                Some(&url),
            )
            .await
            .unwrap();
            let manager = ResumableManager::new(
                runtime.backend.clone(),
                runtime.store.clone(),
                "https://upload.invalid",
                "https://media.invalid",
                256 * 1024,
                300,
            );
            let owner = "e".repeat(64);
            let init = manager
                .init_session(
                    &owner,
                    ResumableUploadInitRequest {
                        sha256: hex::encode(Sha256::digest(b"original")),
                        size: 8,
                        content_type: "application/octet-stream".into(),
                        file_name: None,
                    },
                )
                .await
                .unwrap();
            manager
                .upload_chunk(
                    &init.upload_id,
                    Some(&init.required_headers["Authorization"]),
                    "bytes 0-7/8",
                    Bytes::from_static(b"original"),
                )
                .await
                .unwrap();
            let db = crate::coordination::connect(&url).await.unwrap();
            db.execute("UPDATE upload_sessions SET state=jsonb_set(state-'processing_outbox_required','{expires_at_epoch_secs}','0'::jsonb)", &[]).await.unwrap();
            match action {
                "head" => assert!(matches!(
                    manager
                        .head_session(
                            &init.upload_id,
                            Some(&init.required_headers["Authorization"])
                        )
                        .await,
                    Err(ResumableError::Gone(_))
                )),
                "complete" => assert!(matches!(
                    manager.complete_session(&init.upload_id, &owner).await,
                    Err(ResumableError::Gone(_))
                )),
                _ => assert_eq!(runtime.sweep_expired(1, None).await.unwrap(), 1),
            }
            assert!(
                runtime.store.load(&init.upload_id).await.unwrap().is_none(),
                "{action}/{schema}"
            );
            assert_eq!(runtime.sweep_expired(1, None).await.unwrap(), 0);
            assert!(f.objects.lock().unwrap().staging.is_empty());
        }
    }
}

#[tokio::test]
async fn mirror_registration_stays_anonymous_with_retained_history() {
    for mode in [WriteMode::Gcs, WriteMode::B2Primary] {
        for global in [false, true] {
            let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                return;
            };
            let outbox = crate::outbox::ProcessingOutbox::connect(
                &url,
                "https://processor.invalid/finalized",
                "fixture-token",
            )
            .await
            .unwrap();
            let f = Fixture::new().await;
            let owner = "e".repeat(64);
            let hash = hex::encode(Sha256::digest(b"original"));
            outbox
                .begin_upload(
                    &hash,
                    8,
                    "application/octet-stream",
                    &owner,
                    &f.storage.with_mode(WriteMode::B2).unwrap(),
                )
                .await
                .unwrap();
            let configured = f.storage.with_mode(mode).unwrap();
            *configured.database_url.lock().await = Some(url.clone());
            let mut state = crate::tests::test_state(None, 1);
            let mutable = Arc::get_mut(&mut state).unwrap();
            mutable.media_storage = configured;
            mutable.processing_outbox = global.then_some(outbox);
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), b"original").unwrap();
            crate::finish_migrate_storage(
                &state,
                &crate::MigrateRequest {
                    source_url: "https://cdn.divine.video/fixture".into(),
                    expected_hash: Some(hash.clone()),
                    owner: Some(owner.clone()),
                },
                hash.clone(),
                8,
                "application/octet-stream".into(),
                file.path(),
            )
            .await
            .unwrap();
            let db = crate::coordination::connect(&url).await.unwrap();
            assert!(!db
                .query_one(
                    "SELECT completed FROM upload_processing_intents WHERE sha256=$1 AND owner=$2",
                    &[&hash, &owner],
                )
                .await
                .unwrap()
                .get::<_, bool>(0));
            let rows = db
                .query("SELECT payload FROM upload_processing_outbox", &[])
                .await
                .unwrap();
            assert_eq!(rows.len(), usize::from(global));
            for row in rows {
                let payload: serde_json::Value = row.get(0);
                assert!(payload["owner"].is_null());
            }
        }
    }
}
#[derive(Default)]
struct Objects {
    records: std::collections::HashMap<String, (i64, Bytes)>,
    staging: std::collections::HashMap<String, Bytes>,
    resume_key: Option<String>,
    fail_backup_admission_once: bool,
    lose_admission_ack_once: bool,
    fail_final_admission_once: Option<u16>,
    lose_final_admission_ack_once: bool,
    throttle_final_admission: bool,
    land_stale_final_before_fence: bool,
    final_admission_requests: Vec<(i64, Bytes)>,
    record_downloads: usize,
    fail_record_download_once: bool,
    change_attempt_on_ack_loss: bool,
    hide_lost_admission_ack: bool,
    fail_record_read_once: bool,
    lose_gcs_put_ack: bool,
    lose_b2_put_ack: bool,
    latest_b2_version: Option<String>,
    read_versions: Vec<String>,
    corrupt_b2: bool,
    b2: Option<Copy>,
    gcs: Option<Copy>,
    writes: Vec<&'static str>,
    parts: Vec<Bytes>,
    multipart_owner: Option<String>,
    multipart_mime: String,
    fail_gcs: bool,
    deny_b2: bool,
    corrupt_gcs: bool,
    null_b2_head: bool,
    wrong_pinned_b2_head: bool,
    fail_b2_write: bool,
}
struct Fixture {
    storage: MediaStorage,
    objects: Arc<Mutex<Objects>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn metadata(copy: &Copy) -> serde_json::Value {
    let owner = copy
        .owner
        .as_ref()
        .map(|o| serde_json::json!({"owner":o}))
        .unwrap_or(serde_json::json!({}));
    serde_json::json!({"name":"media","bucket":"fixture-gcs","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"fixture-gcs/media/1","size":copy.bytes.len().to_string(),"contentType":copy.mime,"metadata":owner})
}
fn admission_record(objects: &Objects) -> &(i64, Bytes) {
    objects
        .records
        .iter()
        .find(|(name, _)| name.starts_with("__resumable/b2-primary-writes/"))
        .map(|(_, record)| record)
        .unwrap()
}
fn find(bytes: &[u8], pattern: &[u8]) -> usize {
    bytes
        .windows(pattern.len())
        .position(|s| s == pattern)
        .expect("multipart boundary")
}
fn multipart(bytes: Bytes) -> Copy {
    let start = find(&bytes, b"\r\n\r\n") + 4;
    let end = start + find(&bytes[start..], b"\r\n--");
    let value: serde_json::Value = serde_json::from_slice(&bytes[start..end]).unwrap();
    let start = end + find(&bytes[end..], b"\r\n\r\n") + 4;
    let end = bytes.windows(4).rposition(|s| s == b"\r\n--").unwrap();
    Copy {
        bytes: bytes.slice(start..end),
        owner: value["metadata"]["owner"].as_str().map(str::to_owned),
        mime: value["contentType"].as_str().unwrap().into(),
    }
}
async fn origin(
    State(state): State<Arc<Mutex<Objects>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut f = state.lock().unwrap();
    if uri.path() == "/fixture-resume" {
        let key = f.resume_key.clone().unwrap();
        if method == Method::DELETE {
            f.staging.remove(&key);
            return StatusCode::NO_CONTENT.into_response();
        }
        let range = headers["content-range"].to_str().unwrap();
        let total = range.split('/').nth(1).unwrap().parse::<usize>().unwrap();
        let start = range
            .strip_prefix("bytes ")
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let current = f.staging.entry(key).or_default();
        assert_eq!(start, current.len());
        let mut bytes = current.to_vec();
        bytes.extend_from_slice(&body);
        *current = Bytes::from(bytes);
        if current.len() < total {
            return StatusCode::PERMANENT_REDIRECT.into_response();
        }
        return Json(metadata(&Copy {
            bytes: current.clone(),
            owner: Some("fixture-owner".into()),
            mime: "application/octet-stream".into(),
        }))
        .into_response();
    }
    if uri.path().contains("/storage/") {
        let query: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
                .into_owned()
                .collect();
        let path_name = uri.path().split("/o/").nth(1).unwrap_or("");
        let decoded: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(format!("name={path_name}").as_bytes())
                .into_owned()
                .collect();
        let name = query
            .get("name")
            .or_else(|| decoded.get("name"))
            .cloned()
            .unwrap_or_default();
        if method == Method::POST && uri.path().contains("/copyTo/") {
            assert_eq!(query.get("ifGenerationMatch").unwrap(), "0");
            assert!(f.gcs.is_none());
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let bytes = f.staging.values().next().unwrap().clone();
            f.gcs = Some(Copy {
                bytes,
                owner: value["metadata"]["owner"].as_str().map(str::to_owned),
                mime: value["contentType"].as_str().unwrap().into(),
            });
            f.writes.push("gcs");
            return Json(metadata(f.gcs.as_ref().unwrap())).into_response();
        }
        if query.get("uploadType").is_some_and(|v| v == "resumable") {
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let key = value["name"].as_str().unwrap().to_owned();
            assert!(key.starts_with("__resumable/"));
            assert_eq!(query.get("ifGenerationMatch").unwrap(), "0");
            f.resume_key = Some(key);
            return (
                [(
                    "location",
                    format!(
                        "http://{}/fixture-resume",
                        headers["host"].to_str().unwrap()
                    ),
                )],
                "",
            )
                .into_response();
        }
        if name.starts_with("__resumable/b2_primary/") || name.starts_with("__resumable/uploads/") {
            if method == Method::DELETE {
                f.staging.remove(&name);
                return StatusCode::NO_CONTENT.into_response();
            }
            let Some(bytes) = f.staging.get(&name) else {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error":{"code":404,"message":"missing","errors":[]}})),
                )
                    .into_response();
            };
            if query.get("alt").is_some_and(|v| v == "media") {
                return bytes.clone().into_response();
            }
            return Json(metadata(&Copy {
                bytes: bytes.clone(),
                owner: Some("fixture-owner".into()),
                mime: "application/octet-stream".into(),
            }))
            .into_response();
        }
        if name.starts_with("__resumable/") {
            let error = |code: u16| {
                (
                    StatusCode::from_u16(code).unwrap(),
                    Json(
                        serde_json::json!({"error":{"code":code,"message":"fixture","errors":[]}}),
                    ),
                )
                    .into_response()
            };
            if method == Method::POST {
                let expected = query
                    .get("ifGenerationMatch")
                    .expect("control records must use CAS")
                    .parse::<i64>()
                    .unwrap();
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                if value["gcs"]["state"] == "verified" {
                    f.final_admission_requests.push((expected, body.clone()));
                    if f.throttle_final_admission {
                        return error(429);
                    }
                    if let Some(status) = f.fail_final_admission_once.take() {
                        return error(status);
                    }
                }
                if f.fail_backup_admission_once && value["gcs"]["state"] == "issued" {
                    f.fail_backup_admission_once = false;
                    return error(503);
                }
                let current = f.records.get(&name).map(|r| r.0).unwrap_or(0);
                if f.land_stale_final_before_fence
                    && expected == current
                    && value["b2"]["state"] == "verified"
                    && value["gcs"]["state"] == "acknowledged"
                {
                    // An older actor's final receipt lands first, so this fence loses.
                    f.land_stale_final_before_fence = false;
                    let mut landed: serde_json::Value =
                        serde_json::from_slice(&f.records[&name].1).unwrap();
                    landed["gcs"]["state"] = "verified".into();
                    let landed = Bytes::from(serde_json::to_vec(&landed).unwrap());
                    f.records.insert(name.clone(), (current + 1, landed));
                    return error(412);
                }
                if expected != current {
                    return error(412);
                }
                f.records.insert(name.clone(), (current + 1, body));
                if value["gcs"]["state"] == "verified" && f.lose_final_admission_ack_once {
                    f.lose_final_admission_ack_once = false;
                    return error(503);
                }
                if expected == 0 && f.lose_admission_ack_once {
                    f.lose_admission_ack_once = false;
                    if f.change_attempt_on_ack_loss {
                        let mut changed = value;
                        changed["attempt"] = "another-fixture-attempt".into();
                        f.records.insert(
                            name.clone(),
                            (
                                current + 2,
                                Bytes::from(serde_json::to_vec(&changed).unwrap()),
                            ),
                        );
                    }
                    f.fail_record_read_once = f.hide_lost_admission_ack;
                    return error(503);
                }
            }
            if method == Method::GET && f.fail_record_read_once {
                f.fail_record_read_once = false;
                return error(503);
            }
            if query.get("alt").is_some_and(|v| v == "media") && f.records.contains_key(&name) {
                f.record_downloads += 1;
                if f.fail_record_download_once {
                    f.fail_record_download_once = false;
                    return error(503);
                }
            }
            let Some((generation, bytes)) = f.records.get(&name) else {
                return error(404);
            };
            if query.get("alt").is_some_and(|v| v == "media") {
                if let Some(expected) = query.get("generation") {
                    assert_eq!(expected, &generation.to_string());
                }
                return bytes.clone().into_response();
            }
            return Json(serde_json::json!({"name":name,"bucket":"fixture-gcs","generation":generation.to_string(),"metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"test","size":bytes.len().to_string()})).into_response();
        }
        if method == Method::POST {
            if f.fail_gcs {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            assert!(uri
                .query()
                .unwrap_or_default()
                .contains("ifGenerationMatch=0"));
            assert!(f.gcs.is_none(), "conditional backup must not overwrite");
            let copy = multipart(body);
            f.writes.push("gcs");
            f.gcs = Some(copy);
            if f.lose_gcs_put_ack {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            return Json(metadata(f.gcs.as_ref().unwrap())).into_response();
        }
        let Some(copy) = f.gcs.as_ref() else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":{"code":404,"message":"No such object","errors":[]}}))).into_response();
        };
        if uri.query().unwrap_or_default().contains("alt=media") {
            assert!(uri.query().unwrap().contains("generation=1"));
            return if f.corrupt_gcs {
                Bytes::from_static(b"corrupt")
            } else {
                copy.bytes.clone()
            }
            .into_response();
        }
        return Json(metadata(copy)).into_response();
    }
    if f.deny_b2 {
        return StatusCode::FORBIDDEN.into_response();
    }
    let query = uri.query().unwrap_or_default();
    if method == Method::POST && query == "uploads=" {
        f.multipart_owner = headers
            .get("x-amz-meta-owner")
            .map(|v| v.to_str().unwrap().into());
        f.multipart_mime = headers["content-type"].to_str().unwrap().into();
        return "<InitiateMultipartUploadResult><UploadId>bounded-upload</UploadId></InitiateMultipartUploadResult>".into_response();
    }
    if method == Method::PUT && query.contains("uploadId=") {
        f.parts.push(body);
        return ([("etag", "\"part-etag\"")], "").into_response();
    }
    if method == Method::POST && query.contains("uploadId=") {
        let bytes = f
            .parts
            .iter()
            .flat_map(|p| p.iter().copied())
            .collect::<Vec<_>>();
        f.b2 = Some(Copy {
            bytes: Bytes::from(bytes),
            owner: f.multipart_owner.clone(),
            mime: f.multipart_mime.clone(),
        });
        f.writes.push("b2");
        return (
            [("x-amz-version-id", "b2-version-1")],
            "<CompleteMultipartUploadResult><ETag>not-sha</ETag></CompleteMultipartUploadResult>",
        )
            .into_response();
    }
    if method == Method::PUT {
        if f.fail_b2_write {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        assert!(f.b2.is_none(), "retry must reuse verified primary");
        f.b2 = Some(Copy {
            bytes: body,
            owner: headers
                .get("x-amz-meta-owner")
                .map(|v| v.to_str().unwrap().into()),
            mime: headers["content-type"].to_str().unwrap().into(),
        });
        f.writes.push("b2");
        if f.lose_b2_put_ack {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        return ([("x-amz-version-id", "b2-version-1")], "").into_response();
    }
    let Some(copy) = f.b2.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if method == Method::HEAD {
        let version = if f.null_b2_head {
            "null"
        } else if f.wrong_pinned_b2_head && !query.is_empty() {
            "foreign-version"
        } else if query.is_empty() {
            f.latest_b2_version.as_deref().unwrap_or("b2-version-1")
        } else {
            "b2-version-1"
        };
        let mut response = (
            [
                ("x-amz-version-id", version),
                ("content-type", copy.mime.as_str()),
            ],
            copy.bytes.clone(),
        )
            .into_response();
        if let Some(owner) = &copy.owner {
            response
                .headers_mut()
                .insert("x-amz-meta-owner", owner.parse().unwrap());
        }
        return response;
    }
    let version = if query.contains("versionId=b2-version-2") {
        "b2-version-2"
    } else {
        assert!(
            query.contains("versionId=b2-version-1"),
            "byte read must be version-pinned"
        );
        "b2-version-1"
    };
    f.read_versions.push(version.into());
    (
        [("x-amz-version-id", version)],
        if f.corrupt_b2 {
            Bytes::from_static(b"corrupt")
        } else {
            copy.bytes.clone()
        },
    )
        .into_response()
}
impl Fixture {
    async fn new() -> Self {
        let objects = Arc::new(Mutex::new(Objects::default()));
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
        let gcs = GcsClient::new(
            google_cloud_storage::client::ClientConfig {
                storage_endpoint: endpoint.clone(),
                ..Default::default()
            }
            .anonymous(),
        );
        let b2 = B2Store::new(
            "fixture-b2".into(),
            "us-west-001".into(),
            "fixture".into(),
            "fixture".into(),
        )
        .unwrap()
        .with_endpoint(endpoint);
        let mode = "b2_primary".parse().expect("explicit B2-primary policy");
        let storage = MediaStorage::new(mode, Some(gcs), "fixture-gcs".into(), Some(b2)).unwrap();
        Self {
            storage,
            objects,
            task,
        }
    }
}

#[tokio::test]
async fn b2_primary_writes_primary_before_backup_and_retries_without_rewrite() {
    let f = Fixture::new().await;
    let result = f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("first-owner"),
        )
        .await
        .unwrap();
    assert_eq!(
        result
            .objects
            .iter()
            .map(|r| r.provider)
            .collect::<Vec<_>>(),
        [StorageProvider::B2, StorageProvider::Gcs]
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
    assert_eq!(result.objects[0].version.as_deref(), Some("b2-version-1"));
    assert_eq!(
        f.storage.stat("media").await.unwrap().unwrap().reference,
        result.objects[0]
    );
    let replay = f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("new-owner"),
        )
        .await
        .unwrap();
    assert_eq!(replay.objects, result.objects);
    assert!(!replay.newly_stored);
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn b2_primary_unknown_backup_outcome_is_pending_and_never_blindly_reissued() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_gcs = true;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("first-owner")
        )
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    f.objects.lock().unwrap().fail_gcs = false;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("new-owner")
        )
        .await
        .is_err());
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2"]);
    assert!(objects.gcs.is_none());
}

#[tokio::test]
async fn b2_primary_cannot_fallback_to_backup_or_hide_primary_denial() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().gcs = Some(Copy {
        bytes: Bytes::from_static(b"original"),
        owner: Some("historical-owner".into()),
        mime: "video/mp4".into(),
    });
    assert!(f.storage.stat("media").await.unwrap().is_none());
    f.objects.lock().unwrap().deny_b2 = true;
    assert!(f.storage.stat("media").await.is_err());
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("caller")
        )
        .await
        .is_err());
    assert!(f.objects.lock().unwrap().writes.is_empty());
}

#[tokio::test]
async fn b2_primary_cache_read_requires_matching_backup_bytes() {
    let f = Fixture::new().await;
    f.storage
        .put("media", Bytes::from_static(b"original"), "image/jpeg", None)
        .await
        .unwrap();
    assert_eq!(
        f.storage.read_verified("media").await.unwrap(),
        Bytes::from_static(b"original")
    );
    f.objects.lock().unwrap().corrupt_gcs = true;
    assert!(f.storage.read_verified("media").await.is_err());
}

#[tokio::test]
async fn b2_primary_rejects_null_primary_version_before_claiming_authority() {
    let f = Fixture::new().await;
    {
        let mut objects = f.objects.lock().unwrap();
        objects.b2 = Some(Copy {
            bytes: Bytes::from_static(b"original"),
            owner: None,
            mime: "application/octet-stream".into(),
        });
        objects.null_b2_head = true;
    }
    assert!(
        f.storage.stat("media").await.is_err(),
        "null is not immutable version authority"
    );
}

#[tokio::test]
async fn b2_primary_pinned_metadata_cannot_retarget_to_another_version() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().wrong_pinned_b2_head = true;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner")
        )
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
}

#[tokio::test]
async fn b2_primary_primary_write_failure_never_writes_backup() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_b2_write = true;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner")
        )
        .await
        .is_err());
    assert!(f.objects.lock().unwrap().writes.is_empty());
}

#[tokio::test]
async fn b2_primary_preserves_existing_gcs_provenance_while_bootstrapping_primary() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().gcs = Some(Copy {
        bytes: Bytes::from_static(b"original"),
        owner: Some("historical-owner".into()),
        mime: "video/mp4".into(),
    });
    let result = f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("retry-owner"),
        )
        .await
        .unwrap();
    assert_eq!(result.objects[0].provider, StorageProvider::B2);
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2"]);
    assert_eq!(
        objects.b2.as_ref().unwrap().owner.as_deref(),
        Some("historical-owner")
    );
    assert_eq!(objects.b2.as_ref().unwrap().mime, "video/mp4");
}

#[tokio::test]
async fn b2_primary_streamed_file_writes_both_pinned_copies() {
    let f = Fixture::new().await;
    let file = tempfile::NamedTempFile::new().unwrap();
    let bytes = vec![42u8; 9 * 1024 * 1024];
    tokio::fs::write(file.path(), &bytes).await.unwrap();
    let result = f
        .storage
        .put_file("media", file.path(), "video/mp4", Some("first-owner"))
        .await
        .unwrap();
    assert_eq!(
        result
            .objects
            .iter()
            .map(|r| r.provider)
            .collect::<Vec<_>>(),
        [StorageProvider::B2, StorageProvider::Gcs]
    );
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(objects.b2.as_ref().unwrap().bytes.as_ref(), bytes);
    assert_eq!(objects.gcs.as_ref().unwrap().bytes.as_ref(), bytes);
}

async fn database() -> Option<(crate::outbox::ProcessingOutbox, tokio_postgres::Client)> {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        eprintln!("skipping B2-primary PostgreSQL proof: TEST_COORDINATION_DATABASE_URL not set");
        return None;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    Some((outbox, crate::coordination::connect(&url).await.unwrap()))
}

#[tokio::test]
async fn b2_primary_finalization_pins_b2_source_then_required_gcs_replica() {
    let Some((outbox, db)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let bytes = Bytes::from_static(b"original");
    let hash = hex::encode(Sha256::digest(&bytes));
    let owner = "e".repeat(64);
    let pending = outbox
        .begin_or_resume_upload(&hash, 8, "application/octet-stream", &owner, &f.storage)
        .await
        .unwrap();
    let written = pending
        .put(&hash, bytes, "application/octet-stream", Some(&owner))
        .await
        .unwrap();
    let mut wrong = written.objects.clone();
    wrong.reverse();
    assert!(outbox
        .enqueue_upload(&hash, 8, "application/octet-stream", &owner, wrong)
        .await
        .is_err());
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    outbox
        .enqueue_upload(
            &hash,
            8,
            "application/octet-stream",
            &owner,
            written.objects.clone(),
        )
        .await
        .unwrap();
    let payload: serde_json::Value = db
        .query_one("SELECT payload FROM upload_processing_outbox", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        payload["source"],
        serde_json::to_value(&written.objects[0]).unwrap()
    );
    assert_eq!(
        payload["replicas"],
        serde_json::to_value(&written.objects[1..]).unwrap()
    );
    assert_eq!(payload["source"]["provider"], "b2");
    assert_eq!(payload["replicas"][0]["provider"], "gcs");
    assert_eq!(
        db.query_one("SELECT write_mode FROM upload_processing_intents", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "b2_primary"
    );
    assert_eq!(
        crate::outbox::source_objects_from_client(&db, &hash)
            .await
            .unwrap(),
        written.objects
    );
    let history = outbox.source_history(&hash).await.unwrap();
    assert!(
        history.explicit_primary,
        "the committed event retains its authoritative B2 plus GCS replica provenance"
    );
    assert_eq!(history.objects, written.objects);
}

#[tokio::test]
async fn b2_primary_partial_write_stays_pending_and_retry_ignores_new_admission_mode() {
    let Some((outbox, db)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let hash = hex::encode(Sha256::digest(b"original"));
    let owner = "e".repeat(64);
    outbox
        .begin_or_resume_upload(&hash, 8, "application/octet-stream", &owner, &f.storage)
        .await
        .unwrap();
    f.objects.lock().unwrap().fail_gcs = true;
    assert!(f
        .storage
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "application/octet-stream",
            Some(&owner)
        )
        .await
        .is_err());
    assert_eq!(outbox.recover_intents(&f.storage, 1, 16).await.unwrap(), 0);
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let rolled_back_admission = f.storage.with_mode(WriteMode::Gcs).unwrap();
    let retry = outbox
        .begin_or_resume_upload(&hash, 8, "changed/hint", &owner, &rolled_back_admission)
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(retry.mode).unwrap(), "b2_primary");
    f.objects.lock().unwrap().fail_gcs = false;
    assert!(retry
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("retry-owner")
        )
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
async fn b2_primary_admission_does_not_reinterpret_pending_legacy_dual() {
    let Some((outbox, _db)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let hash = "d".repeat(64);
    let owner = "e".repeat(64);
    outbox
        .begin_or_resume_upload(
            &hash,
            8,
            "application/octet-stream",
            &owner,
            &f.storage.with_mode(WriteMode::Dual).unwrap(),
        )
        .await
        .unwrap();
    let retry = outbox
        .begin_or_resume_upload(&hash, 8, "application/octet-stream", &owner, &f.storage)
        .await
        .unwrap();
    assert_eq!(retry.mode, WriteMode::Dual);
    assert_eq!(
        crate::outbox::required_destinations(retry.mode, "fixture-gcs", Some("fixture-b2"))
            .unwrap()[0]
            .0,
        StorageProvider::Gcs
    );
}

#[tokio::test]
async fn b2_primary_direct_video_requires_ordered_backup_thumbnail_receipt() {
    let Some((outbox, db)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let hash = "d".repeat(64);
    let owner = "e".repeat(64);
    let pending = outbox
        .begin_direct_upload(&hash, 8, "video/mp4", &owner, &f.storage)
        .await
        .unwrap();
    let reservation = pending
        .thumbnail
        .expect("new mode retains strict dual thumbnail barrier");
    let originals = vec![
        ObjectRef {
            provider: StorageProvider::B2,
            bucket: "fixture-b2".into(),
            key: hash.clone(),
            version: Some("b2-original".into()),
        },
        ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-gcs".into(),
            key: hash.clone(),
            version: Some("1".into()),
        },
    ];
    let mut proof = crate::outbox::VerifiedThumbnail {
        sha256: "a".repeat(64),
        size: 13,
        objects: originals
            .iter()
            .map(|r| ObjectRef {
                key: format!("{hash}.jpg"),
                version: Some(
                    if r.provider == StorageProvider::Gcs {
                        "2"
                    } else {
                        "b2-jpeg"
                    }
                    .into(),
                ),
                ..r.clone()
            })
            .collect(),
    };
    proof.objects.reverse();
    assert!(outbox
        .record_thumbnail(
            &hash,
            &owner,
            &reservation,
            originals.clone(),
            proof.clone()
        )
        .await
        .is_err());
    assert!(outbox
        .enqueue_upload(&hash, 8, "video/mp4", &owner, originals.clone())
        .await
        .is_err());
    proof.objects.reverse();
    outbox
        .record_thumbnail(&hash, &owner, &reservation, originals.clone(), proof)
        .await
        .unwrap();
    outbox
        .enqueue_upload(&hash, 8, "video/mp4", &owner, originals)
        .await
        .unwrap();
    let receipt: serde_json::Value = db
        .query_one(
            "SELECT sync_thumbnail_receipt FROM upload_processing_intents",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipt["originals"][0]["provider"], "b2");
    assert_eq!(receipt["thumbnail"]["objects"][1]["provider"], "gcs");
}

#[tokio::test]
async fn b2_primary_session_keeps_its_policy_after_admission_rollback() {
    use crate::resumable::{
        ResumableManager, ResumableRuntime, ResumableUploadInitRequest, SessionStore,
    };
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend.clone(),
        runtime.store.clone(),
        "https://upload.invalid",
        "https://media.invalid",
        1024 * 1024,
        86400,
    );
    let init = manager
        .init_session(
            &"e".repeat(64),
            ResumableUploadInitRequest {
                sha256: "d".repeat(64),
                size: 8,
                content_type: "application/octet-stream".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    let saved = runtime.store.load(&init.upload_id).await.unwrap().unwrap();
    assert!(saved.temp_object.starts_with("__resumable/b2_primary/"));
    let rollback = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(rollback.backend.finalization_storage(&saved).unwrap().mode).unwrap(),
        "b2_primary"
    );
    let mut unavailable = f.storage.with_mode(WriteMode::Gcs).unwrap();
    unavailable.b2 = None;
    let unavailable = ResumableRuntime::new(
        unavailable,
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    assert!(unavailable.backend.finalization_storage(&saved).is_err());
}

#[tokio::test]
async fn b2_primary_uses_existing_gcs_sessions_without_new_database() {
    let f = Fixture::new().await;
    f.storage
        .initialize_coordination(None)
        .await
        .expect("no new DB for new mode");
    let result = crate::resumable::ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await;
    assert!(
        result.is_ok(),
        "B2 primary must not require a coordination database: {}",
        result.err().map(|e| e.to_string()).unwrap_or_default()
    );
    let old_dual = crate::resumable::ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Dual).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await;
    assert!(
        old_dual.is_err(),
        "legacy dual coordination policy stays unchanged"
    );
}

#[tokio::test]
async fn b2_primary_can_retry_verified_primary_only_when_backup_was_not_issued() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_backup_admission_once = true;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("first-owner")
        )
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    let result = f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("other-owner"),
        )
        .await
        .unwrap();
    assert_eq!(result.objects.len(), 2);
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(
        objects.gcs.as_ref().unwrap().owner.as_deref(),
        Some("first-owner")
    );
}
#[tokio::test]
async fn b2_primary_records_actual_ack_before_verification_failure_and_blocks_reissue() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().corrupt_b2 = true;
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    {
        let objects = f.objects.lock().unwrap();
        let (_, bytes) = objects.records.values().next().unwrap();
        let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(value["b2"]["state"], "acknowledged");
        assert_eq!(value["b2"]["object"]["version"], "b2-version-1");
        assert_eq!(value["gcs"]["state"], "not_issued");
    }
    f.objects.lock().unwrap().corrupt_b2 = false;
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
}
#[tokio::test]
async fn b2_primary_concurrent_first_writers_cannot_rewrite_winner_metadata() {
    let f = Fixture::new().await;
    let (one, two) = tokio::join!(
        f.storage.put(
            "media",
            Bytes::from_static(b"original"),
            "image/jpeg",
            Some("owner-a")
        ),
        f.storage.put(
            "media",
            Bytes::from_static(b"original"),
            "image/png",
            Some("owner-b")
        )
    );
    assert!(one.is_ok() || two.is_ok());
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(
        objects.b2.as_ref().unwrap().owner,
        objects.gcs.as_ref().unwrap().owner
    );
    assert_eq!(
        objects.b2.as_ref().unwrap().mime,
        objects.gcs.as_ref().unwrap().mime
    );
}
#[tokio::test]
async fn b2_primary_existing_gcs_two_chunk_session_promotes_and_replays_after_mode_rollback() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend.clone(),
        runtime.store.clone(),
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    );
    let bytes = Bytes::from(vec![42; 256 * 1024 + 13]);
    let hash = hex::encode(Sha256::digest(&bytes));
    let init = manager
        .init_session(
            "fixture-owner",
            ResumableUploadInitRequest {
                sha256: hash.clone(),
                size: bytes.len() as u64,
                content_type: "application/octet-stream".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    let auth = init.required_headers["Authorization"].clone();
    manager
        .upload_chunk(
            &init.upload_id,
            Some(&auth),
            &format!("bytes 0-262143/{}", bytes.len()),
            bytes.slice(..262144),
        )
        .await
        .unwrap();
    manager
        .upload_chunk(
            &init.upload_id,
            Some(&auth),
            &format!("bytes 262144-262156/{}", bytes.len()),
            bytes.slice(262144..),
        )
        .await
        .unwrap();
    assert!(
        f.objects.lock().unwrap().writes.is_empty(),
        "chunks are staging, not canonical promotion"
    );
    let rolled = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await
    .unwrap();
    let rolled_manager = ResumableManager::new(
        rolled.backend,
        rolled.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    );
    let complete = rolled_manager
        .complete_session(&init.upload_id, "fixture-owner")
        .await
        .unwrap();
    assert_eq!(complete.sha256, hash);
    assert_eq!(complete.url, format!("https://media.invalid/{hash}"));
    let replay = rolled_manager
        .complete_session(&init.upload_id, "fixture-owner")
        .await
        .unwrap();
    assert!(!replay.newly_stored);
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(objects.b2.as_ref().unwrap().bytes, bytes);
    assert_eq!(objects.gcs.as_ref().unwrap().bytes, bytes);
    assert!(objects.staging.is_empty());
}

#[tokio::test]
async fn b2_primary_rejects_changed_mapping_and_input_under_existing_admission() {
    let f = Fixture::new().await;
    f.storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner"),
        )
        .await
        .unwrap();
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"different"),
            "video/mp4",
            Some("owner")
        )
        .await
        .is_err());
    {
        let mut objects = f.objects.lock().unwrap();
        let (_, bytes) = objects.records.values_mut().next().unwrap();
        let mut record: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        record["b2_bucket"] = "other-bucket".into();
        *bytes = Bytes::from(serde_json::to_vec(&record).unwrap());
    }
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner")
        )
        .await
        .is_err());
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn b2_primary_rollback_keeps_no_database_drain_capability() {
    let f = Fixture::new().await;
    let rollback = f.storage.with_mode(WriteMode::Gcs).unwrap();
    rollback
        .initialize_coordination(None)
        .await
        .expect("GCS admissions must retain B2-primary drain capability without a new DB");
    let old_dual = f.storage.with_mode(WriteMode::Dual).unwrap();
    assert!(
        old_dual.initialize_coordination(None).await.is_err(),
        "historical B2/dual database guard is unchanged"
    );
}

#[tokio::test]
async fn b2_primary_rollback_preserves_pending_direct_policy_and_new_gcs_admissions() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_backup_admission_once = true;
    assert!(f
        .storage
        .put(
            "media",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner")
        )
        .await
        .is_err());
    let rollback = f.storage.with_mode(WriteMode::Gcs).unwrap();
    assert_eq!(
        rollback.for_recorded_write("untouched").await.unwrap().mode,
        WriteMode::Gcs
    );
    let accepted = rollback.for_recorded_write("media").await.unwrap();
    assert_eq!(accepted.mode, WriteMode::B2Primary);
    let result = accepted
        .put(
            "media",
            Bytes::from_static(b"original"),
            "changed/hint",
            Some("retry-owner"),
        )
        .await
        .unwrap();
    assert_eq!(
        result
            .objects
            .iter()
            .map(|r| r.provider)
            .collect::<Vec<_>>(),
        [StorageProvider::B2, StorageProvider::Gcs]
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn b2_primary_review_missing_b2_rollback_binding_never_downgrades_accepted_policy() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_backup_admission_once = true;
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    let rollback = MediaStorage::new(
        WriteMode::Gcs,
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .unwrap();
    assert!(
        rollback.for_recorded_write("media").await.is_err(),
        "missing pinned B2 binding must fail before GCS-only writes"
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
}
#[tokio::test]
async fn b2_primary_review_lost_initial_admission_ack_recovers_by_exact_readback() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().lose_admission_ack_once = true;
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .expect("exact admission readback recovers the lost acknowledgement");
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn primary_final_receipt_recovers_a_lost_control_write_ack() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().lose_final_admission_ack_once = true;
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .expect("read back the exact committed receipt after a lost ACK");
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn primary_lost_control_ack_with_unreadable_body_stays_unresolved() {
    let f = Fixture::new().await;
    {
        let mut objects = f.objects.lock().unwrap();
        objects.lose_final_admission_ack_once = true;
        objects.fail_record_download_once = true;
    }
    // The generation moved, but without the body nothing proves the commit was
    // this actor's, so the write must not be reported as done.
    let error = f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("update unresolved"));
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.record_downloads, 1);
    assert_eq!(objects.writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn primary_final_receipt_retries_control_write_rate_limits() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_final_admission_once = Some(429);
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .expect("retry a throttled conditional control write");
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(objects.final_admission_requests.len(), 2);
    assert_eq!(
        objects.final_admission_requests[0],
        objects.final_admission_requests[1]
    );
    // The unchanged record generation already shows the throttled write did
    // not land, so its body is never downloaded.
    assert_eq!(objects.record_downloads, 0);
}

#[tokio::test]
async fn primary_control_retries_have_a_fixed_budget() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().throttle_final_admission = true;
    let error = f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap_err();
    // Every attempt was rejected, so the failure is not an uncertain outcome.
    assert!(format!("{error:#}").contains("throttled after retries"));
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    assert_eq!(objects.final_admission_requests.len(), 3);
    assert!(objects
        .final_admission_requests
        .windows(2)
        .all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn primary_control_recovery_preserves_a_competing_attempt_fence() {
    let f = Fixture::new().await;
    {
        let mut objects = f.objects.lock().unwrap();
        objects.lose_admission_ack_once = true;
        objects.change_attempt_on_ack_loss = true;
    }
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    {
        let objects = f.objects.lock().unwrap();
        assert!(objects.writes.is_empty());
        // The losing actor leaves the competitor's record exactly as it was.
        let (generation, bytes) = admission_record(&objects);
        assert_eq!(*generation, 2);
        let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(value["attempt"], "another-fixture-attempt");
        assert_eq!(value["b2"]["state"], "not_issued");
        assert_eq!(value["gcs"]["state"], "not_issued");
    }
    // A later retry takes a fresh fence over the unissued slots and finishes.
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    let objects = f.objects.lock().unwrap();
    assert_eq!(objects.writes, ["b2", "gcs"]);
    let (_, bytes) = admission_record(&objects);
    let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_ne!(value["attempt"], "another-fixture-attempt");
}

#[tokio::test]
async fn primary_unreadable_control_ack_stays_pending_then_resumes_unissued() {
    let f = Fixture::new().await;
    {
        let mut objects = f.objects.lock().unwrap();
        objects.lose_admission_ack_once = true;
        objects.hide_lost_admission_ack = true;
    }
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    assert!(f.objects.lock().unwrap().writes.is_empty());
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn primary_acknowledged_backup_can_resume_without_new_provider_writes() {
    for corrupt_backup in [false, true] {
        let f = Fixture::new().await;
        f.objects.lock().unwrap().fail_final_admission_once = Some(503);
        assert!(f
            .storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .is_err());
        f.objects.lock().unwrap().corrupt_gcs = corrupt_backup;
        let resumed = f
            .storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await;
        assert_eq!(resumed.is_ok(), !corrupt_backup);
        if let Ok(result) = resumed {
            // The failed attempt never returned, so hooks gated on this flag
            // have not run for the upload yet.
            assert!(result.newly_stored);
        }
        assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
    }
}

#[tokio::test]
async fn primary_acknowledged_backup_resume_fences_the_stale_attempt() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().throttle_final_admission = true;
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    let stale_final = {
        let mut objects = f.objects.lock().unwrap();
        objects.throttle_final_admission = false;
        objects.final_admission_requests[0].1.clone()
    };
    f.storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    let objects = f.objects.lock().unwrap();
    let (_, committed) = admission_record(&objects);
    // A still-running original actor reads back the record after its own final
    // write fails its precondition. It must not find its exact proposal there.
    assert_ne!(committed, &stale_final);
    assert_eq!(objects.writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn primary_acknowledged_backup_retry_yields_to_a_landed_original() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().throttle_final_admission = true;
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    {
        let mut objects = f.objects.lock().unwrap();
        objects.throttle_final_admission = false;
        objects.land_stale_final_before_fence = true;
    }
    // The original's final receipt lands between the retry's read and its fence
    // write, so the retry must stand down instead of completing a second time.
    let error = f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("changed during update"));
    let replay = f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    assert!(!replay.newly_stored);
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}
#[tokio::test]
async fn b2_primary_review_direct_none_outbox_preserves_existing_dual_intent_barrier() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    *f.storage.database_url.lock().await = Some(url.clone());
    let hash = hex::encode(Sha256::digest(b"original"));
    let owner = "e".repeat(64);
    old.begin_direct_upload(
        &hash,
        8,
        "video/mp4",
        &owner,
        &f.storage.with_mode(WriteMode::Dual).unwrap(),
    )
    .await
    .unwrap();
    let result = crate::stream_to_gcs_with_hash(
        &f.storage,
        None,
        "application/octet-stream",
        axum::body::Body::from("original"),
        &owner,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        result.storage.mode,
        WriteMode::Dual,
        "actual upload path must resolve old pending intent despite global outboxNone"
    );
    assert!(
        result.thumbnail_finalization.is_some(),
        "old required thumbnail barrier stays pending"
    );
    assert!(
        result.processing_outbox.is_some(),
        "legacy hooks must not replace this historical finalization path"
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["gcs", "b2"]);
    let db = crate::coordination::connect(&url).await.unwrap();
    assert!(!db
        .query_one("SELECT completed FROM upload_processing_intents", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
}
#[tokio::test]
async fn b2_primary_review_mirror_none_outbox_preserves_anonymous_b2_intent() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    *f.storage.database_url.lock().await = Some(url.clone());
    let hash = hex::encode(Sha256::digest(b"original"));
    old.begin_or_resume_upload(
        &hash,
        8,
        "application/octet-stream",
        "",
        &f.storage.with_mode(WriteMode::B2).unwrap(),
    )
    .await
    .unwrap();
    let mut state = crate::tests::test_state(None, 1);
    Arc::get_mut(&mut state).unwrap().media_storage = f.storage.clone();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"original").unwrap();
    let request = crate::MigrateRequest {
        source_url: "https://cdn.divine.video/fixture".into(),
        expected_hash: Some(hash.clone()),
        owner: Some("untrusted-owner-hint".into()),
    };
    crate::finish_migrate_storage(
        &state,
        &request,
        hash,
        8,
        "application/octet-stream".into(),
        file.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        f.objects.lock().unwrap().writes,
        ["b2"],
        "old B2-only mirror policy cannot become a new two-provider intent"
    );
    let db = crate::coordination::connect(&url).await.unwrap();
    assert!(db
        .query_one("SELECT completed FROM upload_processing_intents", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    let payload: serde_json::Value = db
        .query_one("SELECT payload FROM upload_processing_outbox", &[])
        .await
        .unwrap()
        .get(0);
    assert!(payload["owner"].is_null());
    assert_eq!(payload["source"]["provider"], "b2");
}
#[tokio::test]
async fn b2_primary_review_primary_session_keeps_legacy_processing_after_outbox_rollback() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend,
        runtime.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    );
    let owner = "e".repeat(64);
    let hash = hex::encode(Sha256::digest(b"original"));
    let init = manager
        .init_session(
            &owner,
            ResumableUploadInitRequest {
                sha256: hash.clone(),
                size: 8,
                content_type: "application/octet-stream".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    manager
        .upload_chunk(
            &init.upload_id,
            Some(&init.required_headers["Authorization"]),
            "bytes 0-7/8",
            Bytes::from_static(b"original"),
        )
        .await
        .unwrap();
    let rolled = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        rolled.backend,
        rolled.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    )
    .with_outbox(Some(old));
    let result = manager
        .complete_session(&init.upload_id, &owner)
        .await
        .expect("exact B2+GCS receipts must survive rollback/outbox wiring");
    assert_eq!(result.sha256, hash);
    assert!(!result.processing_via_outbox);
    assert!(
        !manager
            .complete_session(&init.upload_id, &owner)
            .await
            .unwrap()
            .newly_stored
    );
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM upload_processing_session_receipts",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn b2_primary_review_new_hash_with_existing_database_keeps_legacy_hooks_without_new_pg_intent(
) {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    *f.storage.database_url.lock().await = Some(url.clone());
    let hash = hex::encode(Sha256::digest(b"original"));
    old.begin_or_resume_upload(
        &hash,
        8,
        "application/octet-stream",
        "prior-owner",
        &f.storage.with_mode(WriteMode::Dual).unwrap(),
    )
    .await
    .unwrap();
    let result = crate::stream_to_gcs_with_hash(
        &f.storage,
        None,
        "application/octet-stream",
        axum::body::Body::from("original"),
        "new-owner",
        None,
    )
    .await
    .unwrap();
    assert!(result.processing_outbox.is_none());
    assert_eq!(result.storage.mode, WriteMode::B2Primary);
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
async fn b2_primary_review_old_staged_session_retains_its_historical_processing_contract() {
    use crate::resumable::{ResumableBackend, ResumableRuntime, UploadSession};
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let _existing = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let session = UploadSession {
        processing_outbox_required: None,
        revision: 0,
        chunk_size: 1024,
        upload_id: "up_historical".into(),
        owner: "e".repeat(64),
        final_sha256: "a".repeat(64),
        declared_size: 8,
        content_type: "application/octet-stream".into(),
        file_name: None,
        expires_at_epoch_secs: u64::MAX,
        next_offset: 8,
        session_url: "staged-v1".into(),
        session_token: "fixture".into(),
        temp_object: format!(
            "__resumable/staged/b2/{}/{}/up_historical",
            hex::encode("fixture-gcs"),
            hex::encode("fixture-b2")
        ),
        storage_complete: true,
        finalized_object: None,
    };
    use crate::resumable::SessionStore;
    runtime.store.save(&session).await.unwrap();
    let history = runtime
        .backend
        .historical_processing(&session)
        .await
        .unwrap()
        .expect("new primary mode cannot remove an accepted legacy session's receipt contract");
    runtime
        .backend
        .begin_finalization(&session, &history)
        .await
        .unwrap();
    assert!(
        history.dispatch_once(1).await.is_err(),
        "history-only adapter cannot claim consumer delivery authority"
    );
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one("SELECT write_mode FROM upload_processing_intents", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "b2"
    );
    assert!(f.objects.lock().unwrap().writes.is_empty());
}

#[tokio::test]
async fn b2_primary_second_review_outbox_enabled_rollback_rejects_missing_primary_on_direct_and_mirror(
) {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    f.objects.lock().unwrap().fail_backup_admission_once = true;
    let hash = hex::encode(Sha256::digest(b"original"));
    let owner = "e".repeat(64);
    assert!(f
        .storage
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "application/octet-stream",
            Some(&owner)
        )
        .await
        .is_err());
    let rollback = MediaStorage::new(
        WriteMode::Gcs,
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .unwrap();
    let direct = crate::stream_to_gcs_with_hash(
        &rollback,
        Some(&outbox),
        "application/octet-stream",
        axum::body::Body::from("original"),
        &owner,
        None,
    )
    .await;
    let direct_failed = direct.is_err();
    let mut state = crate::tests::test_state(None, 1);
    let mutable = Arc::get_mut(&mut state).unwrap();
    mutable.media_storage = rollback;
    mutable.processing_outbox = Some(outbox);
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"original").unwrap();
    let request = crate::MigrateRequest {
        source_url: "https://cdn.divine.video/fixture".into(),
        expected_hash: Some(hash.clone()),
        owner: None,
    };
    let mirror_failed = crate::finish_migrate_storage(
        &state,
        &request,
        hash,
        8,
        "application/octet-stream".into(),
        file.path(),
    )
    .await
    .is_err();
    assert!(
        direct_failed && mirror_failed,
        "direct_failed={direct_failed}, mirror_failed={mirror_failed}"
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}
#[tokio::test]
async fn b2_primary_second_review_thumbnail_outbox_cannot_drop_primary_record_policy() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let hash = hex::encode(Sha256::digest(b"original"));
    f.storage
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "application/octet-stream",
            None,
        )
        .await
        .unwrap();
    let mut state = crate::tests::test_state(None, 1);
    let mutable = Arc::get_mut(&mut state).unwrap();
    mutable.media_storage = f.storage.with_mode(WriteMode::Gcs).unwrap();
    mutable.processing_outbox = Some(outbox);
    assert_eq!(
        crate::thumbnail_storage(&state, &hash).await.unwrap().mode,
        WriteMode::B2Primary,
        "outbox history absence is not authority to downgrade a pinned primary"
    );
}

#[tokio::test]
async fn b2_primary_second_review_native_gcs_session_keeps_existing_processing_contract() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    for mode in [WriteMode::Gcs, WriteMode::B2Primary] {
        for global in [false, true] {
            let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                return;
            };
            let old = crate::outbox::ProcessingOutbox::connect(
                &url,
                "https://processor.invalid/finalized",
                "fixture-token",
            )
            .await
            .unwrap();
            let f = Fixture::new().await;
            let owner = "e".repeat(64);
            let hash = hex::encode(Sha256::digest(b"original"));
            let gcs = f.storage.with_mode(WriteMode::Gcs).unwrap();
            let runtime = ResumableRuntime::new(
                gcs.clone(),
                f.storage.gcs.clone(),
                "fixture-gcs".into(),
                Some(&url),
            )
            .await
            .unwrap();
            let manager = ResumableManager::new(
                runtime.backend,
                runtime.store,
                "https://upload.invalid",
                "https://media.invalid",
                256 * 1024,
                300,
            )
            .with_outbox(Some(old.clone()));
            let init = manager
                .init_session(
                    &owner,
                    ResumableUploadInitRequest {
                        sha256: hash.clone(),
                        size: 8,
                        content_type: "application/octet-stream".into(),
                        file_name: None,
                    },
                )
                .await
                .unwrap();
            manager
                .upload_chunk(
                    &init.upload_id,
                    Some(&init.required_headers["Authorization"]),
                    "bytes 0-7/8",
                    Bytes::from_static(b"original"),
                )
                .await
                .unwrap();
            old.begin_upload(&hash, 8, "application/octet-stream", &owner, &gcs)
                .await
                .unwrap();
            let runtime = ResumableRuntime::new(
                f.storage.with_mode(mode).unwrap(),
                f.storage.gcs.clone(),
                "fixture-gcs".into(),
                Some(&url),
            )
            .await
            .unwrap();
            let migrated = ResumableManager::new(
                runtime.backend.clone(),
                runtime.store.clone(),
                "https://upload.invalid",
                "https://media.invalid",
                256 * 1024,
                300,
            );
            let migrated = migrated.with_outbox(global.then_some(old));
            let result = migrated
                .complete_session(&init.upload_id, &owner)
                .await
                .unwrap();
            assert!(
                result.processing_via_outbox,
                "native GCS session's accepted PG contract must remain scoped to completion"
            );
            let db = crate::coordination::connect(&url).await.unwrap();
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM upload_processing_session_receipts",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                1
            );
            assert!(db
                .query_one("SELECT completed FROM upload_processing_intents", &[])
                .await
                .unwrap()
                .get::<_, bool>(0));
            assert_eq!(f.objects.lock().unwrap().writes, ["gcs"]);
            db.execute("UPDATE upload_sessions SET state=jsonb_set(state,'{expires_at_epoch_secs}','0'::jsonb)",&[]).await.unwrap();
            assert_eq!(runtime.sweep_expired(1, None).await.unwrap(), 1);
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM upload_processing_session_receipts",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                0
            );
        }
    }
}

#[tokio::test]
async fn b2_primary_policy_precedence_matrix_covers_direct_mirror_and_thumbnail() {
    let mut scenarios = 0;
    for mode in [WriteMode::B2Primary, WriteMode::Gcs] {
        for global_outbox in [false, true] {
            for binding in [false, true] {
                for route in ["direct", "mirror", "thumbnail"] {
                    let f = Fixture::new().await;
                    let configured = MediaStorage::new(
                        mode,
                        f.storage.gcs.clone(),
                        "fixture-gcs".into(),
                        binding.then(|| f.storage.b2.clone().unwrap()),
                    );
                    scenarios += 1;
                    if mode == WriteMode::B2Primary && !binding {
                        assert!(configured.is_err());
                        continue;
                    }
                    let configured = configured.unwrap();
                    let outbox = if global_outbox {
                        let Some((value, _)) = database().await else {
                            return;
                        };
                        Some(value)
                    } else {
                        None
                    };
                    let owner = "e".repeat(64);
                    let hash = hex::encode(Sha256::digest(b"original"));
                    f.objects.lock().unwrap().fail_backup_admission_once = true;
                    assert!(f
                        .storage
                        .put(
                            &hash,
                            Bytes::from_static(b"original"),
                            "application/octet-stream",
                            Some(&owner)
                        )
                        .await
                        .is_err());
                    match route {
                        "direct" => {
                            let result = crate::stream_to_gcs_with_hash(
                                &configured,
                                outbox.as_ref(),
                                "application/octet-stream",
                                axum::body::Body::from("original"),
                                &owner,
                                None,
                            )
                            .await;
                            if binding {
                                let result = result.unwrap();
                                assert_eq!(result.storage.mode, WriteMode::B2Primary);
                                assert_eq!(result.processing_outbox.is_some(), global_outbox);
                            } else {
                                assert!(result.is_err());
                            }
                        }
                        "mirror" => {
                            let mut state = crate::tests::test_state(None, 1);
                            let mutable = Arc::get_mut(&mut state).unwrap();
                            mutable.media_storage = configured;
                            mutable.processing_outbox = outbox;
                            let file = tempfile::NamedTempFile::new().unwrap();
                            std::fs::write(file.path(), b"original").unwrap();
                            let request = crate::MigrateRequest {
                                source_url: "https://cdn.divine.video/fixture".into(),
                                expected_hash: Some(hash.clone()),
                                owner: None,
                            };
                            let result = crate::finish_migrate_storage(
                                &state,
                                &request,
                                hash,
                                8,
                                "application/octet-stream".into(),
                                file.path(),
                            )
                            .await;
                            assert_eq!(result.is_ok(), binding);
                        }
                        "thumbnail" => {
                            let mut state = crate::tests::test_state(None, 1);
                            let mutable = Arc::get_mut(&mut state).unwrap();
                            mutable.media_storage = configured;
                            mutable.processing_outbox = outbox;
                            let selected = crate::thumbnail_storage(&state, &hash).await;
                            if binding {
                                assert_eq!(selected.unwrap().mode, WriteMode::B2Primary);
                            } else {
                                assert!(selected.is_err());
                            }
                        }
                        _ => unreachable!(),
                    }
                    let expected = if binding && route != "thumbnail" {
                        vec!["b2", "gcs"]
                    } else {
                        vec!["b2"]
                    };
                    assert_eq!(
                        f.objects.lock().unwrap().writes,
                        expected,
                        "{mode:?}/{global_outbox}/{binding}/{route}"
                    );
                }
            }
        }
    }
    assert_eq!(scenarios, 24);
}

#[tokio::test]
async fn b2_primary_policy_conflict_never_rewrites_an_accepted_pg_mode() {
    let Some((outbox, db)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let owner = "e".repeat(64);
    let hash = hex::encode(Sha256::digest(b"original"));
    outbox
        .begin_upload(
            &hash,
            8,
            "application/octet-stream",
            &owner,
            &f.storage.with_mode(WriteMode::Gcs).unwrap(),
        )
        .await
        .unwrap();
    f.objects.lock().unwrap().fail_backup_admission_once = true;
    assert!(f
        .storage
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "application/octet-stream",
            Some(&owner)
        )
        .await
        .is_err());
    let rollback = f.storage.with_mode(WriteMode::Gcs).unwrap();
    assert!(crate::stream_to_gcs_with_hash(
        &rollback,
        Some(&outbox),
        "application/octet-stream",
        axum::body::Body::from("original"),
        &owner,
        None
    )
    .await
    .is_err());
    let row = db
        .query_one(
            "SELECT write_mode,completed FROM upload_processing_intents",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "gcs");
    assert!(!row.get::<_, bool>(1));
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
}

#[tokio::test]
async fn b2_primary_policy_precedence_matrix_covers_resumable_completion_and_replay() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    let mut scenarios = 0;
    for mode in [WriteMode::B2Primary, WriteMode::Gcs] {
        for global in [false, true] {
            for binding in [false, true] {
                scenarios += 1;
                let f = Fixture::new().await;
                let configured = MediaStorage::new(
                    mode,
                    f.storage.gcs.clone(),
                    "fixture-gcs".into(),
                    binding.then(|| f.storage.b2.clone().unwrap()),
                );
                if mode == WriteMode::B2Primary && !binding {
                    assert!(configured.is_err());
                    continue;
                }
                let configured = configured.unwrap();
                let history = if global {
                    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                        return;
                    };
                    let outbox = crate::outbox::ProcessingOutbox::connect(
                        &url,
                        "https://processor.invalid/finalized",
                        "fixture-token",
                    )
                    .await
                    .unwrap();
                    Some((url, outbox))
                } else {
                    None
                };
                let database = history.as_ref().map(|(url, _)| url.as_str());
                let runtime = ResumableRuntime::new(
                    f.storage.clone(),
                    f.storage.gcs.clone(),
                    "fixture-gcs".into(),
                    database,
                )
                .await
                .unwrap();
                let initial = ResumableManager::new(
                    runtime.backend,
                    runtime.store,
                    "https://upload.invalid",
                    "https://media.invalid",
                    256 * 1024,
                    300,
                );
                let owner = "e".repeat(64);
                let hash = hex::encode(Sha256::digest(b"original"));
                let init = initial
                    .init_session(
                        &owner,
                        ResumableUploadInitRequest {
                            sha256: hash.clone(),
                            size: 8,
                            content_type: "application/octet-stream".into(),
                            file_name: None,
                        },
                    )
                    .await
                    .unwrap();
                initial
                    .upload_chunk(
                        &init.upload_id,
                        Some(&init.required_headers["Authorization"]),
                        "bytes 0-7/8",
                        Bytes::from_static(b"original"),
                    )
                    .await
                    .unwrap();
                let runtime = ResumableRuntime::new(
                    configured,
                    f.storage.gcs.clone(),
                    "fixture-gcs".into(),
                    database,
                )
                .await
                .unwrap();
                let manager = ResumableManager::new(
                    runtime.backend,
                    runtime.store,
                    "https://upload.invalid",
                    "https://media.invalid",
                    256 * 1024,
                    300,
                )
                .with_outbox(history.as_ref().map(|(_, outbox)| outbox.clone()));
                let result = manager.complete_session(&init.upload_id, &owner).await;
                if binding {
                    assert_eq!(result.unwrap().sha256, hash);
                    assert!(
                        !manager
                            .complete_session(&init.upload_id, &owner)
                            .await
                            .unwrap()
                            .newly_stored
                    );
                    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
                } else {
                    assert!(result.is_err());
                    assert!(f.objects.lock().unwrap().writes.is_empty());
                }
            }
        }
    }
    assert_eq!(scenarios, 8);
}

#[tokio::test]
async fn b2_primary_historical_session_waits_for_primary_settlement_then_reuses_exact_copies() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    for mode in [WriteMode::Gcs, WriteMode::B2Primary] {
        for global in [false, true] {
            let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                return;
            };
            let old = crate::outbox::ProcessingOutbox::connect(
                &url,
                "https://processor.invalid/finalized",
                "fixture-token",
            )
            .await
            .unwrap();
            let f = Fixture::new().await;
            let owner = "e".repeat(64);
            let new_owner = "d".repeat(64);
            let hash = hex::encode(Sha256::digest(b"original"));
            let gcs = f.storage.with_mode(WriteMode::Gcs).unwrap();
            let runtime = ResumableRuntime::new(
                gcs.clone(),
                f.storage.gcs.clone(),
                "fixture-gcs".into(),
                Some(&url),
            )
            .await
            .unwrap();
            let initial = ResumableManager::new(
                runtime.backend,
                runtime.store,
                "https://upload.invalid",
                "https://media.invalid",
                256 * 1024,
                300,
            )
            .with_outbox(Some(old.clone()));
            let init = initial
                .init_session(
                    &owner,
                    ResumableUploadInitRequest {
                        sha256: hash.clone(),
                        size: 8,
                        content_type: "application/octet-stream".into(),
                        file_name: None,
                    },
                )
                .await
                .unwrap();
            initial
                .upload_chunk(
                    &init.upload_id,
                    Some(&init.required_headers["Authorization"]),
                    "bytes 0-7/8",
                    Bytes::from_static(b"original"),
                )
                .await
                .unwrap();
            old.begin_upload(&hash, 8, "application/octet-stream", &owner, &gcs)
                .await
                .unwrap();
            f.objects.lock().unwrap().fail_backup_admission_once = true;
            assert!(f
                .storage
                .put(
                    &hash,
                    Bytes::from_static(b"original"),
                    "application/octet-stream",
                    Some(&new_owner)
                )
                .await
                .is_err());
            let runtime = ResumableRuntime::new(
                f.storage.with_mode(mode).unwrap(),
                f.storage.gcs.clone(),
                "fixture-gcs".into(),
                Some(&url),
            )
            .await
            .unwrap();
            let manager = ResumableManager::new(
                runtime.backend,
                runtime.store,
                "https://upload.invalid",
                "https://media.invalid",
                256 * 1024,
                300,
            )
            .with_outbox(global.then_some(old));
            assert!(
                manager
                    .complete_session(&init.upload_id, &owner)
                    .await
                    .is_err(),
                "old GCS writes cannot bypass a partial new primary admission"
            );
            assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
            f.storage
                .put(
                    &hash,
                    Bytes::from_static(b"original"),
                    "application/octet-stream",
                    Some(&new_owner),
                )
                .await
                .unwrap();
            let result = manager
                .complete_session(&init.upload_id, &owner)
                .await
                .unwrap();
            assert!(result.processing_via_outbox);
            assert!(!result.newly_stored);
            assert_eq!(
                f.objects.lock().unwrap().writes,
                ["b2", "gcs"],
                "old session must reuse both pinned copies without a fresh PUT"
            );
            let db = crate::coordination::connect(&url).await.unwrap();
            let event: serde_json::Value = db
                .query_one("SELECT payload FROM upload_processing_outbox", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(event["source"]["provider"], "gcs");
            assert_eq!(event["source"]["version"], "1");
        }
    }
}

#[tokio::test]
async fn b2_primary_fourth_review_pg_only_thumbnail_history_matrix() {
    let mut scenarios = 0;
    for accepted in [WriteMode::Gcs, WriteMode::B2, WriteMode::Dual] {
        for current in [WriteMode::Gcs, WriteMode::B2Primary] {
            for global in [false, true] {
                for binding in [false, true] {
                    scenarios += 1;
                    let f = Fixture::new().await;
                    let configured = MediaStorage::new(
                        current,
                        f.storage.gcs.clone(),
                        "fixture-gcs".into(),
                        binding.then(|| f.storage.b2.clone().unwrap()),
                    );
                    if current == WriteMode::B2Primary && !binding {
                        assert!(configured.is_err());
                        continue;
                    }
                    let configured = configured.unwrap();
                    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
                        return;
                    };
                    let outbox = crate::outbox::ProcessingOutbox::connect(
                        &url,
                        "https://processor.invalid/finalized",
                        "fixture-token",
                    )
                    .await
                    .unwrap();
                    let owner = "e".repeat(64);
                    let hash = hex::encode(Sha256::digest(b"original"));
                    let old = f.storage.with_mode(accepted).unwrap();
                    outbox
                        .begin_upload(&hash, 8, "application/octet-stream", &owner, &old)
                        .await
                        .unwrap();
                    let result = old
                        .put(
                            &hash,
                            Bytes::from_static(b"original"),
                            "application/octet-stream",
                            Some(&owner),
                        )
                        .await
                        .unwrap();
                    outbox
                        .enqueue_upload(
                            &hash,
                            8,
                            "application/octet-stream",
                            &owner,
                            result.objects,
                        )
                        .await
                        .unwrap();
                    assert!(f.objects.lock().unwrap().records.is_empty());
                    *configured.database_url.lock().await = Some(url);
                    let mut state = crate::tests::test_state(None, 1);
                    let mutable = Arc::get_mut(&mut state).unwrap();
                    mutable.media_storage = configured;
                    mutable.processing_outbox = global.then_some(outbox);
                    let before = f.objects.lock().unwrap().writes.clone();
                    let selected = crate::thumbnail_storage(&state, &hash).await;
                    if binding || accepted == WriteMode::Gcs {
                        assert_eq!(
                            selected.unwrap().mode,
                            accepted,
                            "PG-only accepted policy must survive {current:?}/{global}/{binding}"
                        );
                    } else {
                        assert!(selected.is_err());
                    }
                    assert_eq!(f.objects.lock().unwrap().writes, before);
                }
            }
        }
    }
    assert_eq!(scenarios, 24);
}

#[tokio::test]
async fn b2_primary_fourth_review_recovery_never_adopts_unsettled_effects() {
    for intent_mode in [
        WriteMode::B2Primary,
        WriteMode::Gcs,
        WriteMode::Dual,
        WriteMode::B2,
    ] {
        for lost in ["gcs_ack", "b2_ack", "gcs_verified"] {
            let Some((outbox, db)) = database().await else {
                return;
            };
            let f = Fixture::new().await;
            let owner = "e".repeat(64);
            let hash = hex::encode(Sha256::digest(b"original"));
            outbox
                .begin_upload(
                    &hash,
                    8,
                    "application/octet-stream",
                    &owner,
                    &f.storage.with_mode(intent_mode).unwrap(),
                )
                .await
                .unwrap();
            {
                let mut objects = f.objects.lock().unwrap();
                if lost == "b2_ack" {
                    objects.gcs = Some(Copy {
                        bytes: Bytes::from_static(b"original"),
                        owner: Some(owner.clone()),
                        mime: "application/octet-stream".into(),
                    });
                    objects.lose_b2_put_ack = true;
                }
                if lost == "gcs_ack" {
                    objects.lose_gcs_put_ack = true;
                }
                if lost == "gcs_verified" {
                    objects.corrupt_gcs = true;
                }
            }
            assert!(f
                .storage
                .put(
                    &hash,
                    Bytes::from_static(b"original"),
                    "application/octet-stream",
                    Some(&owner)
                )
                .await
                .is_err());
            {
                let mut objects = f.objects.lock().unwrap();
                objects.corrupt_gcs = false;
                objects.read_versions.clear();
                assert!(objects.b2.is_some() && objects.gcs.is_some());
            }
            assert_eq!(
                outbox
                    .recover_intents(&f.storage.with_mode(WriteMode::Gcs).unwrap(), 1, 64)
                    .await
                    .unwrap(),
                0,
                "{intent_mode:?}/{lost} must remain pending despite matching current bytes"
            );
            assert_eq!(
                db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                0
            );
            assert!(!db
                .query_one("SELECT completed FROM upload_processing_intents", &[])
                .await
                .unwrap()
                .get::<_, bool>(0));
            assert!(
                f.objects.lock().unwrap().read_versions.is_empty(),
                "unknown admission cannot trigger media adoption reads"
            );
        }
    }
}

#[tokio::test]
async fn b2_primary_fourth_review_recovery_uses_only_recorded_versions() {
    for intent_mode in [
        WriteMode::B2Primary,
        WriteMode::Gcs,
        WriteMode::Dual,
        WriteMode::B2,
    ] {
        let Some((outbox, db)) = database().await else {
            return;
        };
        let f = Fixture::new().await;
        let owner = "e".repeat(64);
        let hash = hex::encode(Sha256::digest(b"original"));
        outbox
            .begin_upload(
                &hash,
                8,
                "application/octet-stream",
                &owner,
                &f.storage.with_mode(intent_mode).unwrap(),
            )
            .await
            .unwrap();
        let written = f
            .storage
            .put(
                &hash,
                Bytes::from_static(b"original"),
                "application/octet-stream",
                Some(&owner),
            )
            .await
            .unwrap();
        {
            let mut objects = f.objects.lock().unwrap();
            objects.latest_b2_version = Some("b2-version-2".into());
            objects.read_versions.clear();
        }
        assert_eq!(
            outbox
                .recover_intents(&f.storage.with_mode(WriteMode::Gcs).unwrap(), 1, 64)
                .await
                .unwrap(),
            1
        );
        let event: serde_json::Value = db
            .query_one("SELECT payload FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get(0);
        let source = if matches!(intent_mode, WriteMode::Gcs | WriteMode::Dual) {
            &written.objects[1]
        } else {
            &written.objects[0]
        };
        assert_eq!(event["source"], serde_json::to_value(source).unwrap());
        assert_eq!(
            f.objects.lock().unwrap().read_versions,
            ["b2-version-1"],
            "latest v2 is not the admitted receipt"
        );
        assert_eq!(
            db.query_one("SELECT write_mode FROM upload_processing_intents", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            serde_json::to_value(intent_mode).unwrap().as_str().unwrap()
        );
    }
}

#[tokio::test]
async fn b2_primary_fourth_review_missing_recorded_backup_does_not_redownload_primary() {
    let Some((outbox, _)) = database().await else {
        return;
    };
    let f = Fixture::new().await;
    let owner = "e".repeat(64);
    let hash = hex::encode(Sha256::digest(b"original"));
    outbox
        .begin_upload(&hash, 8, "application/octet-stream", &owner, &f.storage)
        .await
        .unwrap();
    f.storage
        .put(
            &hash,
            Bytes::from_static(b"original"),
            "application/octet-stream",
            Some(&owner),
        )
        .await
        .unwrap();
    {
        let mut objects = f.objects.lock().unwrap();
        objects.gcs = None;
        objects.read_versions.clear();
    }
    assert_eq!(outbox.recover_intents(&f.storage, 1, 64).await.unwrap(), 0);
    assert!(
        f.objects.lock().unwrap().read_versions.is_empty(),
        "missing backup must be detected before downloading the surviving original"
    );
}

#[tokio::test]
async fn b2_primary_fifth_review_merged_legacy_history_is_not_an_explicit_primary_contract() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let hash = hex::encode(Sha256::digest(b"original"));
    let object = |provider, bucket: &str, version: &str| ObjectRef {
        provider,
        bucket: bucket.into(),
        key: hash.clone(),
        version: Some(version.into()),
    };
    for (owner, mode, refs) in [
        (
            1,
            WriteMode::Dual,
            vec![
                object(StorageProvider::Gcs, "fixture-gcs", "1"),
                object(StorageProvider::B2, "fixture-b2", "b2-v1"),
            ],
        ),
        (
            2,
            WriteMode::Gcs,
            vec![object(StorageProvider::Gcs, "fixture-gcs", "2")],
        ),
        (
            3,
            WriteMode::B2,
            vec![object(StorageProvider::B2, "fixture-b2", "b2-v2")],
        ),
    ] {
        let owner = format!("{owner:064x}");
        outbox
            .begin_upload(
                &hash,
                8,
                "application/octet-stream",
                &owner,
                &f.storage.with_mode(mode).unwrap(),
            )
            .await
            .unwrap();
        outbox
            .enqueue_upload(&hash, 8, "application/octet-stream", &owner, refs)
            .await
            .unwrap();
    }
    assert_eq!(
        outbox.source_objects(&hash).await.unwrap()[0].provider,
        StorageProvider::B2
    );
    assert!(f.objects.lock().unwrap().records.is_empty());
    for mode in [WriteMode::Gcs, WriteMode::B2Primary] {
        for global in [false, true] {
            let configured = f.storage.with_mode(mode).unwrap();
            *configured.database_url.lock().await = Some(url.clone());
            let mut state = crate::tests::test_state(None, 1);
            let mutable = Arc::get_mut(&mut state).unwrap();
            mutable.media_storage = configured;
            mutable.processing_outbox = global.then(|| outbox.clone());
            assert_eq!(
                crate::thumbnail_storage(&state, &hash).await.unwrap().mode,
                WriteMode::Dual,
                "merged legacy history must preserve GCS-first ordering under {mode:?}/{global}"
            );
        }
    }
    assert!(f.objects.lock().unwrap().writes.is_empty());
}

#[tokio::test]
async fn b2_primary_fifth_review_explicit_single_event_keeps_b2_authoritative_order() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let outbox = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let hash = hex::encode(Sha256::digest(b"original"));
    let owner = "e".repeat(64);
    let refs = vec![
        ObjectRef {
            provider: StorageProvider::B2,
            bucket: "fixture-b2".into(),
            key: hash.clone(),
            version: Some("b2-v1".into()),
        },
        ObjectRef {
            provider: StorageProvider::Gcs,
            bucket: "fixture-gcs".into(),
            key: hash.clone(),
            version: Some("1".into()),
        },
    ];
    outbox
        .begin_upload(&hash, 8, "application/octet-stream", &owner, &f.storage)
        .await
        .unwrap();
    outbox
        .enqueue_upload(&hash, 8, "application/octet-stream", &owner, refs)
        .await
        .unwrap();
    for global in [false, true] {
        let configured = f.storage.with_mode(WriteMode::Gcs).unwrap();
        *configured.database_url.lock().await = Some(url.clone());
        let mut state = crate::tests::test_state(None, 1);
        let mutable = Arc::get_mut(&mut state).unwrap();
        mutable.media_storage = configured;
        mutable.processing_outbox = global.then(|| outbox.clone());
        assert_eq!(
            crate::thumbnail_storage(&state, &hash).await.unwrap().mode,
            WriteMode::B2Primary
        );
    }
}

async fn native_first_completion_migration(
    remove_birth_proof: bool,
    enabled: bool,
    configured_after: bool,
) {
    use crate::resumable::{
        ResumableError, ResumableManager, ResumableRuntime, ResumableUploadInitRequest,
    };
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let owner = "e".repeat(64);
    let hash = hex::encode(Sha256::digest(b"original"));
    let runtime = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let original = ResumableManager::new(
        runtime.backend,
        runtime.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    )
    .with_outbox(enabled.then_some(old.clone()));
    let init = original
        .init_session(
            &owner,
            ResumableUploadInitRequest {
                sha256: hash,
                size: 8,
                content_type: "application/octet-stream".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    original
        .upload_chunk(
            &init.upload_id,
            Some(&init.required_headers["Authorization"]),
            "bytes 0-7/8",
            Bytes::from_static(b"original"),
        )
        .await
        .unwrap();
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    if remove_birth_proof {
        db.execute(
            "UPDATE upload_sessions SET state=state-'processing_outbox_required'",
            &[],
        )
        .await
        .unwrap();
    }
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let migrated = ResumableManager::new(
        runtime.backend,
        runtime.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    )
    .with_outbox(configured_after.then_some(old));
    let result = migrated.complete_session(&init.upload_id, &owner).await;
    if remove_birth_proof {
        assert!(matches!(result,Err(ResumableError::Unavailable(_))),"ambiguous pre-field PG session must fail before promotion, not silently lose a processing contract");
        assert!(f.objects.lock().unwrap().writes.is_empty());
        // The existing original configuration can finish the preserved session.
        assert!(
            original
                .complete_session(&init.upload_id, &owner)
                .await
                .unwrap()
                .processing_via_outbox
        );
    } else {
        assert_eq!(result.unwrap().processing_via_outbox, enabled, "init must retain its accepted processing disposition before any completion intent exists");
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM upload_processing_session_receipts",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        i64::from(enabled)
    );
}
#[tokio::test]
async fn b2_primary_next_review_native_session_birth_processing_survives_before_first_complete() {
    native_first_completion_migration(false, true, false).await;
}
#[tokio::test]
async fn b2_primary_next_review_unmarked_native_pg_session_does_not_guess_processing() {
    native_first_completion_migration(true, true, false).await;
}

#[tokio::test]
async fn b2_primary_next_review_native_legacy_birth_not_retargeted_to_later_outbox() {
    native_first_completion_migration(false, false, true).await;
}

#[tokio::test]
async fn b2_primary_next_review_retained_pg_tracker_is_not_a_new_multipart_dependency() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let f = Fixture::new().await;
    f.storage
        .with_mode(WriteMode::B2)
        .unwrap()
        .initialize_coordination(Some(&url))
        .await
        .unwrap();
    let db = crate::coordination::connect(&url).await.unwrap();
    db.batch_execute("CREATE FUNCTION reject_tracker_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture forbids modern PG tracker writes'; END $$; CREATE TRIGGER reject_tracker BEFORE INSERT OR UPDATE OR DELETE ON b2_multipart_intents FOR EACH ROW EXECUTE FUNCTION reject_tracker_write();").await.unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), vec![42; 9 * 1024 * 1024]).unwrap();
    f.storage
        .put_file(
            "media",
            file.path(),
            "application/octet-stream",
            Some(&"e".repeat(64)),
        )
        .await
        .expect("new primary multipart must use only its GCS admission tracker");
    assert_eq!(
        db.query_one("SELECT count(*) FROM b2_multipart_intents", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn b2_primary_next_review_foreground_expiry_forgets_historical_receipt_before_session() {
    use crate::resumable::{ResumableManager, ResumableRuntime, SessionStore, UploadSession};
    for (action, fault) in [
        ("head", "none"),
        ("complete", "none"),
        ("head", "lookup"),
        ("complete", "lookup"),
        ("head", "forget"),
        ("complete", "forget"),
    ] {
        let Some(url) = crate::outbox::tests::isolated_database_url().await else {
            return;
        };
        let outbox = crate::outbox::ProcessingOutbox::connect(
            &url,
            "https://processor.invalid/finalized",
            "fixture-token",
        )
        .await
        .unwrap();
        let f = Fixture::new().await;
        let owner = "e".repeat(64);
        let hash = hex::encode(Sha256::digest(b"original"));
        let id = "up_expired_fixture";
        let gcs = f.storage.with_mode(WriteMode::Gcs).unwrap();
        let runtime = ResumableRuntime::new(
            gcs.clone(),
            f.storage.gcs.clone(),
            "fixture-gcs".into(),
            Some(&url),
        )
        .await
        .unwrap();
        let session = UploadSession {
            processing_outbox_required: None,
            revision: 0,
            chunk_size: 1024,
            upload_id: id.into(),
            owner: owner.clone(),
            final_sha256: hash.clone(),
            declared_size: 8,
            content_type: "application/octet-stream".into(),
            file_name: None,
            expires_at_epoch_secs: 0,
            next_offset: 8,
            session_url: "https://fixture.invalid/completed".into(),
            session_token: "fixture-token".into(),
            temp_object: "__resumable/uploads/up_expired_fixture/blob".into(),
            storage_complete: true,
            finalized_object: Some(hash.clone()),
        };
        runtime.store.save(&session).await.unwrap();
        outbox
            .begin_upload(&hash, 8, "application/octet-stream", &owner, &gcs)
            .await
            .unwrap();
        outbox
            .enqueue_completed_session(
                id,
                &hash,
                8,
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
        let runtime = ResumableRuntime::new(
            f.storage.clone(),
            f.storage.gcs.clone(),
            "fixture-gcs".into(),
            Some(&url),
        )
        .await
        .unwrap();
        let manager = ResumableManager::new(
            runtime.backend,
            runtime.store,
            "https://upload.invalid",
            "https://media.invalid",
            1024,
            300,
        );
        let db = crate::coordination::connect(&url).await.unwrap();
        if fault == "lookup" {
            db.batch_execute(
                "ALTER TABLE upload_processing_session_receipts RENAME TO retained_receipts",
            )
            .await
            .unwrap();
        } else if fault == "forget" {
            db.batch_execute("CREATE FUNCTION reject_forget() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture receipt failure'; END $$; CREATE TRIGGER reject_forget BEFORE DELETE ON upload_processing_session_receipts FOR EACH ROW EXECUTE FUNCTION reject_forget();").await.unwrap();
        }
        if action == "head" {
            assert!(manager
                .head_session(id, Some("Bearer fixture-token"))
                .await
                .is_err());
        } else {
            assert!(manager.complete_session(id, &owner).await.is_err());
        }
        if fault != "none" {
            assert_eq!(
                db.query_one("SELECT count(*) FROM upload_sessions", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                1,
                "failed cleanup must retain discovery row"
            );
            if fault == "lookup" {
                db.batch_execute(
                    "ALTER TABLE retained_receipts RENAME TO upload_processing_session_receipts",
                )
                .await
                .unwrap();
            } else {
                db.batch_execute(
                    "DROP TRIGGER reject_forget ON upload_processing_session_receipts",
                )
                .await
                .unwrap();
            }
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM upload_processing_session_receipts",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                1
            );
            assert!(manager
                .head_session(id, Some("Bearer fixture-token"))
                .await
                .is_err());
        }
        let db = crate::coordination::connect(&url).await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM upload_processing_session_receipts",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0,
            "{action} must remove receipt before deleting the discovery row"
        );
        assert_eq!(
            db.query_one("SELECT count(*) FROM upload_sessions", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        assert_eq!(
            db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert!(f.objects.lock().unwrap().writes.is_empty());
    }
}

#[tokio::test]
async fn b2_primary_next_review_retained_database_does_not_create_new_pg_tracker() {
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let f = Fixture::new().await;
    f.storage.initialize_coordination(Some(&url)).await.unwrap();
    let db = crate::coordination::connect(&url).await.unwrap();
    assert!(!db
        .query_one(
            "SELECT to_regclass('b2_multipart_intents') IS NOT NULL",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
    // A positively selected historical B2 file path can still initialize its
    // original tracker, independently of new primary admissions.
    f.storage
        .with_mode(WriteMode::B2)
        .unwrap()
        .put_file(
            "media",
            std::path::Path::new("Cargo.toml"),
            "application/octet-stream",
            None,
        )
        .await
        .unwrap();
    assert!(db
        .query_one(
            "SELECT to_regclass('b2_multipart_intents') IS NOT NULL",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
}

#[tokio::test]
async fn b2_primary_complete_handler_keeps_legacy_scan_across_outbox_rollback() {
    use crate::resumable::{ResumableManager, ResumableRuntime, ResumableUploadInitRequest};
    use k256::schnorr::{signature::hazmat::PrehashSigner, SigningKey};
    use tower::ServiceExt;
    let Some(url) = crate::outbox::tests::isolated_database_url().await else {
        return;
    };
    let old = crate::outbox::ProcessingOutbox::connect(
        &url,
        "https://processor.invalid/finalized",
        "fixture-token",
    )
    .await
    .unwrap();
    let f = Fixture::new().await;
    let signing_key = SigningKey::from_bytes(&[7u8; 32]).unwrap();
    let owner = hex::encode(signing_key.verifying_key().to_bytes());
    let hash = hex::encode(Sha256::digest(b"original"));
    let runtime = ResumableRuntime::new(
        f.storage.clone(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend,
        runtime.store,
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    );
    let init = manager
        .init_session(
            &owner,
            ResumableUploadInitRequest {
                sha256: hash.clone(),
                size: 8,
                content_type: "video/mp4".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    manager
        .upload_chunk(
            &init.upload_id,
            Some(&init.required_headers["Authorization"]),
            "bytes 0-7/8",
            Bytes::from_static(b"original"),
        )
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let detector = Router::new().route(
        "/detect",
        axum::routing::post(move |Json(payload): Json<serde_json::Value>| {
            let tx = tx.clone();
            async move {
                tx.send(payload).await.unwrap();
                Json(serde_json::json!({"signals":{"nsfw":{"state":"absent"}}}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let detector_task = tokio::spawn(async move { axum::serve(listener, detector).await.unwrap() });
    let mut state = crate::tests::test_state(None, 1);
    let s = Arc::get_mut(&mut state).unwrap();
    s.resumable_runtime = ResumableRuntime::new(
        f.storage.with_mode(WriteMode::Gcs).unwrap(),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        Some(&url),
    )
    .await
    .unwrap();
    s.processing_outbox = Some(old);
    s.config.ai_detector_base_url = Some(endpoint);
    s.config.cdn_base_url = "https://media.invalid".into();
    let mut event = crate::NostrEvent {
        id: String::new(),
        pubkey: owner,
        created_at: 1,
        kind: 24242,
        tags: vec![vec!["t".into(), "upload".into()]],
        content: String::new(),
        sig: String::new(),
    };
    event.id = crate::compute_event_id(&event).unwrap();
    event.sig = hex::encode(
        signing_key
            .sign_prehash(&hex::decode(&event.id).unwrap())
            .unwrap()
            .to_bytes(),
    );
    let authorization = format!(
        "Nostr {}",
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            serde_json::to_vec(&serde_json::json!({"id":event.id,"pubkey":event.pubkey,"created_at":event.created_at,"kind":event.kind,"tags":event.tags,"content":event.content,"sig":event.sig})).unwrap()
        )
    );
    let request = || {
        axum::http::Request::builder()
            .method("POST")
            .uri(format!("/upload/{}/complete", init.upload_id))
            .header("Authorization", &authorization)
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let app = crate::build_router(state);
    assert_eq!(
        app.clone().oneshot(request()).await.unwrap().status(),
        StatusCode::OK
    );
    let captured = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await;
    // Stop the local actor even on the deliberately failing RED observation.
    if captured.is_err() {
        detector_task.abort();
    }
    let captured = captured
        .expect("successful legacy session must dispatch its NSFW scan")
        .unwrap();
    assert_eq!(captured["sha256"], hash);
    assert_eq!(captured["url"], format!("https://media.invalid/{hash}"));
    assert_eq!(captured["signals"], serde_json::json!(["nsfw"]));
    assert_eq!(
        app.oneshot(request()).await.unwrap().status(),
        StatusCode::OK
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "completion replay must not dispatch a second scan"
    );
    let db = crate::coordination::connect(&url).await.unwrap();
    assert_eq!(
        db.query_one("SELECT count(*) FROM upload_processing_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
    detector_task.abort();
}
