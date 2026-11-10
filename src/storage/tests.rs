use super::*;
use std::future::IntoFuture;

#[test]
fn b2_primary_policy_is_explicit_and_keeps_legacy_order() {
    let mode: WriteMode = "b2_primary".parse().expect("new B2-authoritative policy");
    assert_ne!(mode, WriteMode::Dual);
    assert_eq!(serde_json::to_value(mode).unwrap(), "b2_primary");
    assert_eq!(
        crate::outbox::required_destinations(mode, "gcs-backup", Some("b2-authority")).unwrap(),
        vec![
            (StorageProvider::B2, "b2-authority"),
            (StorageProvider::Gcs, "gcs-backup")
        ]
    );
    assert_eq!(
        crate::outbox::required_destinations(WriteMode::Dual, "gcs-backup", Some("b2-authority"))
            .unwrap(),
        vec![
            (StorageProvider::Gcs, "gcs-backup"),
            (StorageProvider::B2, "b2-authority")
        ]
    );
}

#[test]
fn mode_is_explicit_and_rejects_typos() {
    assert_eq!("dual".parse::<WriteMode>().unwrap(), WriteMode::Dual);
    assert_eq!("b2".parse::<WriteMode>().unwrap(), WriteMode::B2);
    assert!("both".parse::<WriteMode>().is_err());
}

#[test]
fn b2_endpoint_rejects_untrusted_regions() {
    assert!(B2Store::new(
        "bucket".into(),
        "x.example/evil".into(),
        "key".into(),
        "secret".into()
    )
    .is_err());
}

#[test]
fn object_references_match_blossom_shape() {
    let reference = ObjectRef {
        provider: StorageProvider::B2,
        bucket: "bucket".into(),
        key: "key".into(),
        version: Some("immutable".into()),
    };
    let value = serde_json::to_value(reference).unwrap();
    assert_eq!(value["provider"], "b2");
    assert_eq!(value["version"], "immutable");
}

#[tokio::test]
async fn b2_write_pins_version_and_preserves_metadata() {
    use axum::{
        body::Bytes as BodyBytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use std::sync::{Arc, Mutex};
    type Record = (String, String, Bytes);
    async fn origin(
        State(records): State<Arc<Mutex<Vec<Record>>>>,
        method: Method,
        headers: HeaderMap,
        body: BodyBytes,
    ) -> axum::response::Response {
        assert!(headers["authorization"]
            .to_str()
            .unwrap()
            .contains("SignedHeaders="));
        assert!(headers.contains_key("x-amz-content-sha256"));
        if method == Method::HEAD && records.lock().unwrap().is_empty() {
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::PUT {
            records.lock().unwrap().push((
                headers["content-type"].to_str().unwrap().into(),
                headers["x-amz-meta-owner"].to_str().unwrap().into(),
                body,
            ));
            return ([("x-amz-version-id", "version-1")], "").into_response();
        }
        (
            [("x-amz-version-id", "version-1")],
            records.lock().unwrap()[0].2.clone(),
        )
            .into_response()
    }
    let records = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(records.clone()),
        )
        .into_future(),
    );
    let store = B2Store::new(
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let (reference, new) = store
        .put(
            "nested/a b",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner"),
        )
        .await
        .unwrap();
    assert!(new);
    assert_eq!(reference.version.as_deref(), Some("version-1"));
    let records = records.lock().unwrap();
    assert_eq!(records[0].0, "video/mp4");
    assert_eq!(records[0].1, "owner");
    assert_eq!(records[0].2, Bytes::from_static(b"original"));
    task.abort();
}

#[tokio::test]
async fn provider_forbidden_is_not_missing_or_a_successful_write() {
    use axum::{http::StatusCode, routing::any, Router};
    use std::future::IntoFuture;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new().route(
                "/*path",
                any(|| async { (StatusCode::FORBIDDEN, "sensitive-provider-details") }),
            ),
        )
        .into_future(),
    );
    let store = B2Store::new(
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let error = store.stat("key").await.unwrap_err().to_string();
    assert!(error.contains("403"));
    assert!(!error.contains("sensitive"));
    task.abort();
}

#[tokio::test]
async fn dual_retry_repairs_b2_without_rewriting_existing_gcs() {
    use axum::{
        extract::State,
        http::{Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    #[derive(Default)]
    struct Fixture {
        b2_available: AtomicBool,
        b2_written: AtomicBool,
        gcs_writes: AtomicUsize,
    }
    async fn origin(
        State(f): State<Arc<Fixture>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        if uri.path().starts_with("/storage/") {
            if method == Method::POST {
                f.gcs_writes.fetch_add(1, Ordering::SeqCst);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            if uri.query().unwrap_or_default().contains("alt=media") {
                return Bytes::from_static(b"original").into_response();
            }
            return Json(serde_json::json!({"name":"hash","bucket":"gcs-bucket","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"gcs-bucket/hash/1","size":"8","contentType":"video/mp4","metadata":{"owner":"first-owner"}})).into_response();
        }
        if method == Method::HEAD && !f.b2_written.load(Ordering::SeqCst) {
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::PUT {
            if !f.b2_available.load(Ordering::SeqCst) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            f.b2_written.store(true, Ordering::SeqCst);
            return ([("x-amz-version-id", "version-1")], "").into_response();
        }
        (
            [("x-amz-version-id", "version-1")],
            Bytes::from_static(b"original"),
        )
            .into_response()
    }
    let fixture = Arc::new(Fixture::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(fixture.clone()),
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
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let storage =
        MediaStorage::new(WriteMode::Dual, Some(gcs), "gcs-bucket".into(), Some(b2)).unwrap();
    assert!(storage
        .put(
            "hash",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("new-owner")
        )
        .await
        .is_err());
    fixture.b2_available.store(true, Ordering::SeqCst);
    let result = storage
        .put(
            "hash",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("new-owner"),
        )
        .await
        .unwrap();
    assert_eq!(result.objects.len(), 2);
    assert!(result.newly_stored);
    assert_eq!(fixture.gcs_writes.load(Ordering::SeqCst), 0);
    task.abort();
}

#[tokio::test]
async fn b2_file_upload_uses_bounded_multipart_and_verifies_pinned_result() {
    use axum::{
        body::Bytes as BodyBytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use std::sync::{Arc, Mutex};
    #[derive(Default)]
    struct Fixture {
        parts: Vec<Bytes>,
        completed: bool,
    }
    async fn origin(
        State(f): State<Arc<Mutex<Fixture>>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: BodyBytes,
    ) -> axum::response::Response {
        if method == Method::HEAD {
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::POST && uri.query() == Some("uploads=") {
            assert_eq!(headers["content-type"], "video/mp4");
            assert_eq!(headers["x-amz-meta-owner"], "owner");
            return "<InitiateMultipartUploadResult><UploadId>id+space</UploadId></InitiateMultipartUploadResult>".into_response();
        }
        if method == Method::PUT {
            assert!(uri.query().unwrap().contains("uploadId=id%2Bspace"));
            assert!(body.len() <= 8 * 1024 * 1024);
            f.lock().unwrap().parts.push(body);
            return ([("etag", "\"part-etag\"")], "").into_response();
        }
        if method == Method::POST {
            assert!(std::str::from_utf8(&body)
                .unwrap()
                .contains("&quot;part-etag&quot;"));
            f.lock().unwrap().completed = true;
            return ([("x-amz-version-id","multipart-version")],"<CompleteMultipartUploadResult><ETag>not-sha256</ETag></CompleteMultipartUploadResult>").into_response();
        }
        assert_eq!(method, Method::GET);
        assert_eq!(uri.query(), Some("versionId=multipart-version"));
        let mut body = Vec::new();
        for part in &f.lock().unwrap().parts {
            body.extend_from_slice(part);
        }
        ([("x-amz-version-id", "multipart-version")], body).into_response()
    }
    let f = Arc::new(Mutex::new(Fixture::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(f.clone())
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .into_future(),
    );
    let store = B2Store::new(
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let file = tempfile::NamedTempFile::new().unwrap();
    let bytes = vec![42u8; 9 * 1024 * 1024];
    tokio::fs::write(file.path(), bytes).await.unwrap();
    let (reference, new) = store
        .put_file("media", file.path(), "video/mp4", Some("owner"))
        .await
        .unwrap();
    assert!(new);
    assert_eq!(reference.version.as_deref(), Some("multipart-version"));
    let f = f.lock().unwrap();
    assert!(f.completed);
    assert_eq!(f.parts.len(), 2);
    assert_eq!(f.parts[0].len(), 8 * 1024 * 1024);
    assert_eq!(f.parts[1].len(), 1024 * 1024);
    task.abort();
}

#[tokio::test]
async fn stale_multipart_cleanup_preserves_live_leases_and_other_buckets() {
    use axum::{
        extract::State,
        http::{Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use std::sync::{Arc, Mutex};
    let Ok(database_url) = std::env::var("TEST_COORDINATION_DATABASE_URL") else {
        return;
    };
    async fn origin(
        State(aborts): State<Arc<Mutex<Vec<String>>>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        assert_eq!(method, Method::DELETE);
        aborts.lock().unwrap().push(uri.to_string());
        StatusCode::NO_CONTENT.into_response()
    }
    let aborts = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(aborts.clone()),
        )
        .into_future(),
    );
    let store = B2Store::new(
        "multipart-test-bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    store.initialize_coordination(&database_url).await.unwrap();
    let db = crate::coordination::connect(&database_url).await.unwrap();
    db.execute("DELETE FROM b2_multipart_intents WHERE bucket IN ('multipart-test-bucket','multipart-other-bucket')",&[]).await.unwrap();
    db.batch_execute("INSERT INTO b2_multipart_intents VALUES ('multipart-test-bucket','expired','expired-id',now()-interval '1 minute'),('multipart-test-bucket','active','active-id',now()+interval '10 minutes'),('multipart-other-bucket','foreign','foreign-id',now()-interval '1 minute')").await.unwrap();
    assert_eq!(store.sweep_stale_multipart(1).await.unwrap(), 1);
    assert_eq!(
        *aborts.lock().unwrap(),
        vec!["/multipart-test-bucket/expired?uploadId=expired-id"]
    );
    let remaining:i64=db.query_one("SELECT count(*) FROM b2_multipart_intents WHERE bucket IN ('multipart-test-bucket','multipart-other-bucket')",&[]).await.unwrap().get(0);
    assert_eq!(remaining, 2);
    db.execute("DELETE FROM b2_multipart_intents WHERE bucket IN ('multipart-test-bucket','multipart-other-bucket')",&[]).await.unwrap();
    task.abort();
}

#[tokio::test]
async fn concurrent_b2_writers_share_canonical_version_across_replicas() {
    use axum::{
        body::Bytes as BodyBytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode},
        response::IntoResponse,
        routing::any,
        Router,
    };
    use std::sync::{Arc, Mutex};
    let Ok(database_url) = std::env::var("TEST_COORDINATION_DATABASE_URL") else {
        return;
    };
    #[derive(Default)]
    struct Fixture {
        writes: Vec<(String, Bytes)>,
    }
    async fn origin(
        State(f): State<Arc<Mutex<Fixture>>>,
        method: Method,
        headers: HeaderMap,
        body: BodyBytes,
    ) -> axum::response::Response {
        if method == Method::HEAD {
            let existing = f.lock().unwrap().writes.last().cloned();
            if let Some((owner, body)) = existing {
                return (
                    [
                        ("x-amz-version-id", "version-1".to_string()),
                        ("content-length", body.len().to_string()),
                        ("content-type", "video/mp4".to_string()),
                        ("x-amz-meta-owner", owner),
                    ],
                    "",
                )
                    .into_response();
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::PUT {
            f.lock().unwrap().writes.push((
                headers["x-amz-meta-owner"].to_str().unwrap().to_string(),
                body,
            ));
            return ([("x-amz-version-id", "version-1")], "").into_response();
        }
        (
            [("x-amz-version-id", "version-1")],
            f.lock().unwrap().writes[0].1.clone(),
        )
            .into_response()
    }
    let f = Arc::new(Mutex::new(Fixture::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(f.clone()),
        )
        .into_future(),
    );
    let make_storage = || {
        MediaStorage::new(
            WriteMode::B2,
            None,
            "".into(),
            Some(
                B2Store::new(
                    "concurrent-test-bucket".into(),
                    "us-west-001".into(),
                    "key".into(),
                    "secret".into(),
                )
                .unwrap()
                .with_endpoint(endpoint.clone()),
            ),
        )
        .unwrap()
    };
    let first = make_storage();
    let second = make_storage();
    first
        .initialize_coordination(Some(&database_url))
        .await
        .unwrap();
    second
        .initialize_coordination(Some(&database_url))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        first.put(
            "same-hash",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner-a")
        ),
        second.put(
            "same-hash",
            Bytes::from_static(b"original"),
            "video/mp4",
            Some("owner-b")
        )
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.objects, b.objects);
    assert_eq!(f.lock().unwrap().writes.len(), 1);
    task.abort();
}

#[tokio::test]
async fn dual_copy_uses_metadata_of_conditional_gcs_winner() {
    use axum::{
        body::Bytes as BodyBytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    async fn origin(
        State(won): State<Arc<AtomicBool>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        _body: BodyBytes,
    ) -> axum::response::Response {
        if uri.path().starts_with("/upload/") {
            won.store(true, Ordering::SeqCst);
            return (StatusCode::PRECONDITION_FAILED,Json(serde_json::json!({"error":{"code":412,"message":"precondition failed","errors":[]}}))).into_response();
        }
        if uri.path().starts_with("/storage/") {
            if uri.query().unwrap_or_default().contains("alt=media") {
                return Bytes::from_static(b"original").into_response();
            }
            if !won.load(Ordering::SeqCst) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error":{"code":404,"message":"missing","errors":[]}})),
                )
                    .into_response();
            }
            return Json(serde_json::json!({"name":"hash","bucket":"gcs-bucket","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"test","size":"8","contentType":"video/mp4","metadata":{"owner":"winning-owner"}})).into_response();
        }
        if method == Method::HEAD {
            return StatusCode::NOT_FOUND.into_response();
        }
        if method == Method::PUT {
            assert_eq!(headers["x-amz-meta-owner"], "winning-owner");
            assert_eq!(headers["content-type"], "video/mp4");
            return ([("x-amz-version-id", "winner-copy")], "").into_response();
        }
        (
            [("x-amz-version-id", "winner-copy")],
            Bytes::from_static(b"original"),
        )
            .into_response()
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(Arc::new(AtomicBool::new(false))),
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
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let storage =
        MediaStorage::new(WriteMode::Dual, Some(gcs), "gcs-bucket".into(), Some(b2)).unwrap();
    assert_eq!(
        storage
            .put(
                "hash",
                Bytes::from_static(b"original"),
                "application/octet-stream",
                Some("losing-owner")
            )
            .await
            .unwrap()
            .objects
            .len(),
        2
    );
    task.abort();
}

#[tokio::test]
async fn dual_verified_read_requires_both_copies_without_writing() {
    use axum::{
        extract::State,
        http::{Method, StatusCode, Uri},
        response::IntoResponse,
        routing::any,
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    #[derive(Default)]
    struct Fixture {
        ready: AtomicBool,
        corrupt: AtomicBool,
        writes: AtomicUsize,
    }
    async fn origin(
        State(f): State<Arc<Fixture>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        if method == Method::PUT || method == Method::POST {
            f.writes.fetch_add(1, Ordering::SeqCst);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        if uri.path().starts_with("/storage/") {
            if uri.query().unwrap_or_default().contains("alt=media") {
                return Bytes::from_static(b"original").into_response();
            }
            return Json(serde_json::json!({"name":"hash.jpg","bucket":"gcs-bucket","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"test","size":"8","contentType":"image/jpeg","metadata":{"owner":"owner"}})).into_response();
        }
        if !f.ready.load(Ordering::SeqCst) {
            return StatusCode::NOT_FOUND.into_response();
        }
        (
            [
                ("x-amz-version-id", "version-1"),
                ("content-length", "8"),
                ("content-type", "image/jpeg"),
            ],
            if f.corrupt.load(Ordering::SeqCst) {
                Bytes::from_static(b"corrupt!")
            } else {
                Bytes::from_static(b"original")
            },
        )
            .into_response()
    }
    let f = Arc::new(Fixture::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/*path", any(origin))
                .with_state(f.clone()),
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
        "bucket".into(),
        "us-west-001".into(),
        "key".into(),
        "secret".into(),
    )
    .unwrap()
    .with_endpoint(endpoint);
    let storage =
        MediaStorage::new(WriteMode::Dual, Some(gcs), "gcs-bucket".into(), Some(b2)).unwrap();
    assert!(storage.read_verified("hash.jpg").await.is_err());
    assert_eq!(f.writes.load(Ordering::SeqCst), 0);
    f.ready.store(true, Ordering::SeqCst);
    assert_eq!(
        storage.read_verified("hash.jpg").await.unwrap(),
        Bytes::from_static(b"original")
    );
    assert_eq!(f.writes.load(Ordering::SeqCst), 0);
    f.corrupt.store(true, Ordering::SeqCst);
    assert!(storage.read_verified("hash.jpg").await.is_err());
    assert_eq!(f.writes.load(Ordering::SeqCst), 0);
    task.abort();
}
