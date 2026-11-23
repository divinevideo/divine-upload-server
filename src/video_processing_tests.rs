//! Request-level coverage for the restrictive video processing directive.
use super::*;
use axum::{
    body::to_bytes,
    http::{HeaderMap, Request, Uri},
    routing::any,
};
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tower::ServiceExt;

const HEADER: &str = "x-divine-video-processing";
const ORIGINAL: &[u8] = b"synthetic original video bytes";

fn auth_headers() -> HeaderMap {
    let key = SigningKey::from_bytes(&[7; 32]).unwrap();
    let mut event = NostrEvent {
        id: String::new(),
        pubkey: hex::encode(key.verifying_key().to_bytes()),
        created_at: 1,
        kind: BLOSSOM_AUTH_KIND,
        tags: vec![vec!["t".into(), "upload".into()]],
        content: String::new(),
        sig: String::new(),
    };
    event.id = compute_event_id(&event).unwrap();
    event.sig = hex::encode(
        key.sign_prehash(&hex::decode(&event.id).unwrap())
            .unwrap()
            .to_bytes(),
    );
    let value = serde_json::json!({"id":event.id,"pubkey":event.pubkey,"created_at":event.created_at,"kind":event.kind,"tags":event.tags,"content":event.content,"sig":event.sig});
    let encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        serde_json::to_vec(&value).unwrap(),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Nostr {encoded}")).unwrap(),
    );
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("video/mp4"));
    headers
}

#[derive(Default)]
struct Fixture {
    objects: Mutex<HashMap<String, Bytes>>,
    writes: Mutex<Vec<String>>,
    requests: Mutex<Vec<String>>,
    dispatches: Mutex<Vec<String>>,
}

fn metadata(key: &str, bytes: &[u8]) -> serde_json::Value {
    serde_json::json!({"name":key,"bucket":"fixture","generation":"1","metageneration":"1","selfLink":"test","mediaLink":"test","etag":"test","id":"test","size":bytes.len().to_string(),"contentType":"video/mp4"})
}

async fn fixture_request(
    State(f): State<Arc<Fixture>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path();
    if ["/transcode", "/transcribe", "/detect"].contains(&path) {
        assert_eq!(method, Method::POST);
        f.dispatches.lock().unwrap().push(path.into());
        return Json(serde_json::json!({"signals":{"nsfw":{"state":"absent"}}})).into_response();
    }
    f.requests.lock().unwrap().push(format!("{method} {uri}"));
    if method == Method::POST && path.contains("/copyTo/") {
        let (source, destination) = path.split_once("/copyTo/").unwrap();
        let source = source.split("/o/").nth(1).unwrap().replace("%2F", "/");
        let destination = destination.split("/o/").nth(1).unwrap();
        let mut objects = f.objects.lock().unwrap();
        let bytes = objects[&source].clone();
        objects.insert(destination.into(), bytes.clone());
        f.writes.lock().unwrap().push(destination.into());
        return Json(metadata(destination, &bytes)).into_response();
    }
    if method == Method::POST && uri.query().unwrap_or_default().contains("uploadType=media") {
        let params: HashMap<_, _> = url::form_urlencoded::parse(uri.query().unwrap().as_bytes())
            .into_owned()
            .collect();
        let key = &params["name"];
        f.objects.lock().unwrap().insert(key.clone(), body.clone());
        f.writes.lock().unwrap().push(key.clone());
        return Json(metadata(key, &body)).into_response();
    }
    if method == Method::POST {
        // GCS multipart upload: decode metadata and preserve the exact original payload.
        let content_type = headers[header::CONTENT_TYPE].to_str().unwrap();
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap()
            .trim_matches('"');
        let body = String::from_utf8(body.to_vec()).unwrap();
        let parts: Vec<_> = body.split(&format!("--{boundary}")).collect();
        let meta: serde_json::Value =
            serde_json::from_str(parts[1].split_once("\r\n\r\n").unwrap().1.trim()).unwrap();
        let payload = parts[2]
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .strip_suffix("\r\n")
            .unwrap();
        let key = meta["name"].as_str().unwrap();
        let bytes = Bytes::copy_from_slice(payload.as_bytes());
        f.objects.lock().unwrap().insert(key.into(), bytes.clone());
        f.writes.lock().unwrap().push(key.into());
        return Json(metadata(key, &bytes)).into_response();
    }
    let key = path.split("/o/").nth(1).unwrap_or_default();
    // GCS object names are percent encoded, including the session manifest path.
    let key = key.replace("%2F", "/").replace("%2f", "/");
    let mut objects = f.objects.lock().unwrap();
    if method == Method::DELETE {
        objects.remove(&key);
        return StatusCode::NO_CONTENT.into_response();
    }
    let Some(bytes) = objects.get(&key) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":{"code":404,"message":"Not Found","errors":[]}})),
        )
            .into_response();
    };
    if uri.query().unwrap_or_default().contains("alt=media") {
        return bytes.clone().into_response();
    }
    Json(metadata(&key, bytes)).into_response()
}

async fn fixture() -> (Arc<AppState>, Arc<Fixture>, tokio::task::JoinHandle<()>) {
    let f = Arc::new(Fixture::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .fallback(any(fixture_request))
        .with_state(f.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = GcsClient::new(
        ClientConfig {
            storage_endpoint: endpoint.clone(),
            ..Default::default()
        }
        .anonymous(),
    );
    let storage = storage::MediaStorage::new(
        storage::WriteMode::Gcs,
        Some(client.clone()),
        "fixture".into(),
        None,
    )
    .unwrap();
    let runtime =
        resumable::ResumableRuntime::new(storage.clone(), Some(client), "fixture".into(), None)
            .await
            .unwrap();
    let mut state = tests::test_state(Some(&endpoint), 1);
    let s = Arc::get_mut(&mut state).unwrap();
    s.media_storage = storage;
    s.resumable_runtime = runtime;
    s.config.transcoder_url = Some(endpoint.clone());
    s.config.ai_detector_base_url = Some(endpoint);
    (state, f, server)
}

async fn request(
    state: Arc<AppState>,
    method: Method,
    path: &str,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .body(body)
        .unwrap();
    *req.headers_mut() = headers;
    build_router(state).oneshot(req).await.unwrap()
}

async fn assert_status(response: Response, expected: StatusCode) -> serde_json::Value {
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status, expected, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

async fn settle_dispatches(f: &Fixture, transcripts: usize, scans: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready = {
                let calls = f.dispatches.lock().unwrap();
                calls.iter().filter(|s| *s == "/transcribe").count() == transcripts
                    && calls.iter().filter(|s| *s == "/detect").count() == scans
            };
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // All dispatches are spawned by the request before its response. Allow them
    // to reach this loopback server before asserting that HLS was not submitted.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn copy_only_direct_stores_original_and_preserves_transcription_and_moderation() {
    for copy_only in [false, true] {
        let (state, f, server) = fixture().await;
        let hash = hex::encode(Sha256::digest(ORIGINAL));
        let mut headers = auth_headers();
        if copy_only {
            headers.insert(HEADER, HeaderValue::from_static("copy-only"));
        }
        for path in ["/upload", "/"] {
            let value = assert_status(
                request(
                    state.clone(),
                    Method::PUT,
                    path,
                    headers.clone(),
                    Body::from(ORIGINAL),
                )
                .await,
                StatusCode::OK,
            )
            .await;
            assert_eq!(value["sha256"], hash);
            assert_eq!(value["size"], ORIGINAL.len());
        }
        settle_dispatches(&f, 2, 1).await;
        assert_eq!(f.objects.lock().unwrap()[&hash].as_ref(), ORIGINAL);
        assert_eq!(*f.writes.lock().unwrap(), vec![hash]);
        assert_eq!(
            f.dispatches
                .lock()
                .unwrap()
                .iter()
                .filter(|s| *s == "/transcode")
                .count(),
            if copy_only { 0 } else { 2 }
        );
        server.abort();
    }
}

#[tokio::test]
async fn copy_only_invalid_headers_are_rejected_before_auth_body_or_storage() {
    let (state, f, server) = fixture().await;
    for values in [
        vec![""],
        vec!["allow-encode"],
        vec!["Copy-Only"],
        vec!["copy-only, copy-only"],
        vec!["copy-only", "copy-only"],
        vec!["copy-only", "allow-encode"],
        vec![" copy-only"],
    ] {
        for (method, path) in [
            (Method::PUT, "/upload"),
            (Method::POST, "/upload/session/complete"),
            (Method::POST, "/upload/init"),
        ] {
            let mut headers = HeaderMap::new();
            for value in &values {
                headers.append(HEADER, HeaderValue::from_str(value).unwrap());
            }
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            let body = if path.ends_with("init") {
                Body::from(
                    r#"{"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":5,"contentType":"video/mp4"}"#,
                )
            } else {
                Body::from_stream(futures::stream::poll_fn(
                    |_| -> std::task::Poll<Option<Result<Bytes, std::convert::Infallible>>> {
                        panic!("invalid directive must not read body")
                    },
                ))
            };
            assert_status(
                request(state.clone(), method, path, headers, body).await,
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
    }
    assert!(f.requests.lock().unwrap().is_empty());
    assert!(f.dispatches.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn copy_only_init_rejects_unpersisted_directive() {
    let (state, f, server) = fixture().await;
    let mut headers = auth_headers();
    headers.insert(HEADER, HeaderValue::from_static("copy-only"));
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let response = assert_status(request(state, Method::POST, "/upload/init", headers, Body::from(r#"{"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":5,"contentType":"video/mp4"}"#)).await, StatusCode::BAD_REQUEST).await;
    assert!(response["error"].as_str().unwrap().contains("completion"));
    assert!(f.requests.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn copy_only_completion_preserves_existing_on_demand_behavior() {
    for copy_only in [false, true] {
        let (state, f, server) = fixture().await;
        let hash = hex::encode(Sha256::digest(ORIGINAL));
        let mut headers = auth_headers();
        if copy_only {
            headers.insert(HEADER, HeaderValue::from_static("copy-only"));
        }
        let session = resumable::UploadSession {
            processing_outbox_required: Some(false),
            revision: 0,
            chunk_size: 100,
            upload_id: "fixture-session".into(),
            owner: validate_auth(&headers, "upload").unwrap().pubkey,
            final_sha256: hash.clone(),
            declared_size: ORIGINAL.len() as u64,
            content_type: "video/mp4".into(),
            file_name: None,
            expires_at_epoch_secs: u64::MAX,
            next_offset: ORIGINAL.len() as u64,
            session_url: "http://localhost/unused".into(),
            session_token: "synthetic".into(),
            temp_object: "__resumable/uploads/fixture-session/blob".into(),
            storage_complete: true,
            finalized_object: None,
        };
        let manifest = "__resumable/sessions/fixture-session.json";
        f.objects.lock().unwrap().insert(
            manifest.into(),
            Bytes::from(serde_json::to_vec(&session).unwrap()),
        );
        f.objects
            .lock()
            .unwrap()
            .insert(session.temp_object.clone(), Bytes::from_static(ORIGINAL));
        for _ in 0..2 {
            let value = assert_status(
                request(
                    state.clone(),
                    Method::POST,
                    "/upload/fixture-session/complete",
                    headers.clone(),
                    Body::empty(),
                )
                .await,
                StatusCode::OK,
            )
            .await;
            assert_eq!(value["sha256"], hash);
            assert_eq!(value["size"], ORIGINAL.len());
            assert!(value["hlsQueued"].is_null());
        }
        settle_dispatches(&f, 0, 1).await;
        assert_eq!(*f.dispatches.lock().unwrap(), vec!["/detect"]);
        let objects = f.objects.lock().unwrap();
        assert_eq!(objects[&hash].as_ref(), ORIGINAL);
        assert!(!objects.contains_key(&session.temp_object));
        let saved: resumable::UploadSession = serde_json::from_slice(&objects[manifest]).unwrap();
        assert_eq!(saved.processing_outbox_required, Some(false));
        assert_eq!(saved.finalized_object.as_deref(), Some(hash.as_str()));
        assert_eq!(*f.writes.lock().unwrap(), vec![hash, manifest.into()]);
        server.abort();
    }
}

#[tokio::test]
async fn copy_only_rejects_retained_coordination_before_admission_or_session_lookup() {
    let (state, f, server) = fixture().await;
    // GCS initialization retains this URL without opening a database. If the
    // guard reaches historical intent lookup, the request fails instead of 400.
    state
        .media_storage
        .initialize_coordination(Some("postgres://127.0.0.1:1/unused"))
        .await
        .unwrap();
    for (method, path) in [
        (Method::PUT, "/upload"),
        (Method::POST, "/upload/session/complete"),
    ] {
        let mut headers = auth_headers();
        headers.insert(HEADER, HeaderValue::from_static("copy-only"));
        let body = Body::from_stream(futures::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Bytes, std::convert::Infallible>>> {
                panic!("unsupported directive must not read body")
            },
        ));
        let value = assert_status(
            request(state.clone(), method, path, headers, body).await,
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains("durable processing"));
    }
    assert!(f.requests.lock().unwrap().is_empty());
    assert!(f.dispatches.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn copy_only_header_is_allowed_by_real_cors_preflight() {
    let (state, _, server) = fixture().await;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ORIGIN,
        HeaderValue::from_static("https://client.example"),
    );
    headers.insert(
        header::ACCESS_CONTROL_REQUEST_METHOD,
        HeaderValue::from_static("PUT"),
    );
    headers.insert(
        header::ACCESS_CONTROL_REQUEST_HEADERS,
        HeaderValue::from_static(HEADER),
    );
    let response = request(state, Method::OPTIONS, "/upload", headers, Body::empty()).await;
    assert!(response.status().is_success());
    assert!(response.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
        .to_str()
        .unwrap()
        .contains(HEADER));
    server.abort();
}

#[tokio::test]
async fn copy_only_non_ascii_header_fails_without_echoing_input() {
    let (state, f, server) = fixture().await;
    let mut headers = HeaderMap::new();
    headers.insert(
        HEADER,
        HeaderValue::from_bytes(b"sensitive-invalid-\xff").unwrap(),
    );
    let response = assert_status(
        request(state, Method::PUT, "/upload", headers, Body::empty()).await,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(!response.to_string().contains("sensitive-invalid"));
    assert!(f.requests.lock().unwrap().is_empty());
    server.abort();
}
