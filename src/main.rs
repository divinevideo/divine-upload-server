// ABOUTME: Rust upload service for Blossom blob uploads
// ABOUTME: Handles resumable sessions, streaming upload to GCS, and media follow-up hooks

mod media_type;
mod request_log;
mod resumable;
mod thumbnail;

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{delete, get, head, options, post, put},
    Router,
};
use bytes::Bytes;
use futures::StreamExt;
use google_cloud_storage::{
    client::{Client as GcsClient, ClientConfig},
    http::objects::{
        download::Range as DownloadRange,
        get::GetObjectRequest,
        upload::{Media, UploadObjectRequest, UploadType},
        Object,
    },
};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder;
use k256::schnorr::{
    signature::hazmat::{PrehashSigner, PrehashVerifier},
    Signature, SigningKey, VerifyingKey,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::NamedTempFile;
use tower::Service;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};
use tracing_subscriber::filter::Directive;

const DEFAULT_UPLOAD_ROUTE_MAX_BODY_SIZE: u64 = 1024 * 1024;

// Configuration
#[derive(Clone)]
struct Config {
    gcs_bucket: String,
    cdn_base_url: String,
    upload_base_url: String,
    port: u16,
    migration_nsec: Option<String>,
    transcoder_url: Option<String>,
    transcriber_url: Option<String>,
    /// Self-hosted detector invoked for every newly stored video. The request
    /// is best-effort and runs off the upload response path.
    ai_detector_base_url: Option<String>,
    /// Shared secret the transcoder requires on `/transcribe/audio`. Injected
    /// on every proxied transcription so the transcoder can trust the request
    /// already passed this service's Nostr auth.
    transcribe_shared_secret: Option<String>,
    resumable_session_ttl_secs: u64,
    resumable_chunk_size: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            gcs_bucket: env::var("GCS_BUCKET")
                .unwrap_or_else(|_| "divine-blossom-media".to_string()),
            cdn_base_url: env::var("CDN_BASE_URL")
                .unwrap_or_else(|_| "https://media.divine.video".to_string()),
            upload_base_url: env::var("UPLOAD_BASE_URL")
                .unwrap_or_else(|_| "https://upload.divine.video".to_string()),
            port: env::var("PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            migration_nsec: env::var("MIGRATION_NSEC").ok(),
            // URL of the divine-transcoder service for HLS generation
            transcoder_url: env::var("TRANSCODER_URL").ok(),
            // URL of the transcription service (defaults to TRANSCODER_URL when not explicitly set)
            transcriber_url: env::var("TRANSCRIBER_URL")
                .ok()
                .or_else(|| env::var("TRANSCODER_URL").ok()),
            ai_detector_base_url: env::var("AI_DETECTOR_BASE_URL")
                .ok()
                .map(|value| value.trim().trim_end_matches('/').to_string())
                .filter(|value| !value.is_empty()),
            transcribe_shared_secret: env::var("TRANSCRIBE_SHARED_SECRET")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            resumable_session_ttl_secs: env::var("RESUMABLE_SESSION_TTL_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(resumable::DEFAULT_RESUMABLE_SESSION_TTL_SECS),
            resumable_chunk_size: resolve_resumable_chunk_size(
                env::var("RESUMABLE_CHUNK_SIZE")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .filter(|value: &u64| *value > 0)
                    .unwrap_or(resumable::DEFAULT_RESUMABLE_CHUNK_SIZE),
                load_resumable_max_request_body_size(),
            ),
        }
    }
}

fn load_resumable_max_request_body_size() -> u64 {
    [
        "RESUMABLE_MAX_REQUEST_BODY_SIZE",
        "UPLOAD_ROUTE_MAX_BODY_SIZE",
    ]
    .into_iter()
    .find_map(|name| {
        env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value: &u64| *value > 0)
    })
    .unwrap_or(DEFAULT_UPLOAD_ROUTE_MAX_BODY_SIZE)
}

fn resolve_resumable_chunk_size(
    configured_chunk_size: u64,
    upload_route_max_body_size: u64,
) -> u64 {
    configured_chunk_size.min(upload_route_max_body_size)
}

// App state shared across handlers
struct AppState {
    gcs_client: GcsClient,
    config: Config,
    /// Shared HTTP client with explicit timeouts, so a stalled downstream
    /// (e.g. the transcoder) can't pin an inbound request indefinitely.
    http_client: reqwest::Client,
    /// Bounds concurrent `/transcribe` requests. Each holds its buffered audio
    /// (up to `MAX_TRANSCRIBE_AUDIO_BYTES`) for the whole downstream call, so
    /// without this an authenticated flood on the directly-reachable host could
    /// exhaust memory — the edge limiter and the transcoder semaphore don't
    /// protect this process. `MAX_CONCURRENT_TRANSCRIBE` slots cap the buffers.
    transcribe_slots: Arc<tokio::sync::Semaphore>,
}

// Nostr auth event structure
#[derive(Debug, Deserialize)]
struct NostrEvent {
    id: String,
    pubkey: String,
    created_at: i64,
    kind: u32,
    tags: Vec<Vec<String>>,
    content: String,
    sig: String,
}

// Upload response
#[derive(Serialize)]
struct UploadResponse {
    sha256: String,
    size: u64,
    #[serde(rename = "type")]
    content_type: String,
    uploaded: u64,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_url: Option<String>,
    /// Video dimensions as "WIDTHxHEIGHT" (display dimensions after rotation)
    #[serde(skip_serializing_if = "Option::is_none")]
    dim: Option<String>,
}

// Migration request
#[derive(Deserialize)]
struct MigrateRequest {
    source_url: String,
    expected_hash: Option<String>,
    owner: Option<String>, // Owner pubkey for GCS metadata durability
}

// Migration response
#[derive(Serialize)]
struct MigrateResponse {
    sha256: String,
    size: u64,
    #[serde(rename = "type")]
    content_type: String,
    migrated: bool,
    source_url: String,
}

// Error response
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

const BLOSSOM_AUTH_KIND: u32 = 24242;

fn cors_exposed_upload_headers() -> Vec<HeaderName> {
    vec![
        HeaderName::from_static("upload-offset"),
        HeaderName::from_static("upload-length"),
        HeaderName::from_static("upload-expires"),
        HeaderName::from_static("upload-expires-at"),
        HeaderName::from_static("x-divine-chunk-size"),
        // Lets cross-origin callers read the /transcribe backoff hint on 503/429.
        header::RETRY_AFTER,
    ]
}

/// Request headers the browser preflight must allow for uploads to succeed.
///
/// Blossom clients (BUD-02/06) send the blob hash in `x-sha256`; without it in
/// the allow-list the CORS preflight blocks the upload ("Failed to fetch").
/// Kept in sync with the main blossom server's allow-list so a client header is
/// never accepted there but rejected here.
fn cors_allowed_request_headers() -> Vec<HeaderName> {
    vec![
        header::AUTHORIZATION,
        header::CONTENT_TYPE,
        header::CONTENT_RANGE,
        HeaderName::from_static("x-sha256"),
        HeaderName::from_static("x-request-id"),
    ]
}

/// Emit one structured access-log line per request.
///
/// This is what makes the edge's upload records joinable: the divine-blossom
/// edge sets `X-Request-Id` on every proxied upload request, and the same value
/// is logged here as `req_id`. An edge record whose send timed out with no
/// matching line here means the request never completed at origin; a matching
/// line means origin did see it, and shows how long it took.
async fn log_request(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut entry = RequestLogEntry::starting(&req);

    let response = next.run(req).await;
    entry.responded(response.status().as_u16());

    response
}

/// Holds one request's log fields and emits the line when it is dropped.
///
/// Emitting from `Drop` rather than after the `await` is what keeps abandoned
/// requests on the record. When the edge gives up at its timeout the connection
/// goes away, hyper drops the in-flight future, and code after the `await`
/// never runs — so the requests this instrumentation exists to explain would be
/// the only ones producing no origin line at all. They now log with `status=-`,
/// which separates "origin had it and was still working after N ms" from "no
/// line at all", meaning origin never saw the request.
struct RequestLogEntry {
    started: std::time::Instant,
    req_id: String,
    method: String,
    path: String,
    content_length: Option<u64>,
    status: Option<u16>,
}

impl RequestLogEntry {
    fn starting(req: &axum::extract::Request) -> Self {
        Self {
            started: std::time::Instant::now(),
            req_id: request_log::correlation_id(req.headers()),
            method: req.method().as_str().to_string(),
            path: req.uri().path().to_string(),
            content_length: request_log::declared_content_length(req.headers()),
            status: None,
        }
    }

    fn responded(&mut self, status: u16) {
        self.status = Some(status);
    }
}

impl Drop for RequestLogEntry {
    fn drop(&mut self) {
        info!(
            "{}",
            request_log::format_request_log(&request_log::RequestLogFields {
                req_id: std::mem::take(&mut self.req_id),
                method: std::mem::take(&mut self.method),
                path: std::mem::take(&mut self.path),
                status: self.status,
                duration_ms: self.started.elapsed().as_millis() as u64,
                content_length: self.content_length,
            })
        );
    }
}

/// Assemble every route, the CORS layer, and the access log around them.
///
/// Extracted from `main` so tests can drive a real request through the real
/// layer stack, which is the only way to prove the access log is actually
/// wired in front of the routes rather than merely defined.
fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::PUT,
            Method::POST,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(cors_allowed_request_headers())
        .expose_headers(cors_exposed_upload_headers())
        .max_age(std::time::Duration::from_secs(86400));

    Router::new()
        .route("/upload", put(handle_upload))
        .route("/upload", options(handle_cors_preflight))
        .route("/upload/init", post(handle_resumable_init))
        .route("/upload/init", options(handle_cors_preflight))
        .route(
            "/upload/:upload_id/complete",
            post(handle_resumable_complete),
        )
        .route(
            "/upload/:upload_id/complete",
            options(handle_cors_preflight),
        )
        .route("/upload/:upload_id", delete(handle_resumable_abort))
        .route("/upload/:upload_id", options(handle_cors_preflight))
        .route("/sessions/:upload_id", put(handle_session_chunk))
        .route("/sessions/:upload_id", head(handle_session_head))
        .route("/sessions/:upload_id", options(handle_cors_preflight))
        .route("/migrate", post(handle_migrate))
        .route("/migrate", options(handle_cors_preflight))
        .route("/audit", post(handle_audit_log))
        .route("/transcribe", post(handle_transcribe))
        .route("/transcribe", options(handle_cors_preflight))
        .route("/thumbnail/:hash", get(handle_thumbnail_generate))
        .route("/thumbnail/:hash", options(handle_cors_preflight))
        .route("/", get(handle_landing))
        .route("/", put(handle_upload))
        .route("/", options(handle_cors_preflight))
        .layer(cors)
        .layer(axum::middleware::from_fn(log_request))
        .with_state(state)
}

/// `info`-level filter directive for this service's own logs.
///
/// Derived from the crate name rather than spelled out. A directive naming a
/// crate that does not exist matches nothing, and an `EnvFilter` left with no
/// matching directive falls back to `ERROR` — which silently drops every
/// `info!` and `warn!` the service emits, access-log lines included.
fn service_log_directive() -> Result<Directive> {
    Ok(format!("{}=info", env!("CARGO_CRATE_NAME")).parse()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(service_log_directive()?),
        )
        .init();

    let config = Config::from_env();
    let port = config.port;

    // Initialize GCS client
    let gcs_config = ClientConfig::default().with_auth().await?;
    let gcs_client = GcsClient::new(gcs_config);

    let http_client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .expect("failed to build HTTP client");

    let state = Arc::new(AppState {
        gcs_client,
        config,
        http_client,
        transcribe_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TRANSCRIBE)),
    });

    if state.config.transcriber_url.is_some() && state.config.transcribe_shared_secret.is_none() {
        warn!(
            "TRANSCRIBER_URL is set but TRANSCRIBE_SHARED_SECRET is not; \
             /transcribe requests are forwarded without the shared-secret header \
             and the transcoder is expected to reject them"
        );
    }

    let app = build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("Starting HTTP/2 server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // Use hyper's auto builder which supports both HTTP/1 and HTTP/2
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let app = app.clone();

        tokio::spawn(async move {
            let builder = Builder::new(hyper_util::rt::TokioExecutor::new());
            if let Err(e) = builder
                .serve_connection(
                    io,
                    hyper::service::service_fn(move |req| {
                        let mut app = app.clone();
                        async move { app.call(req).await }
                    }),
                )
                .await
            {
                error!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_landing() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(include_str!("landing.html")))
        .unwrap()
}

async fn handle_cors_preflight() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

fn auth_error_response(error: anyhow::Error) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
        .into_response()
}

fn resumable_error_response(error: resumable::ResumableError) -> Response {
    (
        error.status_code(),
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
        .into_response()
}

async fn collect_body_bytes(body: Body) -> Result<Bytes> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.map_err(|error| anyhow!("Stream error: {}", error))?);
    }
    Ok(Bytes::from(bytes))
}

fn header_value(value: u64) -> HeaderValue {
    HeaderValue::from_str(&value.to_string()).expect("numeric header values must be valid")
}

fn build_session_status_response(status: resumable::UploadSessionStatus) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    response.headers_mut().insert(
        resumable::SESSION_OFFSET_HEADER,
        header_value(status.next_offset),
    );
    response.headers_mut().insert(
        resumable::SESSION_LENGTH_HEADER,
        header_value(status.declared_size),
    );
    let expires_at = HeaderValue::from_str(&status.expires_at)
        .expect("session expiry header must be valid ASCII");
    response
        .headers_mut()
        .insert(resumable::SESSION_EXPIRES_AT_HEADER, expires_at.clone());
    response
        .headers_mut()
        .insert(resumable::SESSION_EXPIRES_HEADER, expires_at);
    response.headers_mut().insert(
        resumable::SESSION_CHUNK_SIZE_HEADER,
        header_value(status.chunk_size),
    );
    response
}

/// POST /audit - Receive audit log entries from Fastly edge and write as structured logs.
/// Google Cloud container logging: JSON on stdout is auto-ingested by Cloud Logging.
/// This gives us: queryable logs, retention policies, export to BigQuery, alerting.
async fn handle_audit_log(body: axum::body::Bytes) -> impl IntoResponse {
    // Parse and re-emit as structured log with severity
    match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(mut entry) => {
            // Add Cloud Logging severity field for proper log level
            entry["severity"] = serde_json::json!("NOTICE");
            entry["logging.googleapis.com/labels"] = serde_json::json!({
                "service": "divine-blossom",
                "component": "audit"
            });
            // Print as JSON to stdout so the platform log collector preserves structure.
            println!("{}", entry);
            StatusCode::OK
        }
        Err(e) => {
            error!("Invalid audit log entry: {}", e);
            StatusCode::BAD_REQUEST
        }
    }
}

async fn handle_upload(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Response {
    match process_upload(state, headers, body).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            error!("Upload error: {}", e);
            let status = if e.to_string().contains("auth") || e.to_string().contains("Auth") {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response()
        }
    }
}

fn resumable_manager(
    state: &AppState,
) -> resumable::ResumableManager<resumable::GcsResumableBackend, resumable::GcsSessionStore> {
    resumable::ResumableManager::new(
        resumable::GcsResumableBackend::new(
            state.gcs_client.clone(),
            state.config.gcs_bucket.clone(),
        ),
        resumable::GcsSessionStore::new(state.gcs_client.clone(), state.config.gcs_bucket.clone()),
        state.config.upload_base_url.clone(),
        state.config.cdn_base_url.clone(),
        state.config.resumable_chunk_size,
        state.config.resumable_session_ttl_secs,
    )
}

async fn handle_resumable_init(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<resumable::ResumableUploadInitRequest>,
) -> Response {
    let auth_event = match validate_auth(&headers, "upload") {
        Ok(event) => event,
        Err(error) => return auth_error_response(error),
    };

    if let Some(expected_hash) = get_tag_value(&auth_event.tags, "x") {
        if expected_hash.to_lowercase() != request.sha256.to_lowercase() {
            return resumable_error_response(resumable::ResumableError::BadRequest(
                "Declared sha256 does not match Blossom auth hash tag".to_string(),
            ));
        }
    }

    let manager = resumable_manager(state.as_ref());
    match manager.init_session(&auth_event.pubkey, request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => resumable_error_response(error),
    }
}

async fn handle_session_head(
    State(state): State<Arc<AppState>>,
    Path(upload_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let manager = resumable_manager(state.as_ref());
    match manager
        .head_session(
            &upload_id,
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
        )
        .await
    {
        Ok(status) => build_session_status_response(status),
        Err(error) => resumable_error_response(error),
    }
}

async fn handle_session_chunk(
    State(state): State<Arc<AppState>>,
    Path(upload_id): Path<String>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Response {
    let content_range = match headers
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => value.to_string(),
        None => {
            return resumable_error_response(resumable::ResumableError::BadRequest(
                "Content-Range header required".to_string(),
            ))
        }
    };

    let chunk = match collect_body_bytes(body).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return resumable_error_response(resumable::ResumableError::BadRequest(format!(
                "Failed to read request body: {}",
                error
            )))
        }
    };

    let manager = resumable_manager(state.as_ref());
    match manager
        .upload_chunk(
            &upload_id,
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            &content_range,
            chunk,
        )
        .await
    {
        Ok(status) => build_session_status_response(status),
        Err(error) => resumable_error_response(error),
    }
}

async fn handle_resumable_complete(
    State(state): State<Arc<AppState>>,
    Path(upload_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let auth_event = match validate_auth(&headers, "upload") {
        Ok(event) => event,
        Err(error) => return auth_error_response(error),
    };

    let manager = resumable_manager(state.as_ref());
    match manager
        .complete_session(&upload_id, &auth_event.pubkey)
        .await
    {
        Ok(response) => {
            spawn_nsfw_scan(
                &state,
                &response.sha256,
                &response.content_type,
                response.newly_stored,
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(error) => resumable_error_response(error),
    }
}

async fn handle_resumable_abort(
    State(state): State<Arc<AppState>>,
    Path(upload_id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let manager = resumable_manager(state.as_ref());
    match manager
        .abort_session(
            &upload_id,
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
        )
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => resumable_error_response(error),
    }
}

async fn process_upload(
    state: Arc<AppState>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<UploadResponse> {
    // Validate auth
    let auth_event = validate_auth(&headers, "upload")?;

    // Get content type
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // Stream body while hashing (with owner metadata for durability)
    let (sha256_hash, size, all_bytes, newly_stored) = stream_to_gcs_with_hash(
        &state.gcs_client,
        &state.config.gcs_bucket,
        &content_type,
        body,
        &auth_event.pubkey,
    )
    .await?;

    // Extract thumbnail for videos (non-blocking - failures don't fail the upload)
    let thumbnail_url = if thumbnail::is_video_type(&content_type) {
        match extract_and_upload_thumbnail(
            &state.gcs_client,
            &state.config.gcs_bucket,
            &state.config.cdn_base_url,
            &sha256_hash,
            &all_bytes,
        )
        .await
        {
            Ok(url) => {
                info!("Generated thumbnail for {}", sha256_hash);
                Some(url)
            }
            Err(e) => {
                error!("Thumbnail extraction failed for {}: {}", sha256_hash, e);
                None
            }
        }
    } else {
        None
    };

    // Probe video dimensions (non-blocking - failures don't fail the upload)
    let dim = if thumbnail::is_video_type(&content_type) {
        match probe_video_dimensions(&all_bytes).await {
            Ok(d) => {
                info!("Probed video dimensions for {}: {}", sha256_hash, d);
                Some(d)
            }
            Err(e) => {
                error!("Video probe failed for {}: {}", sha256_hash, e);
                None
            }
        }
    } else {
        None
    };

    // Trigger HLS transcoding for videos (fire-and-forget)
    if thumbnail::is_video_type(&content_type) {
        if let Some(ref transcoder_url) = state.config.transcoder_url {
            // Spawn background task to trigger transcoder - don't block upload response
            let transcoder_url = transcoder_url.clone();
            let hash = sha256_hash.clone();
            let owner = auth_event.pubkey.clone();
            tokio::spawn(async move {
                if let Err(e) = trigger_transcoding(&transcoder_url, &hash, &owner).await {
                    error!("Failed to trigger transcoding for {}: {}", hash, e);
                }
            });
        } else {
            info!(
                "TRANSCODER_URL not configured, skipping HLS transcoding for {}",
                sha256_hash
            );
        }
    }

    // Trigger transcript generation for transcribable media (audio/video)
    if is_transcribable_type(&content_type) {
        if let Some(ref transcriber_url) = state.config.transcriber_url {
            let transcriber_url = transcriber_url.clone();
            let hash = sha256_hash.clone();
            let owner = auth_event.pubkey.clone();
            tokio::spawn(async move {
                if let Err(e) = trigger_transcription(&transcriber_url, &hash, &owner).await {
                    error!("Failed to trigger transcription for {}: {}", hash, e);
                }
            });
        } else {
            info!(
                "TRANSCRIBER_URL not configured, skipping transcript generation for {}",
                sha256_hash
            );
        }
    }

    spawn_nsfw_scan(&state, &sha256_hash, &content_type, newly_stored);

    // Build response
    let extension = get_extension(&content_type);
    let uploaded = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Ok(UploadResponse {
        sha256: sha256_hash.clone(),
        size,
        content_type,
        uploaded,
        url: format!(
            "{}/{}.{}",
            state.config.cdn_base_url, sha256_hash, extension
        ),
        thumbnail_url,
        dim,
    })
}

async fn stream_to_gcs_with_hash(
    client: &GcsClient,
    bucket: &str,
    content_type: &str,
    body: Body,
    owner: &str,
) -> Result<(String, u64, Vec<u8>, bool)> {
    let mut original_bytes = Vec::new();

    // Collect body stream first; original bytes remain the source of truth for hashing/storage.
    let mut stream = body.into_data_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| anyhow!("Stream error: {}", e))?;
        original_bytes.extend_from_slice(&chunk);
    }

    // Keep original bytes immutable for hash/storage integrity.
    // Derivative generation (thumbnail/probe/transcode) can use sanitized bytes.
    let mut derivative_bytes = original_bytes.clone();

    // Sanitize video bytes for derivative processing only.
    if thumbnail::is_video_type(content_type) {
        match sanitize_video(&derivative_bytes).await {
            Ok(sanitized) => {
                info!(
                    "Prepared sanitized derivative bytes: {} -> {} bytes",
                    derivative_bytes.len(),
                    sanitized.len(),
                );
                derivative_bytes = sanitized;
            }
            Err(e) => {
                // Non-fatal: keep original bytes for derivative processing if sanitization fails
                error!(
                    "Video sanitization failed for derivatives, using original: {}",
                    e
                );
            }
        }
    }

    // Hash and store the original uploaded bytes.
    let mut hasher = Sha256::new();
    hasher.update(&original_bytes);
    let total_size = original_bytes.len() as u64;
    let sha256_hash = hex::encode(hasher.finalize());

    // Check if blob already exists
    let exists = client
        .get_object(
            &google_cloud_storage::http::objects::get::GetObjectRequest {
                bucket: bucket.to_string(),
                object: sha256_hash.clone(),
                ..Default::default()
            },
        )
        .await
        .is_ok();

    if exists {
        info!("Blob {} already exists, skipping upload", sha256_hash);
        return Ok((sha256_hash, total_size, derivative_bytes, false));
    }

    // Upload to GCS
    let upload_type = UploadType::Simple(Media::new(sha256_hash.clone()));
    let req = UploadObjectRequest {
        bucket: bucket.to_string(),
        ..Default::default()
    };

    client
        .upload_object(&req, Bytes::from(original_bytes.clone()), &upload_type)
        .await
        .map_err(|e| anyhow!("GCS upload failed: {}", e))?;

    // Set content type and owner metadata for durability
    let mut metadata_map = std::collections::HashMap::new();
    metadata_map.insert("owner".to_string(), owner.to_string());

    let update_req = google_cloud_storage::http::objects::patch::PatchObjectRequest {
        bucket: bucket.to_string(),
        object: sha256_hash.clone(),
        metadata: Some(Object {
            content_type: Some(content_type.to_string()),
            metadata: Some(metadata_map),
            ..Default::default()
        }),
        ..Default::default()
    };
    let _ = client.patch_object(&update_req).await;

    info!(
        "Uploaded {} bytes as {} (owner: {})",
        total_size, sha256_hash, owner
    );
    Ok((sha256_hash, total_size, derivative_bytes, true))
}

#[derive(Debug, Serialize)]
struct DetectorRequest<'a> {
    url: &'a str,
    mime_type: &'a str,
    sha256: &'a str,
    signals: [&'static str; 1],
}

/// Start one evidence-only NSFW scan after a new video is durably stored.
///
/// Repeated direct uploads and repeated resumable completion calls do not
/// resubmit the same content. The detector computes the hash again before it
/// publishes evidence, so the upload server's claim is never trusted blindly.
fn spawn_nsfw_scan(state: &Arc<AppState>, sha256: &str, content_type: &str, newly_stored: bool) {
    if !newly_stored || !thumbnail::is_video_type(content_type) {
        return;
    }
    let Some(detector_base_url) = state.config.ai_detector_base_url.clone() else {
        info!(
            "AI_DETECTOR_BASE_URL not configured, skipping NSFW scan for {}",
            sha256
        );
        return;
    };

    let video_url = format!(
        "{}/{}",
        state.config.cdn_base_url.trim_end_matches('/'),
        sha256
    );
    let hash = sha256.to_string();
    let mime_type = content_type.to_string();
    let client = state.http_client.clone();

    tokio::spawn(async move {
        match trigger_nsfw_scan(&client, &detector_base_url, &video_url, &hash, &mime_type).await {
            Ok(signal_state) => info!("NSFW scan for {} returned {}", hash, signal_state),
            Err(error) => warn!("NSFW scan failed for {}: {}", hash, error),
        }
    });
}

async fn trigger_nsfw_scan(
    client: &reqwest::Client,
    detector_base_url: &str,
    video_url: &str,
    sha256: &str,
    mime_type: &str,
) -> Result<String> {
    let response = client
        .post(format!(
            "{}/detect",
            detector_base_url.trim_end_matches('/')
        ))
        .json(&DetectorRequest {
            url: video_url,
            mime_type,
            sha256,
            signals: ["nsfw"],
        })
        .send()
        .await
        .map_err(|error| anyhow!("detector request failed: {}", error))?;

    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("detector returned HTTP {}", status));
    }

    let payload: serde_json::Value = response
        .json()
        .await
        .map_err(|error| anyhow!("detector returned invalid JSON: {}", error))?;
    let signal_state = payload
        .pointer("/signals/nsfw/state")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("detector response omitted signals.nsfw.state"))?;

    match signal_state {
        // `skipped` is the detector's documented answer for a signal whose
        // model is not configured. It is explicitly not an error there, so it
        // must not be reported as a failed scan here.
        "detected" | "absent" | "skipped" => Ok(signal_state.to_string()),
        other => Err(anyhow!("detector NSFW signal returned {}", other)),
    }
}

/// Extract thumbnail from video and upload to GCS
/// Returns the thumbnail URL on success
async fn extract_and_upload_thumbnail(
    client: &GcsClient,
    bucket: &str,
    cdn_base_url: &str,
    hash: &str,
    video_data: &[u8],
) -> Result<String> {
    // Extract thumbnail using ffmpeg
    let thumb_result = thumbnail::extract_thumbnail(video_data)?;

    // Upload thumbnail to GCS with path: {hash}.jpg (same as video hash but with .jpg extension)
    // This allows serving via CDN at media.divine.video/{hash}.jpg
    let thumb_path = format!("{}.jpg", hash);

    let mut media = Media::new(thumb_path.clone());
    media.content_type = "image/jpeg".into();
    let upload_type = UploadType::Simple(media);
    let req = UploadObjectRequest {
        bucket: bucket.to_string(),
        ..Default::default()
    };

    client
        .upload_object(&req, Bytes::from(thumb_result.data), &upload_type)
        .await
        .map_err(|e| anyhow!("GCS thumbnail upload failed: {}", e))?;

    // Return CDN URL for thumbnail - stored at {hash}.jpg, served via CDN
    Ok(format!("{}/{}.jpg", cdn_base_url, hash))
}

fn media_source_candidates(hash: &str) -> [String; 3] {
    [
        hash.to_string(),
        format!("{}/hls/stream_720p.ts", hash),
        format!("{}/hls/stream_480p.ts", hash),
    ]
}

async fn download_best_available_media_bytes(
    client: &GcsClient,
    bucket: &str,
    hash: &str,
) -> Result<(Vec<u8>, String)> {
    let mut failures = Vec::new();

    for object in media_source_candidates(hash) {
        match client
            .download_object(
                &GetObjectRequest {
                    bucket: bucket.to_string(),
                    object: object.clone(),
                    ..Default::default()
                },
                &DownloadRange::default(),
            )
            .await
        {
            Ok(data) => {
                if object == hash {
                    return Ok((data, object));
                }

                warn!(
                    "Original blob missing for {}, using fallback media source {} for thumbnail generation",
                    hash, object
                );
                return Ok((data, object));
            }
            Err(e) => failures.push(format!("{}: {}", object, e)),
        }
    }

    Err(anyhow!(
        "No recoverable media source found for {} ({})",
        hash,
        failures.join(" | ")
    ))
}

/// On-demand thumbnail generation endpoint
/// Downloads video from GCS, generates thumbnail, stores it, returns the image
async fn handle_thumbnail_generate(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Validate hash format (64 hex characters)
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "application/json")],
            Json(ErrorResponse {
                error: "Invalid hash format".to_string(),
            })
            .into_response(),
        )
            .into_response();
    }

    let hash = hash.to_lowercase();

    // First check if thumbnail already exists
    let thumb_path = format!("{}.jpg", hash);
    let thumb_exists = state
        .gcs_client
        .get_object(&GetObjectRequest {
            bucket: state.config.gcs_bucket.clone(),
            object: thumb_path.clone(),
            ..Default::default()
        })
        .await
        .is_ok();

    if thumb_exists {
        // Thumbnail already exists, download and return it
        match state
            .gcs_client
            .download_object(
                &GetObjectRequest {
                    bucket: state.config.gcs_bucket.clone(),
                    object: thumb_path,
                    ..Default::default()
                },
                &DownloadRange::default(),
            )
            .await
        {
            Ok(data) => {
                return (StatusCode::OK, [(header::CONTENT_TYPE, "image/jpeg")], data)
                    .into_response();
            }
            Err(e) => {
                error!("Failed to download existing thumbnail: {}", e);
            }
        }
    }

    // Download the original blob, or fall back to the best available HLS transport stream.
    let (video_data, source_object) = match download_best_available_media_bytes(
        &state.gcs_client,
        &state.config.gcs_bucket,
        &hash,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to download video {}: {}", hash, e);
            return (
                StatusCode::NOT_FOUND,
                [(header::CONTENT_TYPE, "application/json")],
                Json(ErrorResponse {
                    error: "Video not found".to_string(),
                })
                .into_response(),
            )
                .into_response();
        }
    };

    if source_object != hash {
        warn!(
            "Generating thumbnail for {} from fallback source {}",
            hash, source_object
        );
    }

    // Generate thumbnail
    let thumb_result = match thumbnail::extract_thumbnail(&video_data) {
        Ok(result) => result,
        Err(e) => {
            error!("Failed to generate thumbnail for {}: {}", hash, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                Json(ErrorResponse {
                    error: "Failed to generate thumbnail".to_string(),
                })
                .into_response(),
            )
                .into_response();
        }
    };

    // Upload thumbnail to GCS
    let thumb_path = format!("{}.jpg", hash);
    let mut media = Media::new(thumb_path.clone());
    media.content_type = "image/jpeg".into();
    let upload_type = UploadType::Simple(media);
    let req = UploadObjectRequest {
        bucket: state.config.gcs_bucket.clone(),
        ..Default::default()
    };

    if let Err(e) = state
        .gcs_client
        .upload_object(&req, Bytes::from(thumb_result.data.clone()), &upload_type)
        .await
    {
        error!("Failed to upload thumbnail for {}: {}", hash, e);
        // Still return the thumbnail even if upload failed
    }

    info!("Generated on-demand thumbnail for {}", hash);

    // Return the thumbnail image
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/jpeg")],
        thumb_result.data,
    )
        .into_response()
}

fn new_temp_media_path(suffix: &str) -> Result<tempfile::TempPath> {
    NamedTempFile::with_suffix(suffix)
        .map(|file| file.into_temp_path())
        .map_err(|e| anyhow!("Failed to create temp file {}: {}", suffix, e))
}

/// Sanitize a video file by remuxing with ffmpeg
/// This strips invalid MP4 atoms (e.g. malformed clap boxes from iPhone),
/// ensures faststart (moov before mdat), and produces a web-compatible MP4.
/// Uses -c copy so it's lossless and fast (no re-encoding).
async fn sanitize_video(input_bytes: &[u8]) -> Result<Vec<u8>> {
    use tokio::process::Command;

    let input_path = new_temp_media_path(".mp4")?;
    let output_path = new_temp_media_path(".mp4")?;

    // Write input to temp file
    tokio::fs::write(&input_path, input_bytes)
        .await
        .map_err(|e| anyhow!("Failed to write temp input: {}", e))?;

    // Remux with ffmpeg: -c copy (no re-encode), +faststart (moov at front)
    let output = Command::new("ffmpeg")
        .args([
            "-y", // Overwrite output
            "-v",
            "warning", // Only show warnings/errors
            "-i",
            input_path.to_str().unwrap(),
            "-c",
            "copy", // Copy streams without re-encoding
            "-movflags",
            "+faststart", // Put moov atom at front
            output_path.to_str().unwrap(),
        ])
        .output()
        .await
        .map_err(|e| anyhow!("Failed to run ffmpeg: {}", e))?;

    // Clean up input
    let _ = input_path.close();

    if !output.status.success() {
        let _ = output_path.close();
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffmpeg sanitize failed: {}", stderr));
    }

    // Read sanitized output
    let sanitized = tokio::fs::read(&output_path)
        .await
        .map_err(|e| anyhow!("Failed to read sanitized output: {}", e))?;

    // Clean up output
    let _ = output_path.close();

    Ok(sanitized)
}

/// Probe video data with ffprobe to get display dimensions (respecting rotation metadata).
/// Returns "WIDTHxHEIGHT" string suitable for the Nostr `dim` imeta tag.
async fn probe_video_dimensions(video_bytes: &[u8]) -> Result<String> {
    use tokio::process::Command;

    let probe_path = new_temp_media_path(".mp4")?;

    // Write to temp file for ffprobe
    tokio::fs::write(&probe_path, video_bytes)
        .await
        .map_err(|e| anyhow!("Failed to write temp file for probe: {}", e))?;

    let output = Command::new("ffprobe")
        .args([
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            "-select_streams",
            "v:0",
            probe_path.to_str().unwrap(),
        ])
        .output()
        .await
        .map_err(|e| anyhow!("Failed to run ffprobe: {}", e))?;

    // Clean up temp file
    let _ = probe_path.close();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffprobe failed: {}", stderr));
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| anyhow!("Failed to parse ffprobe output: {}", e))?;

    let stream = json["streams"]
        .as_array()
        .and_then(|s| s.first())
        .ok_or_else(|| anyhow!("No video stream found"))?;

    let width = stream["width"].as_u64().unwrap_or(0) as u32;
    let height = stream["height"].as_u64().unwrap_or(0) as u32;

    if width == 0 || height == 0 {
        return Err(anyhow!("Could not determine video dimensions"));
    }

    // Check rotation from tags (older FFmpeg / older files)
    let mut rotation: i32 = stream["tags"]["rotate"]
        .as_str()
        .and_then(|r| r.parse().ok())
        .unwrap_or(0);

    // Check side_data_list for Display Matrix rotation (newer FFmpeg)
    if rotation == 0 {
        if let Some(side_data) = stream["side_data_list"].as_array() {
            for sd in side_data {
                if sd["side_data_type"].as_str() == Some("Display Matrix") {
                    if let Some(r) = sd["rotation"].as_f64() {
                        rotation = r.round() as i32;
                    } else if let Some(r) =
                        sd["rotation"].as_str().and_then(|s| s.parse::<f64>().ok())
                    {
                        rotation = r.round() as i32;
                    }
                }
            }
        }
    }

    let rotation_abs = rotation.unsigned_abs() % 360;

    // Compute display dimensions (after applying rotation)
    let (display_width, display_height) = if rotation_abs == 90 || rotation_abs == 270 {
        (height, width)
    } else {
        (width, height)
    };

    Ok(format!("{}x{}", display_width, display_height))
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        build_router, build_session_status_response, compute_event_id,
        cors_allowed_request_headers, cors_exposed_upload_headers, decode_auth_event,
        get_extension, handle_transcribe, is_transcribable_type, log_request,
        media_source_candidates, new_temp_media_path, proxy_transcribe_audio,
        resolve_resumable_chunk_size, server_tag_host, service_log_directive, transcribe_audio_url,
        transcribe_server_tag_allowed, trigger_nsfw_scan, AppState, Config, GcsClient, NostrEvent,
        TranscribeParams, BLOSSOM_AUTH_KIND, TRANSCRIBE_SHED_RETRY_AFTER,
    };
    use crate::request_log;
    use crate::resumable;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{header, HeaderValue, Request, StatusCode};
    use axum::{routing::get, routing::post, Json, Router};
    use google_cloud_storage::client::ClientConfig;
    use k256::schnorr::{signature::hazmat::PrehashSigner, SigningKey};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;
    use tracing_subscriber::EnvFilter;

    fn test_state(transcriber_url: Option<&str>, transcribe_slots: usize) -> Arc<AppState> {
        Arc::new(AppState {
            gcs_client: GcsClient::new(ClientConfig::default().anonymous()),
            config: Config {
                gcs_bucket: "test-bucket".to_string(),
                cdn_base_url: "https://media.divine.video".to_string(),
                upload_base_url: "https://upload.divine.video".to_string(),
                port: 0,
                migration_nsec: None,
                transcoder_url: None,
                transcriber_url: transcriber_url.map(str::to_string),
                ai_detector_base_url: None,
                transcribe_shared_secret: Some("secret".to_string()),
                resumable_session_ttl_secs: resumable::DEFAULT_RESUMABLE_SESSION_TTL_SECS,
                resumable_chunk_size: resumable::DEFAULT_RESUMABLE_CHUNK_SIZE,
            },
            http_client: reqwest::Client::new(),
            transcribe_slots: Arc::new(tokio::sync::Semaphore::new(transcribe_slots)),
        })
    }

    /// Collects everything the `tracing` subscriber writes, so a test can
    /// assert on the log line a request actually produced.
    #[derive(Clone, Default)]
    struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl LogCapture {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
        type Writer = LogCapture;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Subscribe with the service's own directive and an empty `RUST_LOG`,
    /// which is how the deployed container runs.
    fn capture_service_logs() -> (LogCapture, tracing::subscriber::DefaultGuard) {
        let capture = LogCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("").add_directive(service_log_directive().unwrap()))
            .with_writer(capture.clone())
            .finish();

        (
            capture.clone(),
            tracing::subscriber::set_default(subscriber),
        )
    }

    #[test]
    fn the_service_log_directive_lets_this_crates_info_lines_through() {
        // A directive naming a crate that does not exist matches nothing, and
        // `EnvFilter` then falls back to ERROR — which mutes every access-log
        // line the service emits. Deriving the target from the crate name is
        // only worth anything if it actually resolves, so pin that here.
        let (capture, _guard) = capture_service_logs();

        tracing::info!("info level reaches the subscriber");

        assert!(
            capture
                .contents()
                .contains("info level reaches the subscriber"),
            "expected an info line, got: {}",
            capture.contents()
        );
    }

    #[tokio::test]
    async fn a_request_through_the_router_logs_the_edge_correlation_id() {
        // The formatting functions are unit-tested in `request_log`; what this
        // covers is the wiring — that the layer sits in front of the routes and
        // reads the correlation ID, method, path, and final status off a real
        // request/response pair.
        let (capture, _guard) = capture_service_logs();

        let response = build_router(test_state(None, 1))
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .header(request_log::REQUEST_ID_HEADER, "edge-id-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let logged = capture.contents();
        assert!(
            logged.contains("[REQUEST] req_id=edge-id-123"),
            "expected the edge correlation ID, got: {logged}"
        );
        assert!(logged.contains("method=GET"), "got: {logged}");
        assert!(logged.contains("path=/"), "got: {logged}");
        assert!(logged.contains("status=200"), "got: {logged}");
    }

    #[tokio::test]
    async fn the_logged_status_is_the_response_status() {
        // Guards against a line that always reports 200: an unmatched route is
        // the cheapest response this service produces that is not a success,
        // and it also shows the layer covers requests no route handles.
        let (capture, _guard) = capture_service_logs();

        let response = build_router(test_state(None, 1))
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/no-such-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(
            capture.contents().contains("status=404"),
            "got: {}",
            capture.contents()
        );
    }

    #[tokio::test]
    async fn a_request_without_an_edge_correlation_id_still_logs() {
        // Resumable chunk appends go straight to this service and carry no edge
        // header. They are the one path with no edge record, so an origin
        // record is all there is.
        let (capture, _guard) = capture_service_logs();

        build_router(test_state(None, 1))
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(
            capture.contents().contains("[REQUEST] req_id=-"),
            "got: {}",
            capture.contents()
        );
    }

    #[tokio::test]
    async fn an_abandoned_request_is_still_logged() {
        // What the edge giving up at its timeout looks like from here: the
        // future is dropped mid-flight and code after the `await` never runs.
        // Without a line for these, the requests under investigation are the
        // only ones leaving no origin record, and "no line" could not tell
        // "origin never saw it" from "origin was still working on it".
        let (capture, _guard) = capture_service_logs();

        let app = Router::new()
            .route(
                "/never-completes",
                get(|| async {
                    std::future::pending::<()>().await;
                    StatusCode::OK
                }),
            )
            .layer(axum::middleware::from_fn(log_request));

        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/never-completes")
                    .header(request_log::REQUEST_ID_HEADER, "abandoned-id")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await;

        assert!(outcome.is_err(), "the request should not have completed");

        let logged = capture.contents();
        assert!(logged.contains("req_id=abandoned-id"), "got: {logged}");
        assert!(logged.contains("status=-"), "got: {logged}");
        assert!(logged.contains("path=/never-completes"), "got: {logged}");
    }

    #[test]
    fn extension_matching_ignores_case_and_parameters() {
        // The extension lands in the response `url`, which is published as the
        // Blossom descriptor, so a video that took the video path must not be
        // handed back as `.bin`.
        assert_eq!(get_extension("video/mp4"), "mp4");
        assert_eq!(get_extension("VIDEO/MP4"), "mp4");
        assert_eq!(get_extension("video/mp4;codecs=\"avc1.42E01E\""), "mp4");
        assert_eq!(get_extension("Video/QuickTime"), "mov");
        assert_eq!(get_extension("IMAGE/PNG"), "png");
        assert_eq!(get_extension("audio/mpeg; rate=44100"), "mp3");
    }

    #[test]
    fn undeclared_types_still_get_the_fallback_extension() {
        assert_eq!(get_extension("application/octet-stream"), "bin");
        assert_eq!(get_extension(""), "bin");
        assert_eq!(get_extension(";codecs=avc1"), "bin");
    }

    #[test]
    fn transcribable_matching_ignores_case_and_parameters() {
        assert!(is_transcribable_type("video/mp4"));
        assert!(is_transcribable_type("VIDEO/MP4"));
        assert!(is_transcribable_type("Audio/MPEG"));
        assert!(is_transcribable_type("audio/ogg; codecs=opus"));
        assert!(!is_transcribable_type("image/png"));
        assert!(!is_transcribable_type("application/octet-stream"));
        assert!(!is_transcribable_type(""));
    }

    #[tokio::test]
    async fn detector_request_uses_verified_hash_url_and_nsfw_only() {
        let (payload_tx, mut payload_rx) = tokio::sync::mpsc::channel(1);
        let app = Router::new().route(
            "/detect",
            post(move |Json(payload): Json<serde_json::Value>| {
                let payload_tx = payload_tx.clone();
                async move {
                    payload_tx.send(payload).await.expect("capture request");
                    Json(serde_json::json!({
                        "signals": { "nsfw": { "state": "absent" } }
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind detector fixture");
        let address = listener.local_addr().expect("fixture address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve detector fixture");
        });

        let hash = "5322a546ac6f5f79f70050a2550cda08a7070ddd531a15a7d81cab992d6fd600";
        let state = trigger_nsfw_scan(
            &reqwest::Client::new(),
            &format!("http://{address}/"),
            &format!("https://media.divine.video/{hash}"),
            hash,
            "video/mp4",
        )
        .await
        .expect("detector scan");

        assert_eq!(state, "absent");
        let payload = payload_rx.recv().await.expect("captured request");
        assert_eq!(payload["url"], format!("https://media.divine.video/{hash}"));
        assert_eq!(payload["sha256"], hash);
        assert_eq!(payload["mime_type"], "video/mp4");
        assert_eq!(payload["signals"], serde_json::json!(["nsfw"]));
    }

    /// Serve one canned `/detect` response and return the fixture's base URL.
    async fn detector_fixture(state: &'static str) -> String {
        let app = Router::new().route(
            "/detect",
            post(move || async move {
                Json(serde_json::json!({
                    "signals": { "nsfw": { "state": state } }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind detector fixture");
        let address = listener.local_addr().expect("fixture address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve detector fixture");
        });
        format!("http://{address}")
    }

    async fn scan_against(base_url: &str) -> anyhow::Result<String> {
        let hash = "5322a546ac6f5f79f70050a2550cda08a7070ddd531a15a7d81cab992d6fd600";
        trigger_nsfw_scan(
            &reqwest::Client::new(),
            base_url,
            &format!("https://media.divine.video/{hash}"),
            hash,
            "video/mp4",
        )
        .await
    }

    #[tokio::test]
    async fn unconfigured_detector_model_is_not_a_failed_scan() {
        // The detector answers `skipped` when a signal's model is not
        // configured, and documents that as explicitly not an error. Folding it
        // into the error arm would log a failed scan for every single upload.
        let base_url = detector_fixture("skipped").await;

        assert_eq!(
            scan_against(&base_url)
                .await
                .expect("skipped is not an error"),
            "skipped"
        );
    }

    #[tokio::test]
    async fn detector_signal_error_is_reported_as_a_failure() {
        let base_url = detector_fixture("error").await;

        let message = scan_against(&base_url)
            .await
            .expect_err("error state must surface as a failure")
            .to_string();
        assert!(message.contains("error"), "unexpected message: {message}");
    }

    fn transcribe_auth_header(extra_tags: Vec<Vec<String>>) -> axum::http::HeaderMap {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]).expect("test signing key");
        let pubkey = hex::encode(signing_key.verifying_key().to_bytes());
        let mut tags = vec![vec!["t".to_string(), "media".to_string()]];
        tags.extend(extra_tags);
        let mut event = NostrEvent {
            id: String::new(),
            pubkey,
            created_at: 1,
            kind: BLOSSOM_AUTH_KIND,
            tags,
            content: String::new(),
            sig: String::new(),
        };
        event.id = compute_event_id(&event).expect("compute test event id");
        let id_bytes = hex::decode(&event.id).expect("hex event id");
        event.sig = hex::encode(
            signing_key
                .sign_prehash(&id_bytes)
                .expect("sign test event")
                .to_bytes(),
        );

        let event_json = serde_json::to_string(&serde_json::json!({
            "id": event.id,
            "pubkey": event.pubkey,
            "created_at": event.created_at,
            "kind": event.kind,
            "tags": event.tags,
            "content": event.content,
            "sig": event.sig,
        }))
        .expect("serialize test event");
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, event_json);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Nostr {encoded}"))
                .expect("auth header value"),
        );
        headers
    }

    fn future_expiration_tag() -> Vec<String> {
        let expiration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_secs()
            + 300;
        vec!["expiration".to_string(), expiration.to_string()]
    }

    #[test]
    fn transcribe_audio_url_appends_path_and_trims_trailing_slash() {
        assert_eq!(
            transcribe_audio_url("https://transcoder.example"),
            "https://transcoder.example/transcribe/audio"
        );
        assert_eq!(
            transcribe_audio_url("https://transcoder.example/"),
            "https://transcoder.example/transcribe/audio"
        );
    }

    #[test]
    fn temp_media_paths_are_unique_per_request() {
        let first = new_temp_media_path(".mp4").expect("first temp path");
        let second = new_temp_media_path(".mp4").expect("second temp path");
        let first_path = first.to_string_lossy().to_string();
        let second_path = second.to_string_lossy().to_string();

        assert_ne!(first_path, second_path);
        assert!(first_path.ends_with(".mp4"));
        assert!(second_path.ends_with(".mp4"));
    }

    #[test]
    fn media_source_candidates_prefer_original_then_hls_variants() {
        let hash = "5b48aa1fcf30af61243ac9307eb98b7fa22df1c58573c3ca5d1b14fc30099929";
        let candidates = media_source_candidates(hash);

        assert_eq!(candidates[0], hash);
        assert_eq!(candidates[1], format!("{}/hls/stream_720p.ts", hash));
        assert_eq!(candidates[2], format!("{}/hls/stream_480p.ts", hash));
    }

    #[test]
    fn session_responses_include_upload_expires_at_header() {
        let response = build_session_status_response(resumable::UploadSessionStatus {
            next_offset: 0,
            declared_size: 1024,
            expires_at: "2026-03-28T00:40:00Z".to_string(),
            chunk_size: 8 * 1024 * 1024,
        });

        assert_eq!(
            response
                .headers()
                .get("Upload-Expires-At")
                .expect("contract expiry header"),
            "2026-03-28T00:40:00Z"
        );
    }

    #[test]
    fn cors_exposes_upload_expires_at_header() {
        assert!(cors_exposed_upload_headers()
            .iter()
            .any(|header| header.as_str() == "upload-expires-at"));
    }

    #[test]
    fn cors_allows_blossom_upload_request_headers() {
        let headers = cors_allowed_request_headers();
        let allowed: Vec<&str> = headers.iter().map(|header| header.as_str()).collect();

        for expected in [
            "authorization",
            "content-type",
            "content-range",
            "x-sha256",
            "x-request-id",
        ] {
            assert!(
                allowed.contains(&expected),
                "missing CORS request header: {expected}",
            );
        }
    }

    #[test]
    fn advertised_chunk_size_is_capped_to_upload_route_body_limit() {
        assert_eq!(
            resolve_resumable_chunk_size(8 * 1024 * 1024, 1024 * 1024),
            1024 * 1024
        );
    }

    #[test]
    fn advertised_chunk_size_keeps_smaller_configured_value() {
        assert_eq!(
            resolve_resumable_chunk_size(512 * 1024, 1024 * 1024),
            512 * 1024
        );
    }

    #[test]
    fn decode_auth_event_accepts_standard_and_url_safe() {
        use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
        use base64::Engine;
        // All-0xff bytes force index-63 chars (`/` vs `_`); five bytes force
        // standard padding that url-safe-unpadded omits — so both encodings
        // differ and both must decode back.
        let raw: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff];
        let standard = STANDARD.encode(raw);
        let url_safe = URL_SAFE_NO_PAD.encode(raw);
        assert_ne!(standard, url_safe);
        assert_eq!(decode_auth_event(&standard).unwrap(), raw);
        assert_eq!(decode_auth_event(&url_safe).unwrap(), raw);
    }

    #[test]
    fn decode_auth_event_rejects_non_base64() {
        assert!(decode_auth_event("@ not base64 @").is_err());
    }

    #[test]
    fn server_tag_host_strips_scheme_port_and_path() {
        assert_eq!(server_tag_host("media.divine.video"), "media.divine.video");
        assert_eq!(
            server_tag_host("https://media.divine.video"),
            "media.divine.video"
        );
        assert_eq!(
            server_tag_host("https://media.divine.video:443/upload"),
            "media.divine.video"
        );
        assert_eq!(
            server_tag_host("HTTPS://Media.Divine.Video/"),
            "media.divine.video"
        );
        // Crafted values must resolve to the real authority, not a decoy tail.
        assert_eq!(
            server_tag_host("https://evil.example/x://media.divine.video"),
            "evil.example"
        );
        assert_eq!(
            server_tag_host("https://media.divine.video:x@evil.com"),
            "evil.com"
        );
    }

    #[test]
    fn transcribe_server_tag_allows_divine_hosts_only() {
        assert!(transcribe_server_tag_allowed("https://media.divine.video"));
        assert!(transcribe_server_tag_allowed("upload.divine.video"));
        assert!(!transcribe_server_tag_allowed("https://evil.example.com"));
        assert!(!transcribe_server_tag_allowed("cdn.divine.video"));
        assert!(!transcribe_server_tag_allowed(
            "https://evil.example/x://media.divine.video"
        ));
        assert!(!transcribe_server_tag_allowed(
            "https://media.divine.video:x@evil.com"
        ));
    }

    #[tokio::test]
    async fn proxy_transcribe_audio_forwards_request_and_returns_response() {
        use axum::routing::post;
        use axum::Router;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Captured {
            content_type: Option<String>,
            secret: Option<String>,
            language: Option<String>,
            body: Vec<u8>,
        }

        async fn capture(
            axum::extract::State(store): axum::extract::State<
                std::sync::Arc<std::sync::Mutex<Option<Captured>>>,
            >,
            headers: axum::http::HeaderMap,
            axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >,
            body: bytes::Bytes,
        ) -> axum::response::Response {
            *store.lock().unwrap() = Some(Captured {
                content_type: headers
                    .get(axum::http::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string),
                secret: headers
                    .get("x-divine-transcribe-secret")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string),
                language: params.get("language").cloned(),
                body: body.to_vec(),
            });
            let mut response = axum::response::Response::new(axum::body::Body::from(
                "WEBVTT\n\n00:00.000 --> 00:01.000\nhi\n",
            ));
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("text/vtt; charset=utf-8"),
            );
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("7"),
            );
            response
        }

        let store: Arc<Mutex<Option<Captured>>> = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route("/transcribe/audio", post(capture))
            .with_state(store.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test server");
        });

        let client = reqwest::Client::new();
        let base = format!("http://{}", addr);
        let audio = bytes::Bytes::from_static(b"RIFF....fake-wav");

        let (status, content_type, retry_after, vtt) = proxy_transcribe_audio(
            &client,
            &base,
            Some("s3cr3t"),
            audio.clone(),
            Some("  en-US  "),
        )
        .await
        .expect("proxy call succeeds");

        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(content_type, "text/vtt; charset=utf-8");
        assert_eq!(retry_after.as_deref(), Some("7"));
        assert!(String::from_utf8_lossy(&vtt).starts_with("WEBVTT"));

        let captured = store
            .lock()
            .unwrap()
            .clone()
            .expect("server captured the forwarded request");
        assert_eq!(captured.content_type.as_deref(), Some("audio/wav"));
        assert_eq!(captured.secret.as_deref(), Some("s3cr3t"));
        // Language is forwarded once, trimmed.
        assert_eq!(captured.language.as_deref(), Some("en-US"));
        assert_eq!(captured.body, audio.to_vec());
    }

    #[tokio::test]
    async fn handle_transcribe_requires_expiration_tag() {
        let response = handle_transcribe(
            State(test_state(Some("http://127.0.0.1:9"), 1)),
            transcribe_auth_header(vec![]),
            axum::extract::Query(TranscribeParams { language: None }),
            Body::from("audio"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn handle_transcribe_rejects_disallowed_server_tag() {
        let response = handle_transcribe(
            State(test_state(Some("http://127.0.0.1:9"), 1)),
            transcribe_auth_header(vec![
                future_expiration_tag(),
                vec!["server".to_string(), "https://evil.example".to_string()],
            ]),
            axum::extract::Query(TranscribeParams { language: None }),
            Body::from("audio"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn handle_transcribe_rejects_empty_body() {
        let response = handle_transcribe(
            State(test_state(Some("http://127.0.0.1:9"), 1)),
            transcribe_auth_header(vec![future_expiration_tag()]),
            axum::extract::Query(TranscribeParams { language: None }),
            Body::empty(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn handle_transcribe_sheds_when_admission_slots_are_exhausted() {
        let response = handle_transcribe(
            State(test_state(Some("http://127.0.0.1:9"), 0)),
            transcribe_auth_header(vec![future_expiration_tag()]),
            axum::extract::Query(TranscribeParams { language: None }),
            Body::from("audio"),
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static(TRANSCRIBE_SHED_RETRY_AFTER))
        );
    }
}

/// Decodes a Blossom auth event, accepting either encoding: BUD-11 specifies
/// URL-safe unpadded Base64, while the current Divine client emits standard
/// padded Base64. Accepting both keeps deployed clients working while allowing
/// compliant `-`/`_`/unpadded tokens.
fn decode_auth_event(encoded: &str) -> Result<Vec<u8>> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    use base64::Engine;
    for engine in [STANDARD, URL_SAFE_NO_PAD, URL_SAFE, STANDARD_NO_PAD] {
        if let Ok(bytes) = engine.decode(encoded) {
            return Ok(bytes);
        }
    }
    Err(anyhow!("Invalid base64 authorization event"))
}

fn validate_auth(headers: &axum::http::HeaderMap, required_action: &str) -> Result<NostrEvent> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .ok_or_else(|| anyhow!("Authorization header required"))?
        .to_str()
        .map_err(|_| anyhow!("Invalid authorization header"))?;

    if !auth_header.starts_with("Nostr ") {
        return Err(anyhow!("Authorization must start with 'Nostr '"));
    }

    let event_json = decode_auth_event(&auth_header[6..])?;

    let event: NostrEvent =
        serde_json::from_slice(&event_json).map_err(|e| anyhow!("Invalid event JSON: {}", e))?;

    validate_event(&event, required_action)?;

    Ok(event)
}

fn validate_event(event: &NostrEvent, required_action: &str) -> Result<()> {
    // Check kind
    if event.kind != BLOSSOM_AUTH_KIND {
        return Err(anyhow!(
            "Invalid event kind: expected {}",
            BLOSSOM_AUTH_KIND
        ));
    }

    // Check action tag
    let action = get_tag_value(&event.tags, "t");
    if action.as_deref() != Some(required_action) {
        return Err(anyhow!(
            "Action mismatch: expected {}, got {:?}",
            required_action,
            action
        ));
    }

    // Check expiration
    if let Some(expiration) = get_tag_value(&event.tags, "expiration") {
        let exp: i64 = expiration.parse().unwrap_or(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if now > exp {
            return Err(anyhow!("Authorization expired"));
        }
    }

    // Verify event ID
    let computed_id = compute_event_id(event)?;
    if computed_id != event.id {
        return Err(anyhow!("Invalid event ID"));
    }

    // Verify signature
    verify_signature(event)?;

    Ok(())
}

fn get_tag_value(tags: &[Vec<String>], tag_name: &str) -> Option<String> {
    tags.iter()
        .find(|tag| tag.len() >= 2 && tag[0] == tag_name)
        .map(|tag| tag[1].clone())
}

fn compute_event_id(event: &NostrEvent) -> Result<String> {
    let serialized = serde_json::to_string(&(
        0,
        &event.pubkey,
        event.created_at,
        event.kind,
        &event.tags,
        &event.content,
    ))
    .map_err(|e| anyhow!("Serialization error: {}", e))?;

    let mut hasher = Sha256::new();
    hasher.update(serialized.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

fn verify_signature(event: &NostrEvent) -> Result<()> {
    let pubkey_bytes = hex::decode(&event.pubkey).map_err(|_| anyhow!("Invalid pubkey hex"))?;
    let sig_bytes = hex::decode(&event.sig).map_err(|_| anyhow!("Invalid signature hex"))?;
    let msg_bytes = hex::decode(&event.id).map_err(|_| anyhow!("Invalid event ID hex"))?;

    // Convert Vec<u8> to [u8; 32] for pubkey
    let pubkey_array: [u8; 32] = pubkey_bytes
        .try_into()
        .map_err(|_| anyhow!("Invalid pubkey length"))?;

    let verifying_key =
        VerifyingKey::from_bytes(&pubkey_array).map_err(|e| anyhow!("Invalid pubkey: {}", e))?;

    let signature = Signature::try_from(sig_bytes.as_slice())
        .map_err(|e| anyhow!("Invalid signature: {}", e))?;

    // Use verify_prehash since the event ID is already a SHA-256 hash
    verifying_key
        .verify_prehash(&msg_bytes, &signature)
        .map_err(|_| anyhow!("Invalid signature"))?;

    Ok(())
}

fn get_extension(content_type: &str) -> &'static str {
    match media_type::normalize(content_type).as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/quicktime" => "mov",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "application/pdf" => "pdf",
        _ => "bin",
    }
}

fn is_transcribable_type(content_type: &str) -> bool {
    let normalized = media_type::normalize(content_type);
    normalized.starts_with("video/") || normalized.starts_with("audio/")
}

/// Trigger HLS transcoding for a video (fire-and-forget)
/// Sends a POST request to the divine-transcoder service
async fn trigger_transcoding(transcoder_url: &str, hash: &str, owner: &str) -> Result<()> {
    info!(
        "Triggering HLS transcoding for {} via {}",
        hash, transcoder_url
    );

    let client = reqwest::Client::new();
    let transcode_request = serde_json::json!({
        "hash": hash,
        "owner": owner
    });

    let response = client
        .post(format!("{}/transcode", transcoder_url))
        .header("Content-Type", "application/json")
        .json(&transcode_request)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to call transcoder: {}", e))?;

    if response.status().is_success() {
        info!("Transcoding triggered successfully for {}", hash);
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("Transcoder returned error {}: {}", status, body))
    }
}

/// Max audio body accepted by `POST /transcribe`. Matches the production ingress
/// limit for this host: the `divine-upload-server` HTTPRoute attaches the
/// `upload-body-size` SnippetsFilter (`client_max_body_size 16m`), which is a
/// location-context directive and so overrides the 100 MiB gateway-wide
/// `ClientSettingsPolicy`. Anything larger is rejected before it reaches this
/// handler, so a higher limit here would be a lie.
/// Editor clip audio (16 kHz mono PCM, ~32 KB/s) is far under this.
const MAX_TRANSCRIBE_AUDIO_BYTES: usize = 16 * 1024 * 1024;

/// Concurrent `/transcribe` requests admitted before shedding load. Caps the
/// buffered *inbound* audio at
/// `MAX_CONCURRENT_TRANSCRIBE * MAX_TRANSCRIBE_AUDIO_BYTES` (~128 MiB). The
/// downstream transcoder response is buffered separately and uncapped — a
/// trusted internal service returning small WebVTT — so it is not counted
/// here. Tune to the pod's memory limit; the transcoder's own semaphore is a
/// separate cap.
const MAX_CONCURRENT_TRANSCRIBE: usize = 8;

/// Retry-After (seconds, as sent) advertised when the local admission
/// semaphore sheds an over-capacity `/transcribe` request.
const TRANSCRIBE_SHED_RETRY_AFTER: &str = "5";

/// Content-Type stamped on the body forwarded to the transcoder. Editor clips
/// are extracted as 16 kHz mono WAV, so every inbound body is relabeled WAV
/// regardless of the client's declared type.
const TRANSCRIBE_FORWARD_CONTENT_TYPE: &str = "audio/wav";

/// Divine hosts a `t=media` transcription token may be `server`-scoped to
/// (BUD-11). A token carrying a `server` tag for any other domain is being
/// replayed from elsewhere and is rejected on this route.
const TRANSCRIBE_ALLOWED_SERVER_HOSTS: [&str; 2] = ["media.divine.video", "upload.divine.video"];

/// Reduces a BUD-11 `server` tag value to a bare lowercase host. Parses with
/// `url::Url` so the authority is extracted correctly — dropping userinfo,
/// path, and port — rather than a hand-rolled split that a crafted value such
/// as `https://evil.example/x://media.divine.video` or
/// `https://media.divine.video:x@evil.com` could slip past. Bare hosts (no
/// scheme) are supported by prepending `https://` before parsing.
fn server_tag_host(value: &str) -> String {
    let trimmed = value.trim();
    let candidate = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };
    url::Url::parse(&candidate)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_ascii_lowercase))
        .unwrap_or_else(|| trimmed.to_ascii_lowercase())
}

/// Whether a `server` tag value targets this service (BUD-11 domain match).
fn transcribe_server_tag_allowed(server: &str) -> bool {
    TRANSCRIBE_ALLOWED_SERVER_HOSTS.contains(&server_tag_host(server).as_str())
}

#[derive(Debug, Deserialize)]
struct TranscribeParams {
    /// Optional BCP-47 recognition-language hint, forwarded to the transcoder.
    language: Option<String>,
}

/// POST /transcribe — authenticated proxy for synchronous audio transcription.
///
/// The editor posts extracted clip audio here (Blossom kind-24242 auth,
/// `t=media`) and gets WebVTT back. We forward the bytes to the private
/// transcoder's `/transcribe/audio` (the same service the by-hash flow uses)
/// and pass its response straight through.
///
/// Rate limiting lives at the edge (`media.divine.video`, divine-blossom). This
/// directly-reachable host adds local admission control (`transcribe_slots`) to
/// bound buffered-audio memory, not a per-pubkey throttle. An audio-hash result
/// cache in the transcoder remains a deferred optimization — every call is still
/// an uncached, billable provider request.
async fn handle_transcribe(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(params): Query<TranscribeParams>,
    body: Body,
) -> Response {
    let auth_event = match validate_auth(&headers, "media") {
        Ok(event) => event,
        Err(error) => return auth_error_response(error),
    };
    // BUD-11: a transcription token must expire. `validate_auth` only checks
    // the `expiration` tag when present, so a token without one would be valid
    // forever — reject it here on this billable route.
    if get_tag_value(&auth_event.tags, "expiration").is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            "Authorization must include an expiration tag",
        )
            .into_response();
    }

    // BUD-11: honor an explicit `server` scope. A token scoped to another
    // domain is being replayed here — `validate_auth` never inspects it.
    if let Some(server) = get_tag_value(&auth_event.tags, "server") {
        if !transcribe_server_tag_allowed(&server) {
            return (
                StatusCode::UNAUTHORIZED,
                "Authorization server tag does not match this host",
            )
                .into_response();
        }
    }

    let Some(transcriber_url) = state.config.transcriber_url.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Transcription is not configured",
        )
            .into_response();
    };

    // Admission control: cap concurrent buffered-audio memory. Acquired before
    // reading the body so an over-limit request is shed without buffering it.
    let _permit = match state.transcribe_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, TRANSCRIBE_SHED_RETRY_AFTER)],
                "Transcription is busy; retry shortly",
            )
                .into_response();
        }
    };

    let audio = match axum::body::to_bytes(body, MAX_TRANSCRIBE_AUDIO_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => {
            // axum's `to_bytes` error is opaque: it fires both on oversize and
            // on a genuine mid-body stream failure (client disconnect, HTTP/2
            // RST, ingress read timeout). Log it so transient failures aren't
            // invisible; 413 stays the best-guess status since this cap mirrors
            // the ingress `client_max_body_size`.
            error!("Transcribe body read failed: {}", error);
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Audio exceeds {} bytes", MAX_TRANSCRIBE_AUDIO_BYTES),
            )
                .into_response();
        }
    };
    if audio.is_empty() {
        return (StatusCode::BAD_REQUEST, "Empty audio body").into_response();
    }

    match proxy_transcribe_audio(
        &state.http_client,
        &transcriber_url,
        state.config.transcribe_shared_secret.as_deref(),
        audio,
        params.language.as_deref(),
    )
    .await
    {
        Ok((status, content_type, retry_after, body)) => {
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            if let Ok(value) = HeaderValue::from_str(&content_type) {
                response.headers_mut().insert(header::CONTENT_TYPE, value);
            }
            if let Some(retry_after) = retry_after {
                if let Ok(value) = HeaderValue::from_str(&retry_after) {
                    response.headers_mut().insert(header::RETRY_AFTER, value);
                }
            }
            response
        }
        Err(error) => {
            error!("Transcribe proxy failed: {}", error);
            (StatusCode::BAD_GATEWAY, "Transcription service unavailable").into_response()
        }
    }
}

/// Builds the transcoder's `/transcribe/audio` URL, tolerating a trailing
/// slash on the configured base URL.
fn transcribe_audio_url(transcriber_url: &str) -> String {
    format!("{}/transcribe/audio", transcriber_url.trim_end_matches('/'))
}

/// Header carrying the shared secret the transcoder requires on
/// `/transcribe/audio` (that service is `--allow-unauthenticated`).
const TRANSCRIBE_SECRET_HEADER: &str = "X-Divine-Transcribe-Secret";

/// Forwards [audio] to the transcoder's `/transcribe/audio`, returning its
/// status, content-type, and body verbatim so WebVTT (or an error) passes
/// straight back to the caller.
///
/// [secret] is injected as the transcoder's shared-secret header; this is what
/// lets the transcoder trust that the request already passed this service's
/// Nostr auth. When it is `None` the header is omitted and the transcoder is
/// expected to reject the request; a startup `warn!` flags that misconfig.
async fn proxy_transcribe_audio(
    client: &reqwest::Client,
    transcriber_url: &str,
    secret: Option<&str>,
    audio: Bytes,
    language: Option<&str>,
) -> Result<(StatusCode, String, Option<String>, Bytes)> {
    let url = transcribe_audio_url(transcriber_url);
    let lang = language.map(str::trim).filter(|lang| !lang.is_empty());

    let mut request = client
        .post(&url)
        .header(
            reqwest::header::CONTENT_TYPE,
            TRANSCRIBE_FORWARD_CONTENT_TYPE,
        )
        .body(audio);
    if let Some(lang) = lang {
        request = request.query(&[("language", lang)]);
    }
    if let Some(secret) = secret {
        request = request.header(TRANSCRIBE_SECRET_HEADER, secret);
    }

    let response = request
        .send()
        .await
        .map_err(|e| anyhow!("Failed to call transcriber: {}", e))?;

    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("text/vtt; charset=utf-8")
        .to_string();
    // Forward a throttle backoff verbatim if the transcoder ever emits one
    // (429/503), so the caller isn't left guessing when to retry.
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .bytes()
        .await
        .map_err(|e| anyhow!("Failed to read transcriber response: {}", e))?;

    Ok((status, content_type, retry_after, body))
}

/// Trigger transcript generation for audio/video (fire-and-forget)
async fn trigger_transcription(transcriber_url: &str, hash: &str, owner: &str) -> Result<()> {
    info!(
        "Triggering transcript generation for {} via {}",
        hash, transcriber_url
    );

    let client = reqwest::Client::new();
    let request_payload = serde_json::json!({
        "hash": hash,
        "owner": owner
    });

    let response = client
        .post(format!("{}/transcribe", transcriber_url))
        .header("Content-Type", "application/json")
        .json(&request_payload)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to call transcriber: {}", e))?;

    if response.status().is_success() {
        info!("Transcription triggered successfully for {}", hash);
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("Transcriber returned error {}: {}", status, body))
    }
}

/// Handle migration requests - fetch from URL and upload to GCS
/// POST /migrate { "source_url": "https://cdn.example.com/hash", "expected_hash": "abc123" }
async fn handle_migrate(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateRequest>,
) -> Response {
    match process_migrate(state, request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            error!("Migration error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response()
        }
    }
}

async fn process_migrate(state: Arc<AppState>, request: MigrateRequest) -> Result<MigrateResponse> {
    info!("Migration request for: {}", request.source_url);

    // Validate URL is from allowed Blossom/CDN sources
    // Expanded to include popular Blossom servers for BUD-04 mirror support
    let allowed_domains = [
        // Divine infrastructure
        "cdn.divine.video",
        "blossom.divine.video",
        // Satellite.earth
        "cdn.satellite.earth",
        "satellite.earth",
        // nostr.build - popular media host
        "nostr.build",
        "image.nostr.build",
        "media.nostr.build",
        "video.nostr.build",
        // void.cat - another popular host
        "void.cat",
        // Primal
        "primal.b-cdn.net",
        "media.primal.net",
        // Other Blossom servers
        "blossom.oxtr.dev",
        "blossom.primal.net",
        "files.sovbit.host",
        "blossom.f7z.io",
        "nostrcheck.me",
    ];
    let url = url::Url::parse(&request.source_url).map_err(|e| anyhow!("Invalid URL: {}", e))?;

    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL must have a host"))?;
    if !allowed_domains.iter().any(|d| host.ends_with(d)) {
        return Err(anyhow!("Source URL must be from an allowed domain"));
    }

    // Fetch content from source
    let client = reqwest::Client::new();
    let mut response = client
        .get(&request.source_url)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to fetch source: {}", e))?;

    // If we get 401, try with Nostr auth
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        info!("Source requires auth, attempting Nostr auth...");

        if let Some(nsec) = &state.config.migration_nsec {
            let auth_header = create_blossom_auth(nsec, "get", &request.source_url)?;
            response = client
                .get(&request.source_url)
                .header("Authorization", auth_header)
                .send()
                .await
                .map_err(|e| anyhow!("Failed to fetch source with auth: {}", e))?;
        } else {
            return Err(anyhow!(
                "Source requires auth but no MIGRATION_NSEC configured"
            ));
        }
    }

    if !response.status().is_success() {
        return Err(anyhow!("Source returned status: {}", response.status()));
    }

    // Get content type from response
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // Stream and hash the content
    let mut hasher = Sha256::new();
    let mut all_bytes = Vec::new();
    let mut total_size: u64 = 0;

    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| anyhow!("Stream error: {}", e))?;
        hasher.update(&chunk);
        total_size += chunk.len() as u64;
        all_bytes.extend_from_slice(&chunk);
    }

    let sha256_hash = hex::encode(hasher.finalize());

    // Verify hash if expected_hash is provided
    if let Some(expected) = &request.expected_hash {
        if &sha256_hash != expected {
            return Err(anyhow!(
                "Hash mismatch: expected {}, got {}",
                expected,
                sha256_hash
            ));
        }
    }

    // Check if blob already exists in GCS
    let exists = state
        .gcs_client
        .get_object(
            &google_cloud_storage::http::objects::get::GetObjectRequest {
                bucket: state.config.gcs_bucket.clone(),
                object: sha256_hash.clone(),
                ..Default::default()
            },
        )
        .await
        .is_ok();

    if exists {
        info!("Blob {} already exists, skipping migration", sha256_hash);
        return Ok(MigrateResponse {
            sha256: sha256_hash,
            size: total_size,
            content_type,
            migrated: false,
            source_url: request.source_url,
        });
    }

    // Upload to GCS
    let upload_type = UploadType::Simple(Media::new(sha256_hash.clone()));
    let req = UploadObjectRequest {
        bucket: state.config.gcs_bucket.clone(),
        ..Default::default()
    };

    state
        .gcs_client
        .upload_object(&req, Bytes::from(all_bytes), &upload_type)
        .await
        .map_err(|e| anyhow!("GCS upload failed: {}", e))?;

    // Set content type and owner metadata for durability
    let metadata_map = request.owner.as_ref().map(|owner| {
        let mut m = std::collections::HashMap::new();
        m.insert("owner".to_string(), owner.clone());
        m
    });

    let update_req = google_cloud_storage::http::objects::patch::PatchObjectRequest {
        bucket: state.config.gcs_bucket.clone(),
        object: sha256_hash.clone(),
        metadata: Some(Object {
            content_type: Some(content_type.clone()),
            metadata: metadata_map,
            ..Default::default()
        }),
        ..Default::default()
    };
    let _ = state.gcs_client.patch_object(&update_req).await;

    info!(
        "Migrated {} bytes as {} from {} (owner: {:?})",
        total_size, sha256_hash, request.source_url, request.owner
    );

    Ok(MigrateResponse {
        sha256: sha256_hash,
        size: total_size,
        content_type,
        migrated: true,
        source_url: request.source_url,
    })
}

/// Create a Blossom auth header from an nsec
/// nsec is a bech32-encoded Nostr secret key
fn create_blossom_auth(nsec: &str, action: &str, _url: &str) -> Result<String> {
    // Decode nsec (bech32)
    let secret_key_bytes = decode_nsec(nsec)?;

    // Create signing key
    let signing_key = SigningKey::from_bytes(&secret_key_bytes)
        .map_err(|e| anyhow!("Invalid secret key: {}", e))?;

    // Get public key
    let verifying_key = signing_key.verifying_key();
    let pubkey_bytes = verifying_key.to_bytes();
    let pubkey_hex = hex::encode(pubkey_bytes);

    // Create event timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Create expiration (5 minutes from now)
    let expiration = now + 300;

    // Create tags
    let tags = vec![
        vec!["t".to_string(), action.to_string()],
        vec!["expiration".to_string(), expiration.to_string()],
    ];

    // Create event (without id and sig)
    let event_data = serde_json::json!([0, pubkey_hex, now, BLOSSOM_AUTH_KIND, tags, ""]);

    // Hash to get event ID
    let event_str = serde_json::to_string(&event_data)?;
    let mut hasher = Sha256::new();
    hasher.update(event_str.as_bytes());
    let event_id = hex::encode(hasher.finalize());

    // Sign the event ID
    let id_bytes = hex::decode(&event_id)?;
    let signature = signing_key
        .sign_prehash(&id_bytes)
        .map_err(|e| anyhow!("Signing error: {}", e))?;
    let sig_hex = hex::encode(signature.to_bytes());

    // Create full event
    let event = serde_json::json!({
        "id": event_id,
        "pubkey": pubkey_hex,
        "created_at": now,
        "kind": BLOSSOM_AUTH_KIND,
        "tags": tags,
        "content": "",
        "sig": sig_hex
    });

    // Base64 encode for Authorization header
    let event_json = serde_json::to_string(&event)?;
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, event_json);

    Ok(format!("Nostr {}", encoded))
}

/// Decode an nsec (bech32-encoded Nostr secret key) to raw bytes
fn decode_nsec(nsec: &str) -> Result<[u8; 32]> {
    if !nsec.starts_with("nsec1") {
        return Err(anyhow!("Invalid nsec: must start with 'nsec1'"));
    }

    // Simple bech32 decode (Nostr uses bech32 without checksum verification for keys)
    let data = &nsec[5..]; // Skip "nsec1" prefix

    // Bech32 alphabet
    const CHARSET: &str = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

    let mut bits: Vec<u8> = Vec::new();
    for c in data.chars() {
        let val = CHARSET
            .find(c)
            .ok_or_else(|| anyhow!("Invalid bech32 character: {}", c))? as u8;
        bits.push(val);
    }

    // Convert 5-bit groups to 8-bit bytes
    let mut result = Vec::new();
    let mut acc: u32 = 0;
    let mut bits_count = 0;

    for val in bits {
        acc = (acc << 5) | (val as u32);
        bits_count += 5;
        while bits_count >= 8 {
            bits_count -= 8;
            result.push((acc >> bits_count) as u8);
            acc &= (1 << bits_count) - 1;
        }
    }

    // Take the first 32 bytes (ignore any padding/checksum)
    if result.len() < 32 {
        return Err(anyhow!("Invalid nsec: decoded data too short"));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&result[..32]);
    Ok(key)
}
