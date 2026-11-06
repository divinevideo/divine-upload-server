//! Bounded internal HTTP boundary. No automatic processing worker is started.
use super::*;
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use std::time::Duration;

pub async fn configured(database_url: Option<&str>) -> Result<Option<InboxRuntime>> {
    let Some(settings) = InboxSettings::from_lookup(|key| std::env::var(key).ok())? else {
        return Ok(None);
    };
    let url = database_url
        .ok_or_else(|| anyhow!("COORDINATION_DATABASE_URL required for finalization inbox"))?;
    let store = tokio::time::timeout(Duration::from_secs(15), async {
        InboxStore::from_client(crate::coordination::connect(url).await?).await
    })
    .await
    .map_err(|_| anyhow!("inbox database initialization deadline exceeded"))??;
    Ok(Some(InboxRuntime {
        store,
        settings: Arc::new(settings),
        deadline: Duration::from_secs(10),
    }))
}

pub fn router(runtime: Option<InboxRuntime>) -> Router {
    Router::new()
        .route("/internal/media-finalized", post(handle))
        .route("/internal/media-processing/claim", post(handle))
        .route("/internal/media-processing/lookup", post(handle))
        .route("/internal/media-processing/renew", post(handle))
        .route("/internal/media-processing/complete", post(handle))
        .route("/internal/media-processing/attempts/begin", post(handle))
        .route("/internal/media-processing/attempts/prepare", post(handle))
        .route("/internal/media-processing/attempts/inspect", post(handle))
        .route("/internal/media-processing/attempts/dispatch", post(handle))
        .with_state(runtime)
}

fn response(status: StatusCode, value: serde_json::Value) -> Response {
    let mut result = (status, Json(value)).into_response();
    result
        .headers_mut()
        .insert("cache-control", "private, no-store".parse().unwrap());
    result
}

fn error(status: StatusCode, message: &'static str) -> Response {
    response(status, serde_json::json!({"error":message}))
}

fn token_matches(request: &Request, expected: &str) -> bool {
    let Some(candidate) = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    if candidate.len() != expected.len() {
        return false;
    }
    // Compare fixed-size MACs instead of early-exit secret string equality.
    let mut one = Hmac::<Sha256>::new_from_slice(expected.as_bytes()).unwrap();
    one.update(b"divine-finalization-inbox-v1");
    let mut two = Hmac::<Sha256>::new_from_slice(candidate.as_bytes()).unwrap();
    two.update(b"divine-finalization-inbox-v1");
    one.verify_slice(&two.finalize().into_bytes()).is_ok()
}

struct InvalidRequest;

impl From<InvalidRequest> for Response {
    fn from(_: InvalidRequest) -> Self {
        error(StatusCode::BAD_REQUEST, "invalid processing request")
    }
}

fn parse<T: DeserializeOwned>(body: &[u8]) -> std::result::Result<T, InvalidRequest> {
    serde_json::from_slice(body).map_err(|_| InvalidRequest)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Completion {
    lease: Lease,
    manifest: ObjectRef,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lookup {
    processing_job_id: String,
}

async fn handle(State(runtime): State<Option<InboxRuntime>>, request: Request) -> Response {
    let Some(runtime) = runtime else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "finalization inbox disabled",
        );
    };
    let intake = request.uri().path() == "/internal/media-finalized";
    let expected = if intake {
        &runtime.settings.intake_token
    } else {
        &runtime.settings.worker_token
    };
    if !token_matches(&request, expected) {
        return error(StatusCode::UNAUTHORIZED, "invalid processing credential");
    }
    if request
        .uri()
        .path()
        .starts_with("/internal/media-processing/attempts/")
        && !runtime.settings.producer_journal_enabled
    {
        return error(StatusCode::SERVICE_UNAVAILABLE, "producer journal disabled");
    }
    let key = request
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let path = request.uri().path().to_owned();
    let deadline = tokio::time::Instant::now() + runtime.deadline;
    let body =
        match tokio::time::timeout_at(deadline, to_bytes(request.into_body(), 32 * 1024)).await {
            Err(_) => {
                return error(
                    StatusCode::REQUEST_TIMEOUT,
                    "processing request deadline exceeded",
                )
            }
            Ok(Err(_)) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "processing body unavailable or too large",
                )
            }
            Ok(Ok(value)) => value,
        };
    let operation = async {
        match path.as_str() {
            "/internal/media-processing/attempts/begin" => {
                let input: journal::Begin = parse(&body)?;
                let attempt = runtime
                    .store
                    .journal_begin(&input.lease)
                    .await
                    .map_err(database_error)?;
                Ok(serde_json::json!({"attempt":attempt}))
            }
            "/internal/media-processing/attempts/prepare" => {
                let input: journal::Prepare = parse(&body)?;
                let attempt = runtime
                    .store
                    .journal_prepare(input)
                    .await
                    .map_err(database_error)?;
                Ok(serde_json::json!({"attempt":attempt}))
            }
            "/internal/media-processing/attempts/inspect" => {
                let input: journal::Inspect = parse(&body)?;
                let attempt = runtime
                    .store
                    .journal_inspect(&input.attempt_id)
                    .await
                    .map_err(database_error)?;
                Ok(serde_json::json!({"attempt":attempt}))
            }
            "/internal/media-processing/attempts/dispatch" => {
                let input: journal::Dispatch = parse(&body)?;
                runtime
                    .store
                    .journal_dispatch(input)
                    .await
                    .map_err(database_error)?;
                Err(error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "producer parent gate unavailable",
                ))
            }
            "/internal/media-finalized" => {
                let event: MediaFinalizedV1 = parse(&body)?;
                if validate_event(&event, &key).is_err() {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "invalid finalization envelope",
                    ));
                }
                runtime
                    .store
                    .accept(&event, &key)
                    .await
                    .map_err(database_error)?;
                Ok(serde_json::json!({"schema_version":1,"job_id":event.job_id,"durable":true}))
            }
            "/internal/media-processing/lookup" => {
                let input: Lookup = parse(&body)?;
                if !is_hash(&input.processing_job_id) {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "invalid processing identity",
                    ));
                }
                let receipt = runtime
                    .store
                    .lookup(&input.processing_job_id)
                    .await
                    .map_err(database_error)?;
                Ok(serde_json::json!({"receipt":receipt}))
            }
            "/internal/media-processing/claim" => {
                if parse::<serde_json::Value>(&body)? != serde_json::json!({}) {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "claim body must be empty object",
                    ));
                }
                let assignment = runtime.store.claim().await.map_err(database_error)?;
                Ok(serde_json::json!({"assignment":assignment}))
            }
            "/internal/media-processing/renew" => {
                let lease: Lease = parse(&body)?;
                if !runtime.store.renew(&lease).await.map_err(database_error)? {
                    return Err(error(
                        StatusCode::CONFLICT,
                        "processing assignment is stale",
                    ));
                }
                Ok(serde_json::json!({"renewed":true}))
            }
            "/internal/media-processing/complete" => {
                let completion: Completion = parse(&body)?;
                let completed = if runtime.settings.producer_journal_enabled {
                    runtime
                        .store
                        .journal_completion_replay(&completion.lease, &completion.manifest)
                        .await
                } else {
                    runtime
                        .store
                        .complete(&completion.lease, &completion.manifest)
                        .await
                }
                .map_err(database_error)?;
                if !completed {
                    return Err(error(
                        StatusCode::CONFLICT,
                        "processing assignment is stale",
                    ));
                }
                Ok(serde_json::json!({"completed":true}))
            }
            _ => unreachable!(),
        }
    };
    match tokio::time::timeout_at(deadline, operation).await {
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "processing persistence deadline exceeded",
        ),
        Ok(Err(value)) => value,
        Ok(Ok(value)) => response(StatusCode::OK, value),
    }
}

fn database_error(error: anyhow::Error) -> Response {
    if let Some(error) = error.downcast_ref::<journal::JournalError>() {
        let (status, message) = match error {
            journal::JournalError::Invalid => {
                (StatusCode::BAD_REQUEST, "invalid producer journal request")
            }
            journal::JournalError::Stale => (StatusCode::CONFLICT, "producer assignment is stale"),
            journal::JournalError::Conflict => {
                (StatusCode::CONFLICT, "conflicting producer artifact plan")
            }
            journal::JournalError::GateUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "producer parent gate unavailable",
            ),
        };
        return self::error(status, message);
    }
    if error.is::<InboxConflict>() {
        return self::error(StatusCode::CONFLICT, "conflicting finalization input");
    }
    if error.is::<InvalidCompletion>() {
        return self::error(StatusCode::BAD_REQUEST, "invalid private completion marker");
    }
    // Never return or log raw SQL, connection strings, payloads or provider errors.
    self::error(
        StatusCode::SERVICE_UNAVAILABLE,
        "processing persistence unavailable",
    )
}
