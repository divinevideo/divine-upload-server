use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

#[tokio::test]
async fn producer_journal_routes_are_disabled_by_default() {
    for path in [
        "/internal/media-processing/attempts/begin",
        "/internal/media-processing/attempts/prepare",
        "/internal/media-processing/attempts/inspect",
        "/internal/media-processing/attempts/dispatch",
        "/internal/media-processing/attempts/gate/claim",
        "/internal/media-processing/attempts/gate/inspect",
    ] {
        let response = router(None)
            .oneshot(request(path, "fixture-worker-token", "", b"{}".to_vec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(response.headers()["cache-control"], "private, no-store");
    }
}

#[test]
fn explicit_enablement_requires_separate_tokens() {
    assert!(InboxSettings::from_lookup(|_| None).unwrap().is_none());
    assert!(InboxSettings::from_lookup(
        |key| (key == "MEDIA_FINALIZED_INBOX_ENABLED").then(|| "true".into())
    )
    .is_err());
    let value = InboxSettings::from_lookup(|key| match key {
        "MEDIA_FINALIZED_INBOX_ENABLED" => Some("true".into()),
        "MEDIA_FINALIZED_INBOX_TOKEN" => Some("fixture-intake-token".into()),
        "MEDIA_PROCESSING_WORKER_TOKEN" => Some("fixture-worker-token".into()),
        _ => None,
    })
    .unwrap();
    assert!(value.is_some());
}

async fn runtime() -> Option<InboxRuntime> {
    let (store, _) = super::database_tests::stores().await?;
    Some(InboxRuntime {
        store,
        settings: Arc::new(InboxSettings {
            intake_token: "fixture-intake-token".into(),
            worker_token: "fixture-worker-token".into(),
            producer_journal_enabled: false,
        }),
        deadline: std::time::Duration::from_secs(10),
        parent_gate: None,
    })
}

fn request(path: &str, token: &str, key: &str, body: Vec<u8>) -> Request<Body> {
    Request::post(path)
        .header("Authorization", format!("Bearer {token}"))
        .header("Idempotency-Key", key)
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn authenticated_intake_ack_matches_only_after_durable_work_and_claim_is_separate() {
    let Some(runtime) = runtime().await else {
        return;
    };
    let app = router(Some(runtime.clone()));
    let event = super::database_tests::event();
    let body = serde_json::to_vec(&event).unwrap();
    let unauthorized = app
        .clone()
        .oneshot(request(
            "/internal/media-finalized",
            "fixture-worker-token",
            &event.job_id,
            body.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(request(
            "/internal/media-finalized",
            "fixture-intake-token",
            &event.job_id,
            body,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let ack: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(
        ack,
        serde_json::json!({"schema_version":1,"job_id":event.job_id,"durable":true})
    );
    assert_eq!(
        runtime
            .store
            .database
            .lock()
            .await
            .query_one(
                "SELECT count(*) FROM media_finalization_inbox WHERE registration_state='pending'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let unauthorized = app
        .clone()
        .oneshot(request(
            "/internal/media-processing/claim",
            "fixture-intake-token",
            "",
            b"{}".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(request(
            "/internal/media-processing/claim",
            "fixture-worker-token",
            "",
            b"{}".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 32768).await.unwrap()).unwrap();
    let assignment: Assignment = serde_json::from_value(result["assignment"].clone()).unwrap();
    let completion = serde_json::json!({"lease":Lease::from(&assignment),"manifest":super::database_tests::marker(&assignment)});
    let response = app
        .oneshot(request(
            "/internal/media-processing/complete",
            "fixture-worker-token",
            "",
            serde_json::to_vec(&completion).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn invalid_oversized_and_database_timeout_never_ack_durable() {
    let Some(mut runtime) = runtime().await else {
        return;
    };
    let app = router(Some(runtime.clone()));
    let event = super::database_tests::event();
    for (body, expected) in [
        (b"invalid".to_vec(), StatusCode::BAD_REQUEST),
        (vec![b' '; 65536], StatusCode::PAYLOAD_TOO_LARGE),
    ] {
        let result = app
            .clone()
            .oneshot(request(
                "/internal/media-finalized",
                "fixture-intake-token",
                &event.job_id,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), expected);
    }
    let _lock = runtime.store.database.lock().await;
    runtime.deadline = std::time::Duration::from_millis(100);
    let response = router(Some(runtime.clone()))
        .oneshot(request(
            "/internal/media-finalized",
            "fixture-intake-token",
            &event.job_id,
            serde_json::to_vec(&event).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    drop(_lock);
    assert_eq!(
        runtime
            .store
            .database
            .lock()
            .await
            .query_one("SELECT count(*) FROM media_finalization_inbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
async fn disabled_auth_before_body_and_slow_body_are_bounded() {
    let disabled = router(None)
        .oneshot(request(
            "/internal/media-finalized",
            "fixture-intake-token",
            "",
            b"{}".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::SERVICE_UNAVAILABLE);
    let Some(mut runtime) = runtime().await else {
        return;
    };
    runtime.deadline = std::time::Duration::from_millis(100);
    let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = observed.clone();
    let body = Body::from_stream(futures::stream::once(async move {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"{}"))
    }));
    let request = Request::post("/internal/media-finalized")
        .body(body)
        .unwrap();
    let response = router(Some(runtime.clone()))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!observed.load(std::sync::atomic::Ordering::SeqCst));
    let body = Body::from_stream(futures::stream::once(async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"{}"))
    }));
    let request = Request::post("/internal/media-finalized")
        .header("Authorization", "Bearer fixture-intake-token")
        .body(body)
        .unwrap();
    let response = router(Some(runtime)).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}
