use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

const BEGIN: &str = "/internal/media-processing/attempts/begin";
const PREPARE: &str = "/internal/media-processing/attempts/prepare";
const INSPECT: &str = "/internal/media-processing/attempts/inspect";
const DISPATCH: &str = "/internal/media-processing/attempts/dispatch";

#[tokio::test]
async fn planned_replay_lock_wait_cannot_accept_expired_lease() {
    let Some((one, two, assignment)) = fixture().await else {
        return;
    };
    let lease = serde_json::to_value(Lease::from(&assignment)).unwrap();
    let begun = call(one.clone(), BEGIN, serde_json::json!({"lease":lease})).await;
    assert_eq!(begun.0, StatusCode::OK);
    let id = begun.1["attempt"]["attempt_id"].as_str().unwrap();
    let plan = serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":artifacts()});
    let prepared = call(one.clone(), PREPARE, plan.clone()).await;
    assert_eq!(prepared.0, StatusCode::OK);
    let waiter_pid: i32 = one
        .store
        .database
        .lock()
        .await
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    one.store.database.lock().await.execute(
        "UPDATE media_processing_jobs SET lease_until=clock_timestamp()+interval '2 seconds' WHERE processing_job_id=$1",
        &[&assignment.processing_job_id],
    ).await.unwrap();
    let mut holder = two.store.database.lock().await;
    let tx = holder.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
        &[&assignment.processing_job_id],
    )
    .await
    .unwrap();
    let request = tokio::spawn(call(one.clone(), PREPARE, plan));
    database_tests::wait_for_expiry_while_locked(&tx, waiter_pid, &assignment.processing_job_id)
        .await;
    tx.commit().await.unwrap();
    drop(holder);
    assert_eq!(request.await.unwrap().0, StatusCode::CONFLICT);
    assert_eq!(
        call(two.clone(), INSPECT, serde_json::json!({"attempt_id":id}))
            .await
            .1,
        prepared.1
    );
}

fn settings(enabled: bool) -> InboxSettings {
    InboxSettings::from_lookup(|key| match key {
        "MEDIA_FINALIZED_INBOX_ENABLED" => Some("true".into()),
        "MEDIA_FINALIZED_INBOX_TOKEN" => Some("fixture-intake-token".into()),
        "MEDIA_PROCESSING_WORKER_TOKEN" => Some("fixture-worker-token".into()),
        "MEDIA_PROCESSING_JOURNAL_ENABLED" => Some(enabled.to_string()),
        _ => None,
    })
    .unwrap()
    .unwrap()
}

async fn fixture() -> Option<(InboxRuntime, InboxRuntime, Assignment)> {
    let (one, two) = database_tests::stores().await?;
    let event = database_tests::event();
    one.accept(&event, &event.job_id).await.unwrap();
    let assignment = one.claim().await.unwrap().unwrap();
    let runtime = |store| InboxRuntime {
        store,
        settings: Arc::new(settings(true)),
        deadline: std::time::Duration::from_secs(10),
    };
    Some((runtime(one), runtime(two), assignment))
}

async fn call(
    runtime: InboxRuntime,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::post(path)
        .header("Authorization", "Bearer fixture-worker-token")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = router(Some(runtime)).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 32768).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

fn artifacts() -> serde_json::Value {
    let names = [
        "stream_720p.ts",
        "stream_720p.mp4",
        "stream_480p.ts",
        "stream_480p.mp4",
        "stream_720p.m3u8",
        "stream_480p.m3u8",
        "master.m3u8",
    ];
    serde_json::Value::Array(names.iter().map(|name| serde_json::json!({
        "filename": name, "sha256": "c".repeat(64), "size": 188,
        "content_type": if name.ends_with(".ts") {"video/mp2t"} else if name.ends_with(".mp4") {"video/mp4"} else {"application/vnd.apple.mpegurl"}
    })).collect())
}

#[test]
fn journal_configuration_requires_explicit_valid_enablement() {
    for value in ["yes", "1", ""] {
        assert!(InboxSettings::from_lookup(|key| match key {
            "MEDIA_FINALIZED_INBOX_ENABLED" => Some("true".into()),
            "MEDIA_FINALIZED_INBOX_TOKEN" => Some("fixture-intake-token".into()),
            "MEDIA_PROCESSING_WORKER_TOKEN" => Some("fixture-worker-token".into()),
            "MEDIA_PROCESSING_JOURNAL_ENABLED" => Some(value.into()),
            _ => None,
        })
        .is_err());
    }
    assert!(InboxSettings::from_lookup(
        |key| (key == "MEDIA_PROCESSING_JOURNAL_ENABLED").then(|| "true".into())
    )
    .is_err());
}

#[tokio::test]
async fn concurrent_begin_persists_one_nonce_and_plan_replay_is_immutable() {
    let Some((one, two, assignment)) = fixture().await else {
        return;
    };
    let lease = serde_json::to_value(Lease::from(&assignment)).unwrap();
    let (a, b) = tokio::join!(
        call(one.clone(), BEGIN, serde_json::json!({"lease":lease})),
        call(two.clone(), BEGIN, serde_json::json!({"lease":lease}))
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    let id = a.1["attempt"]["attempt_id"].as_str().unwrap();
    assert_eq!(id.len(), 32);
    assert!(id
        .bytes()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)));
    assert_eq!(a.1, b.1);
    assert_eq!(a.1["attempt"]["phase"], "allocated");
    let plan = serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":artifacts()});
    let prepared = call(one.clone(), PREPARE, plan.clone()).await;
    assert_eq!(prepared.0, StatusCode::OK);
    assert_eq!(prepared.1["attempt"]["phase"], "planned");
    assert_eq!(
        prepared.1["attempt"]["artifacts"].as_array().unwrap().len(),
        7
    );
    let artifact = &prepared.1["attempt"]["artifacts"][0];
    assert_eq!(
        artifact["target"]["key"],
        format!("{}/revisions/v1/{}/stream_720p.ts", assignment.sha256, id)
    );
    assert_eq!(artifact["target"]["provider"], "b2");
    assert_eq!(artifact["metadata"]["source-sha256"], assignment.sha256);
    assert_eq!(artifact["metadata"]["hls-filename"], "stream_720p.ts");
    assert!(artifact["target"].get("version").is_none());
    let mut reversed = plan.clone();
    reversed["artifacts"].as_array_mut().unwrap().reverse();
    assert_eq!(call(two.clone(), PREPARE, reversed).await.1, prepared.1);
    let mut changed = plan;
    changed["artifacts"][0]["sha256"] = serde_json::json!("d".repeat(64));
    assert_eq!(
        call(two.clone(), PREPARE, changed).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(two, INSPECT, serde_json::json!({"attempt_id":id}))
            .await
            .1,
        prepared.1
    );
    let count: i64 = one
        .store
        .database
        .lock()
        .await
        .query_one("SELECT count(*) FROM media_storage_operations", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 7);
}

#[tokio::test]
async fn invalid_plans_are_atomic_and_never_create_partial_intents() {
    let Some((runtime, _, assignment)) = fixture().await else {
        return;
    };
    let lease = serde_json::to_value(Lease::from(&assignment)).unwrap();
    let begun = call(runtime.clone(), BEGIN, serde_json::json!({"lease":lease})).await;
    assert_eq!(begun.0, StatusCode::OK);
    let id = begun.1["attempt"]["attempt_id"].as_str().unwrap();
    let mut invalid = Vec::new();
    let mut missing = artifacts();
    missing.as_array_mut().unwrap().pop();
    invalid.push(missing);
    let mut duplicate = artifacts();
    duplicate[1] = duplicate[0].clone();
    invalid.push(duplicate);
    let mut traversal = artifacts();
    traversal[0]["filename"] = serde_json::json!("../stream_720p.ts");
    invalid.push(traversal);
    let mut wrong_type = artifacts();
    wrong_type[0]["content_type"] = serde_json::json!("text/plain");
    invalid.push(wrong_type);
    let mut zero = artifacts();
    zero[0]["size"] = serde_json::json!(0);
    invalid.push(zero);
    let mut huge = artifacts();
    huge[0]["size"] = serde_json::json!(32 * 1024 * 1024 + 1);
    invalid.push(huge);
    let mut total = artifacts();
    for entry in total.as_array_mut().unwrap().iter_mut().take(4) {
        entry["size"] = serde_json::json!(20 * 1024 * 1024);
    }
    invalid.push(total);
    let mut playlist = artifacts();
    playlist[6]["size"] = serde_json::json!(64 * 1024 + 1);
    invalid.push(playlist);
    let mut foreign = artifacts();
    foreign[0]["target"] = serde_json::json!({"bucket":"foreign"});
    invalid.push(foreign);
    for plan in invalid {
        assert_eq!(
            call(
                runtime.clone(),
                PREPARE,
                serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":plan})
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    let current = call(runtime, INSPECT, serde_json::json!({"attempt_id":id})).await;
    assert_eq!(current.1["attempt"]["phase"], "allocated");
    assert_eq!(current.1["attempt"]["artifacts"], serde_json::json!([]));
}

#[tokio::test]
async fn expired_and_superseded_attempts_remain_readable_without_write_authority() {
    let Some((one, two, assignment)) = fixture().await else {
        return;
    };
    let lease = serde_json::to_value(Lease::from(&assignment)).unwrap();
    let begun = call(one.clone(), BEGIN, serde_json::json!({"lease":lease})).await;
    assert_eq!(begun.0, StatusCode::OK);
    let id = begun.1["attempt"]["attempt_id"].as_str().unwrap();
    let mut changed = database_tests::event();
    changed.replicas.push(ObjectRef {
        provider: StorageProvider::Gcs,
        bucket: "synthetic-gcs".into(),
        key: changed.sha256.clone(),
        version: Some("123".into()),
    });
    changed.refresh_job_id().unwrap();
    two.store.accept(&changed, &changed.job_id).await.unwrap();
    assert_eq!(
        call(one.clone(), BEGIN, serde_json::json!({"lease":lease}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            one.clone(),
            PREPARE,
            serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":artifacts()})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(two.clone(), INSPECT, serde_json::json!({"attempt_id":id}))
            .await
            .1,
        begun.1
    );
    one.store
        .database
        .lock()
        .await
        .execute(
            "UPDATE media_processing_jobs SET lease_until=clock_timestamp()-interval '1 second'",
            &[],
        )
        .await
        .unwrap();
    let fresh = two.store.claim().await.unwrap().unwrap();
    let new = call(
        two.clone(),
        BEGIN,
        serde_json::json!({"lease":Lease::from(&fresh)}),
    )
    .await;
    assert_eq!(new.0, StatusCode::OK);
    assert_ne!(new.1["attempt"]["attempt_id"], id);
    two.store
        .database
        .lock()
        .await
        .execute(
            "UPDATE media_processing_jobs SET lease_until=clock_timestamp()-interval '1 second'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        call(
            two.clone(),
            BEGIN,
            serde_json::json!({"lease":Lease::from(&fresh)})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(two, INSPECT, serde_json::json!({"attempt_id":id}))
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn missing_parent_gate_never_dispatches_or_bypasses_completion() {
    let Some((runtime, _, assignment)) = fixture().await else {
        return;
    };
    let lease = serde_json::to_value(Lease::from(&assignment)).unwrap();
    let begun = call(runtime.clone(), BEGIN, serde_json::json!({"lease":lease})).await;
    assert_eq!(begun.0, StatusCode::OK);
    let id = begun.1["attempt"]["attempt_id"].as_str().unwrap();
    assert_eq!(
        call(
            runtime.clone(),
            PREPARE,
            serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":artifacts()})
        )
        .await
        .0,
        StatusCode::OK
    );
    let denied = call(
        runtime.clone(),
        DISPATCH,
        serde_json::json!({"lease":lease,"attempt_id":id,"filename":"stream_720p.ts"}),
    )
    .await;
    assert_eq!(denied.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        denied.1,
        serde_json::json!({"error":"producer parent gate unavailable"})
    );
    let marker = database_tests::marker(&assignment);
    assert_eq!(
        call(
            runtime.clone(),
            "/internal/media-processing/complete",
            serde_json::json!({"lease":lease,"manifest":marker})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let row=runtime.store.database.lock().await.query_one("SELECT state,manifest,(SELECT count(*) FROM media_storage_operations WHERE state<>'prepared') FROM media_processing_jobs",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert!(row.get::<_, Option<serde_json::Value>>(1).is_none());
    assert_eq!(row.get::<_, i64>(2), 0);
}

#[tokio::test]
async fn journal_scope_preserves_completed_receipt_replay_and_blocks_no_journal_bypass() {
    let Some((runtime, _, assignment)) = fixture().await else {
        return;
    };
    let lease = Lease::from(&assignment);
    let marker = database_tests::marker(&assignment);
    let payload = serde_json::json!({"lease":lease,"manifest":marker});
    assert_eq!(
        call(
            runtime.clone(),
            "/internal/media-processing/complete",
            payload.clone()
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(runtime.store.complete(&lease, &marker).await.unwrap());
    runtime.store.database.lock().await.execute("UPDATE media_processing_jobs SET attempt_deadline=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
    assert_eq!(
        call(
            runtime.clone(),
            "/internal/media-processing/complete",
            payload
        )
        .await
        .0,
        StatusCode::OK
    );
    let mut different = marker;
    different.version = Some("different-version".into());
    assert_eq!(
        call(
            runtime,
            "/internal/media-processing/complete",
            serde_json::json!({"lease":lease,"manifest":different})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn journal_requires_worker_auth_and_explicit_optin() {
    let Some((mut runtime, _, assignment)) = fixture().await else {
        return;
    };
    let wrong = Request::post(BEGIN)
        .header("Authorization", "Bearer fixture-intake-token")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        router(Some(runtime.clone()))
            .oneshot(wrong)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    runtime.settings = Arc::new(settings(false));
    assert_eq!(
        call(
            runtime,
            BEGIN,
            serde_json::json!({"lease":Lease::from(&assignment)})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
