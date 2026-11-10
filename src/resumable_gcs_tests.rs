//! Real HTTP generation-precondition proofs for the new-mode session manifest.
use super::*;
use axum::{
    extract::State,
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
};
#[derive(Default)]
struct StateData {
    generation: i64,
    bytes: Option<Bytes>,
}
async fn origin(
    State(state): State<Arc<Mutex<StateData>>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let query: HashMap<String, String> =
        url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let mut s = state.lock().unwrap();
    let error = |code: u16| {
        (
            StatusCode::from_u16(code).unwrap(),
            Json(serde_json::json!({"error":{"code":code,"message":"fixture","errors":[]}})),
        )
            .into_response()
    };
    if method == Method::POST {
        let required = query
            .get("ifGenerationMatch")
            .expect("manifest writes must be conditional")
            .parse::<i64>()
            .unwrap();
        if required != s.generation {
            return error(412);
        }
        s.generation += 1;
        s.bytes = Some(body);
    }
    let Some(bytes) = s.bytes.as_ref() else {
        return error(404);
    };
    if query.get("alt").is_some_and(|v| v == "media") {
        if let Some(generation) = query.get("generation") {
            assert_eq!(generation, &s.generation.to_string());
        }
        return bytes.clone().into_response();
    }
    Json(serde_json::json!({"name":"manifest","bucket":"fixture","generation":s.generation.to_string(),"metageneration":"1","size":bytes.len().to_string(),"id":"manifest","selfLink":"fixture","mediaLink":"fixture","etag":"fixture"})).into_response()
}
fn session() -> UploadSession {
    UploadSession {
        processing_outbox_required: None,
        revision: 0,
        chunk_size: 1024,
        upload_id: "up_fixture".into(),
        owner: "a".repeat(64),
        final_sha256: "b".repeat(64),
        declared_size: 42,
        content_type: "application/octet-stream".into(),
        file_name: None,
        expires_at_epoch_secs: now_epoch_secs() + 300,
        next_offset: 0,
        session_url: "https://fixture.invalid/resumable".into(),
        session_token: "fixture".into(),
        temp_object: "__resumable/b2_primary/66697874757265/6232/up_fixture/blob".into(),
        storage_complete: false,
        finalized_object: None,
    }
}
#[tokio::test]
async fn b2_primary_gcs_manifest_cas_and_tombstone_survive_stale_writers() {
    let state = Arc::new(Mutex::new(StateData::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new().route("/*path", any(origin)).with_state(state),
        )
        .into_future(),
    );
    let client = GcsClient::new(
        google_cloud_storage::client::ClientConfig {
            storage_endpoint: endpoint,
            ..Default::default()
        }
        .anonymous(),
    );
    let store = GcsSessionStore::new(client, "fixture");
    let original = session();
    store.save(&original).await.unwrap();
    assert!(
        store.save(&original).await.is_err(),
        "create-only identity cannot be replaced"
    );
    let mut one = store.load(&original.upload_id).await.unwrap().unwrap();
    let two = store.load(&original.upload_id).await.unwrap().unwrap();
    assert!(one.revision > 0);
    one.next_offset = 21;
    store.save(&one).await.unwrap();
    assert!(
        store.save(&two).await.is_err(),
        "stale generation cannot advance offset"
    );
    assert_eq!(
        store
            .load(&original.upload_id)
            .await
            .unwrap()
            .unwrap()
            .next_offset,
        21
    );
    store.delete(&original.upload_id).await.unwrap();
    assert!(store.load(&original.upload_id).await.unwrap().is_none());
    assert!(
        store.save(&one).await.is_err(),
        "deleted session cannot resurrect"
    );
    assert!(
        store.save(&original).await.is_err(),
        "initial create cannot resurrect tombstone"
    );
    task.abort();
}
