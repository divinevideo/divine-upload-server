//! Fixed, narrow edge coordination client; no provider or worker credentials forwarded.
use super::*;
use std::time::Duration;

pub(super) const SCHEMA: &str = include_str!("../database/migrations/005_parent_gate_broker.sql");

#[derive(Clone)]
pub(super) struct Client {
    http: reqwest::Client,
    base: url::Url,
    token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Gate {
    Copying {
        attempt_id: String,
    },
    Erasing {},
    Released {
        version: u32,
        settled_attempt_id: String,
        released_from_generation: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Observed {
    gate: Gate,
    generation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Readback {
    version: u32,
    parent: String,
    attempt_id: String,
    observed: Option<Observed>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    Missing {},
    Released {
        generation: String,
        settled_attempt_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Claim {
    version: u32,
    parent: String,
    attempt_id: String,
    purpose: String,
    expected: Expected,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BeginClaim {
    pub lease: Lease,
    pub attempt_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inspect {
    pub attempt_id: String,
}

#[derive(Serialize)]
pub(super) struct Receipt {
    attempt_id: String,
    request: Option<serde_json::Value>,
    outcome: Option<serde_json::Value>,
    observation: Option<serde_json::Value>,
    dispatch_available: bool,
}

fn generation(value: &str) -> bool {
    value
        .parse::<u64>()
        .is_ok_and(|n| n > 0 && n.to_string() == value)
}
fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| (33..=126).contains(&b))
}
fn unknown() -> anyhow::Error {
    journal::JournalError::GateUnavailable.into()
}

impl Client {
    pub(super) fn configured(settings: &InboxSettings) -> Result<Option<Arc<Self>>> {
        Self::from_lookup(settings, |key| std::env::var(key).ok())
    }

    fn from_lookup(
        settings: &InboxSettings,
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<Arc<Self>>> {
        match lookup("MEDIA_PARENT_GATE_ENABLED").as_deref() {
            None | Some("false") => return Ok(None),
            Some("true") => {}
            _ => bail!("MEDIA_PARENT_GATE_ENABLED must be true or false"),
        }
        if !settings.producer_journal_enabled {
            bail!("parent gate requires producer journal");
        }
        let endpoint = lookup("MEDIA_PARENT_GATE_URL")
            .ok_or_else(|| anyhow!("parent gate endpoint required"))?;
        let token = lookup("MEDIA_PARENT_GATE_TOKEN")
            .ok_or_else(|| anyhow!("parent gate credential required"))?;
        if token == settings.worker_token || token == settings.intake_token {
            bail!("parent gate credential must be distinct");
        }
        Ok(Some(Arc::new(Self::new(&endpoint, token, false)?)))
    }
    fn new(endpoint: &str, token: String, loopback: bool) -> Result<Self> {
        let base =
            url::Url::parse(endpoint).map_err(|_| anyhow!("invalid parent gate endpoint"))?;
        let local = cfg!(test)
            && loopback
            && base.scheme() == "http"
            && base.host_str() == Some("127.0.0.1");
        if !(base.scheme() == "https" || local)
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path() != "/admin/api/b2-parent-gate/"
        {
            bail!("invalid parent gate endpoint");
        }
        if !(16..=1024).contains(&token.len()) || !token.bytes().all(|b| (33..=126).contains(&b)) {
            bail!("invalid parent gate credential");
        }
        let http = reqwest::Client::builder()
            // reqwest can replay protocol-refused HTTP/2 requests, and Hyper can
            // recover unstarted pooled requests. Use a fresh HTTP/1 connection
            // for this one-shot mutation boundary instead of those retry paths.
            .http1_only()
            .pool_max_idle_per_host(0)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(2))
            .no_proxy()
            .build()?;
        Ok(Self { http, base, token })
    }
    async fn call(&self, op: &str, body: &serde_json::Value) -> Result<Readback> {
        if !matches!(op, "claim" | "inspect" | "check") {
            return Err(unknown());
        }
        let url = self.base.join(op).map_err(|_| unknown())?;
        let mut response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|_| unknown())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(unknown());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unknown())? {
            if bytes.len() + chunk.len() > 8192 {
                return Err(unknown());
            }
            bytes.extend_from_slice(&chunk);
        }
        let fields: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if !fields
            .as_object()
            .is_some_and(|v| v.len() == 4 && v.contains_key("observed"))
        {
            return Err(unknown());
        }
        let result: Readback = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if result.version != 1
            || result.parent != body["parent"]
            || result.attempt_id != body["attempt_id"]
        {
            return Err(unknown());
        }
        if let Some(observed) = &result.observed {
            if !generation(&observed.generation) {
                return Err(unknown());
            }
            let valid = match &observed.gate {
                Gate::Copying { attempt_id } => identity(attempt_id),
                Gate::Erasing {} => true,
                Gate::Released {
                    version,
                    settled_attempt_id,
                    released_from_generation,
                } => {
                    *version == 1
                        && identity(settled_attempt_id)
                        && generation(released_from_generation)
                }
            };
            if !valid {
                return Err(unknown());
            }
        }
        Ok(result)
    }
}

impl InboxStore {
    async fn gate_receipt(&self, id: &str) -> Result<Receipt> {
        if !journal::valid_id(id) {
            return Err(journal::JournalError::Invalid.into());
        }
        let row=self.database.lock().await.query_opt("SELECT gate_claim_request,gate_claim_outcome,gate_observation FROM media_storage_attempts WHERE attempt_id=$1 AND subject_kind='producer'",&[&id]).await?.ok_or(journal::JournalError::Stale)?;
        Ok(Receipt {
            attempt_id: id.into(),
            request: row.get(0),
            outcome: row.get(1),
            observation: row.get(2),
            dispatch_available: false,
        })
    }

    pub(super) async fn gate_claim(&self, client: &Client, input: BeginClaim) -> Result<Receipt> {
        // Return the retained request before any HTTP. It permanently consumes
        // this attempt's mutation identity, even after intermediate owners.
        let old = self.gate_receipt(&input.attempt_id).await?;
        if old.request.is_some() {
            return Ok(old);
        }
        let attempt = {
            let mut db = self.database.lock().await;
            let tx = db.transaction().await?;
            journal::live_job(&tx, &input.lease).await?;
            let attempt = journal::read_attempt(&tx, &input.attempt_id)
                .await?
                .ok_or(journal::JournalError::Stale)?;
            if attempt.processing_job_id != input.lease.processing_job_id
                || attempt.lease_generation != input.lease.lease_generation
                || attempt.input_revision != input.lease.input_revision
                || attempt.phase != "planned"
            {
                return Err(journal::JournalError::Stale.into());
            }
            tx.commit().await?;
            attempt
        };
        let inspect =
            serde_json::json!({"version":1,"parent":attempt.sha256,"attempt_id":input.attempt_id});
        let current = client.call("inspect", &inspect).await?;
        let expected = match current.observed {
            None => Expected::Missing {},
            Some(Observed {
                gate: Gate::Released {
                    settled_attempt_id, ..
                },
                generation,
            }) if settled_attempt_id != input.attempt_id => Expected::Released {
                generation,
                settled_attempt_id,
            },
            _ => {
                let raced = self.gate_receipt(&input.attempt_id).await?;
                if raced.request.is_some() {
                    return Ok(raced);
                }
                return Err(unknown());
            }
        };
        let claim = serde_json::to_value(Claim {
            version: 1,
            parent: attempt.sha256,
            attempt_id: input.attempt_id.clone(),
            purpose: "existing_family".into(),
            expected,
        })?;
        let send = {
            let mut db = self.database.lock().await;
            let tx = db.transaction().await?;
            journal::live_job(&tx, &input.lease).await?;
            let changed=tx.execute("UPDATE media_storage_attempts SET gate_claim_request=$2 WHERE attempt_id=$1 AND gate_claim_request IS NULL",&[&input.attempt_id,&claim]).await?==1;
            tx.commit().await?;
            changed
        };
        if send {
            // Commit-before-HTTP. Cancellation, timeout or crash leaves a consumed
            // immutable request, never an automatically retryable admission.
            let outcome = match client.call("claim", &claim).await {
                Ok(readback) => serde_json::json!({"state":"observed","readback":readback}),
                Err(_) => serde_json::json!({"state":"unknown"}),
            };
            self.database.lock().await.execute("UPDATE media_storage_attempts SET gate_claim_outcome=$3 WHERE attempt_id=$1 AND gate_claim_request=$2 AND gate_claim_outcome IS NULL",&[&input.attempt_id,&claim,&outcome]).await?;
        }
        self.gate_receipt(&input.attempt_id).await
    }

    pub(super) async fn gate_inspect(&self, client: &Client, id: &str) -> Result<Receipt> {
        let receipt = self.gate_receipt(id).await?;
        let Some(request) = &receipt.request else {
            return Ok(receipt);
        };
        let claim: Claim = serde_json::from_value(request.clone())?;
        let readback = client
            .call(
                "inspect",
                &serde_json::json!({"version":1,"parent":claim.parent,"attempt_id":id}),
            )
            .await?;
        // Store the exact observation separately. Missing or another generation
        // is not a settled/cancelled claim; it never permits another send/release.
        let observation = serde_json::to_value(readback)?;
        self.database.lock().await.execute("UPDATE media_storage_attempts SET gate_observation=$2 WHERE attempt_id=$1 AND gate_claim_request=$3",&[&id,&observation,&request]).await?;
        self.gate_receipt(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        Json, Router,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn gate_client_refuses_credentialed_or_nonfixed_endpoints() {
        let settings = InboxSettings {
            intake_token: "fixture-intake-token".into(),
            worker_token: "fixture-worker-token".into(),
            producer_journal_enabled: true,
        };
        assert!(Client::from_lookup(&settings, |_| None).unwrap().is_none());
        for (flag, token) in [
            ("true", None),
            ("yes", None),
            ("true", Some("fixture-worker-token")),
            ("true", Some("fixture-intake-token")),
        ] {
            assert!(Client::from_lookup(&settings, |name| match name {
                "MEDIA_PARENT_GATE_ENABLED" => Some(flag.into()),
                "MEDIA_PARENT_GATE_URL" =>
                    Some("https://synthetic.invalid/admin/api/b2-parent-gate/".into()),
                "MEDIA_PARENT_GATE_TOKEN" => token.map(str::to_string),
                _ => None,
            })
            .is_err());
        }
        for endpoint in [
            "http://example.invalid/admin/api/b2-parent-gate/",
            "https://example.invalid/other/",
            "https://user:pass@example.invalid/admin/api/b2-parent-gate/",
            "https://example.invalid/admin/api/b2-parent-gate/?token=x",
            "https://example.invalid/admin/api/b2-parent-gate/#x",
        ] {
            assert!(Client::new(endpoint, "fixture-coordination-token".into(), false).is_err());
        }
        assert!(Client::new(
            "https://example.invalid/admin/api/b2-parent-gate/",
            "fixture-coordination-token".into(),
            false
        )
        .is_ok());
        for bad in ["0", "01", "-1", "18446744073709551616"] {
            assert!(!generation(bad));
        }
    }

    #[tokio::test]
    async fn gate_client_refuses_redirects_and_incomplete_or_foreign_readbacks() {
        use axum::response::IntoResponse;
        let hits = Arc::new(AtomicUsize::new(0));
        for case in 0..6 {
            let seen = hits.clone();
            let app=Router::new().route("/admin/api/b2-parent-gate/inspect",post(move |Json(body):Json<serde_json::Value>|async move{
                if case==0 {return (StatusCode::FOUND,[("location","/redirect-target")],"redirect").into_response();}
                let mut reply=serde_json::json!({"version":1,"parent":body["parent"],"attempt_id":body["attempt_id"],"observed":null});
                match case {
                    1=>{reply.as_object_mut().unwrap().remove("observed");},
                    2=>{reply["parent"]=serde_json::json!("foreign-parent");},
                    3=>{reply["observed"]=serde_json::json!({"gate":{"state":"erasing"},"generation":"0"});},
                    4=>{reply["observed"]=serde_json::json!({"gate":{"state":"erasing","unexpected":true},"generation":"19"});},
                    _=>{return (StatusCode::OK,"x".repeat(8193)).into_response();}
                }
                Json(reply).into_response()
            })).route("/redirect-target",axum::routing::get(move||{seen.fetch_add(1,Ordering::SeqCst);async{StatusCode::OK}}));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!(
                "http://127.0.0.1:{}/admin/api/b2-parent-gate/",
                listener.local_addr().unwrap().port()
            );
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let client = Client::new(&url, "fixture-coordination-token".into(), true).unwrap();
            assert!(client.call("inspect",&serde_json::json!({"version":1,"parent":"a".repeat(64),"attempt_id":"b".repeat(32)})).await.is_err(),"case {case}");
            task.abort();
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "coordination credential must not follow redirects"
        );
    }

    #[derive(Clone)]
    struct Edge {
        store: InboxStore,
        claims: Arc<AtomicUsize>,
        current: Arc<Mutex<Option<Observed>>>,
        delayed: bool,
    }
    async fn inspect(
        State(edge): State<Edge>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        assert_eq!(
            headers["authorization"],
            "Bearer fixture-coordination-token"
        );
        Json(
            serde_json::json!({"version":1,"parent":body["parent"],"attempt_id":body["attempt_id"],"observed":edge.current.lock().await.clone()}),
        )
    }
    async fn claim(
        State(edge): State<Edge>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        assert_eq!(
            headers["authorization"],
            "Bearer fixture-coordination-token"
        );
        let id = body["attempt_id"].as_str().unwrap();
        let row = edge
            .store
            .database
            .lock()
            .await
            .query_one(
                "SELECT gate_claim_request FROM media_storage_attempts WHERE attempt_id=$1",
                &[&id],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, serde_json::Value>(0),
            body,
            "mutation must already be committed on another connection"
        );
        edge.claims.fetch_add(1, Ordering::SeqCst);
        *edge.current.lock().await = Some(Observed {
            gate: Gate::Copying {
                attempt_id: id.into(),
            },
            generation: "91".into(),
        });
        if edge.delayed {
            tokio::time::sleep(Duration::from_secs(4)).await;
        }
        (
            StatusCode::OK,
            inspect(State(edge), headers, Json(body)).await,
        )
    }
    async fn fixture(
        delayed: bool,
    ) -> Option<(
        InboxStore,
        InboxStore,
        Assignment,
        String,
        Client,
        Edge,
        tokio::task::JoinHandle<()>,
    )> {
        let (one, two) = database_tests::stores().await?;
        let event = database_tests::event();
        one.accept(&event, &event.job_id).await.unwrap();
        let assignment = one.claim().await.unwrap().unwrap();
        let lease = Lease::from(&assignment);
        let begun = one.journal_begin(&lease).await.unwrap();
        let begun = serde_json::to_value(begun).unwrap();
        let id = begun["attempt_id"].as_str().unwrap().to_string();
        let artifacts=["stream_720p.ts","stream_720p.mp4","stream_480p.ts","stream_480p.mp4","stream_720p.m3u8","stream_480p.m3u8","master.m3u8"].iter().map(|filename|serde_json::json!({
            "filename":filename,"sha256":"d".repeat(64),"size":188,"content_type":if filename.ends_with(".ts"){"video/mp2t"}else if filename.ends_with(".mp4"){"video/mp4"}else{"application/vnd.apple.mpegurl"}
        })).collect::<Vec<_>>();
        let prepare = serde_json::from_value(
            serde_json::json!({"lease":lease,"attempt_id":id,"artifacts":artifacts}),
        )
        .unwrap();
        one.journal_prepare(prepare).await.unwrap();
        let edge = Edge {
            store: two.clone(),
            claims: Arc::new(AtomicUsize::new(0)),
            current: Arc::new(Mutex::new(None)),
            delayed,
        };
        let app = Router::new()
            .route("/admin/api/b2-parent-gate/inspect", post(inspect))
            .route("/admin/api/b2-parent-gate/claim", post(claim))
            .with_state(edge.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://127.0.0.1:{}/admin/api/b2-parent-gate/",
            listener.local_addr().unwrap().port()
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::new(&url, "fixture-coordination-token".into(), true).unwrap();
        Some((one, two, assignment, id, client, edge, server))
    }
    #[tokio::test]
    async fn broker_routes_keep_worker_auth_and_dispatch_closed() {
        use axum::{
            body::{to_bytes, Body},
            http::Request,
        };
        use tower::ServiceExt;
        let Some((one, _two, assignment, id, client, edge, server)) = fixture(false).await else {
            return;
        };
        let runtime = InboxRuntime {
            store: one,
            settings: Arc::new(InboxSettings {
                intake_token: "fixture-intake-token".into(),
                worker_token: "fixture-worker-token".into(),
                producer_journal_enabled: true,
            }),
            deadline: Duration::from_secs(10),
            parent_gate: Some(Arc::new(client)),
        };
        let app = router(Some(runtime));
        let path = "/internal/media-processing/attempts/gate/claim";
        let request = |path: &str, token: &str, body: Vec<u8>| {
            Request::post(path)
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap()
        };
        let denied = app
            .clone()
            .oneshot(request(path, "fixture-intake-token", vec![b'x'; 32769]))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(denied.headers()["cache-control"], "private, no-store");
        let mut payload = serde_json::json!({"lease":Lease::from(&assignment),"attempt_id":id,"purpose":"original_write"});
        assert_eq!(
            app.clone()
                .oneshot(request(
                    path,
                    "fixture-worker-token",
                    serde_json::to_vec(&payload).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(edge.claims.load(Ordering::SeqCst), 0);
        payload.as_object_mut().unwrap().remove("purpose");
        let response = app
            .clone()
            .oneshot(request(
                path,
                "fixture-worker-token",
                serde_json::to_vec(&payload).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 8192).await.unwrap();
        let receipt: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(receipt["gate"]["dispatch_available"], false);
        assert!(!String::from_utf8_lossy(&body).contains("fixture-coordination-token"));
        payload["filename"] = serde_json::json!("master.m3u8");
        let blocked = app
            .clone()
            .oneshot(request(
                "/internal/media-processing/attempts/dispatch",
                "fixture-worker-token",
                serde_json::to_vec(&payload).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        let complete = serde_json::json!({"lease":Lease::from(&assignment),"manifest":database_tests::marker(&assignment)});
        assert_eq!(
            app.oneshot(request(
                "/internal/media-processing/complete",
                "fixture-worker-token",
                serde_json::to_vec(&complete).unwrap()
            ))
            .await
            .unwrap()
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(edge.claims.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn gate_mutation_is_committed_once_across_two_connections() {
        let Some((one, two, assignment, id, client, edge, server)) = fixture(false).await else {
            return;
        };
        let input = || BeginClaim {
            lease: Lease::from(&assignment),
            attempt_id: id.clone(),
        };
        let (a, b) = tokio::join!(
            one.gate_claim(&client, input()),
            two.gate_claim(&client, input())
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(edge.claims.load(Ordering::SeqCst), 1);
        let receipt = two.gate_receipt(&id).await.unwrap();
        assert_eq!(receipt.outcome.as_ref().unwrap()["state"], "observed");
        assert!(!receipt.dispatch_available);
        assert!(one.journal_dispatch(serde_json::from_value(serde_json::json!({"lease":Lease::from(&assignment),"attempt_id":id,"filename":"master.m3u8"})).unwrap()).await.is_err());
        server.abort();
    }
    #[tokio::test]
    async fn lost_claim_response_and_intermediate_owners_never_resurrect_attempt() {
        let Some((one, two, assignment, id, client, edge, server)) = fixture(true).await else {
            return;
        };
        let input = || BeginClaim {
            lease: Lease::from(&assignment),
            attempt_id: id.clone(),
        };
        let receipt = one.gate_claim(&client, input()).await.unwrap();
        assert_eq!(receipt.outcome.as_ref().unwrap()["state"], "unknown");
        let reconciled = two.gate_inspect(&client, &id).await.unwrap();
        assert_eq!(
            reconciled.observation.as_ref().unwrap()["observed"]["generation"],
            "91"
        );
        assert_eq!(
            reconciled.outcome, receipt.outcome,
            "observation cannot erase historical timeout"
        );
        for next in [
            None,
            Some(Observed {
                gate: Gate::Released {
                    version: 1,
                    settled_attempt_id: "intermediate-owner".into(),
                    released_from_generation: "92".into(),
                },
                generation: "93".into(),
            }),
        ] {
            *edge.current.lock().await = next;
            let replay = two.gate_claim(&client, input()).await.unwrap();
            assert_eq!(replay.request, receipt.request);
            assert!(!replay.dispatch_available);
        }
        assert_eq!(edge.claims.load(Ordering::SeqCst), 1);
        server.abort();
    }
}
