#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ObjectRef;

    fn object(version: &str) -> ObjectRef {
        ObjectRef {
            provider: crate::storage::StorageProvider::B2,
            bucket: "private-media".into(),
            key: "a".repeat(64),
            version: Some(version.into()),
        }
    }

    #[test]
    fn job_identity_pins_version_and_pipeline() {
        let one = MediaFinalizedV1::new(
            &"a".repeat(64),
            3,
            "video/mp4",
            &"b".repeat(64),
            object("v1"),
        )
        .unwrap();
        let duplicate = MediaFinalizedV1::new(
            &"a".repeat(64),
            3,
            "video/mp4",
            &"b".repeat(64),
            object("v1"),
        )
        .unwrap();
        let changed = MediaFinalizedV1::new(
            &"a".repeat(64),
            3,
            "video/mp4",
            &"b".repeat(64),
            object("v2"),
        )
        .unwrap();
        assert_eq!(one.job_id, duplicate.job_id);
        assert_ne!(one.job_id, changed.job_id);
        assert_eq!(one.schema_version, 1);
        let another_owner = MediaFinalizedV1::new(
            &"a".repeat(64),
            3,
            "video/mp4",
            &"c".repeat(64),
            object("v1"),
        )
        .unwrap();
        assert_ne!(one.job_id, another_owner.job_id);
        let mut pipeline = one.clone();
        pipeline.pipeline_version = 2;
        pipeline.refresh_job_id().unwrap();
        assert_ne!(one.job_id, pipeline.job_id);
    }

    #[test]
    fn missing_version_and_wrong_hash_fail_before_queueing() {
        let mut source = object("v1");
        source.version = None;
        assert!(
            MediaFinalizedV1::new(&"a".repeat(64), 3, "video/mp4", &"b".repeat(64), source)
                .is_err()
        );
        assert!(MediaFinalizedV1::new(
            &"c".repeat(64),
            3,
            "video/mp4",
            &"b".repeat(64),
            object("v1")
        )
        .is_err());
    }

    #[tokio::test]
    async fn delivery_requires_matching_durable_acknowledgement() {
        use axum::{routing::post, Json, Router};
        let event = MediaFinalizedV1::new(
            &"a".repeat(64),
            3,
            "video/mp4",
            &"b".repeat(64),
            object("v1"),
        )
        .unwrap();
        let app = Router::new().route("/ok", post(|headers: axum::http::HeaderMap, Json(event): Json<MediaFinalizedV1>| async move {
            assert_eq!(headers.get("authorization").unwrap(), "Bearer fixture-token");
            Json(serde_json::json!({"schema_version":1,"job_id":event.job_id,"durable":true}))
        })).route("/wrong", post(||async {Json(serde_json::json!({"schema_version":1,"job_id":"wrong","durable":true}))}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            ProcessingClient::new_for_test(&format!("{base}/ok"), "fixture-token").unwrap();
        client.deliver(&event).await.unwrap();
        let wrong =
            ProcessingClient::new_for_test(&format!("{base}/wrong"), "fixture-token").unwrap();
        assert!(wrong.deliver(&event).await.is_err());
        server.abort();
    }
}

use crate::storage::ObjectRef;
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Internal processing input. Object versions are opaque provider identities, not ETags.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaFinalizedV1 {
    pub schema_version: u32,
    pub job_id: String,
    pub source: ObjectRef,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<ObjectRef>,
    pub sha256: String,
    pub size: u64,
    pub content_type: String,
    pub owner: Option<String>,
    pub pipeline_version: u32,
}

impl MediaFinalizedV1 {
    pub fn new(
        hash: &str,
        size: u64,
        content_type: &str,
        owner: &str,
        source: ObjectRef,
    ) -> Result<Self> {
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || source.key != hash
        {
            bail!("processing source hash is invalid");
        }
        if (!owner.is_empty()
            && (owner.len() != 64 || !owner.bytes().all(|b| b.is_ascii_hexdigit())))
            || content_type.is_empty()
            || source.bucket.is_empty()
            || source.version.as_deref().unwrap_or("").is_empty()
        {
            bail!("processing source metadata is incomplete");
        }
        // Canonical array encoding keeps opaque strings unambiguous and identity stable.
        let mut event = Self {
            schema_version: 1,
            job_id: String::new(),
            source,
            replicas: Vec::new(),
            sha256: hash.into(),
            size,
            content_type: content_type.into(),
            owner: if owner.is_empty() {
                None
            } else {
                Some(owner.into())
            },
            pipeline_version: 1,
        };
        event.refresh_job_id()?;
        Ok(event)
    }
    pub fn refresh_job_id(&mut self) -> Result<()> {
        self.job_id = hex::encode(Sha256::digest(serde_json::to_vec(&(
            self.pipeline_version,
            &self.source,
            &self.replicas,
            &self.owner,
        ))?));
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProcessingClient {
    client: reqwest::Client,
    endpoint: url::Url,
    token: String,
}

#[derive(Deserialize)]
struct DurableAck {
    schema_version: u32,
    job_id: String,
    durable: bool,
}

impl ProcessingClient {
    pub fn new(endpoint: &str, token: &str) -> Result<Self> {
        Self::build(endpoint, token, false)
    }

    fn build(endpoint: &str, token: &str, allow_http: bool) -> Result<Self> {
        let endpoint =
            url::Url::parse(endpoint).map_err(|_| anyhow!("invalid processing endpoint"))?;
        if (!allow_http && endpoint.scheme() != "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
            || endpoint.query().is_some()
            || token.trim().is_empty()
        {
            bail!("processing endpoint must be HTTPS without userinfo, query or fragment and have an authentication token");
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| anyhow!("processing HTTP client configuration failed"))?;
        Ok(Self {
            client,
            endpoint,
            token: token.to_owned(),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(endpoint: &str, token: &str) -> Result<Self> {
        Self::build(endpoint, token, true)
    }

    pub async fn deliver(&self, event: &MediaFinalizedV1) -> Result<()> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .header("Idempotency-Key", &event.job_id)
            .json(event)
            .send()
            .await
            .map_err(|_| anyhow!("processing delivery transport failed"))?;
        if !response.status().is_success() {
            bail!(
                "processing delivery returned HTTP {}",
                response.status().as_u16()
            );
        }
        // Bound provider-controlled responses, including chunked responses without length.
        if response.content_length().is_some_and(|size| size > 8192) {
            bail!("processing acknowledgement too large");
        }
        let mut response = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow!("processing acknowledgement read failed"))?
        {
            if bytes.len() + chunk.len() > 8192 {
                bail!("processing acknowledgement too large");
            }
            bytes.extend_from_slice(&chunk);
        }
        let ack: DurableAck = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow!("processing acknowledgement is invalid"))?;
        if ack.schema_version != 1 || ack.job_id != event.job_id || !ack.durable {
            bail!("processing consumer did not confirm durable acceptance");
        }
        Ok(())
    }
}
