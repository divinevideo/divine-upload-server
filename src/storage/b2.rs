use super::{ObjectRef, StorageProvider, StoredObject};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use reqwest::{Method, Response, StatusCode};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
use time::OffsetDateTime;

/// The endpoint is derived from the region. Redirects are disabled so credentials
/// are never forwarded to a provider-selected host. Secrets are not Debug.
#[derive(Clone)]
pub struct B2Store {
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
    client: reqwest::Client,
    read_client: reqwest::Client,
    tracker: std::sync::Arc<tokio::sync::Mutex<Option<tokio_postgres::Client>>>,
}
impl B2Store {
    pub fn new(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> Result<Self> {
        if bucket.is_empty()
            || !bucket
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            bail!("Invalid B2 bucket");
        }
        if region.is_empty()
            || region.len() > 63
            || region.starts_with('-')
            || region.ends_with('-')
            || !region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            bail!("Invalid B2 region");
        }
        if access_key.is_empty() || secret_key.is_empty() {
            bail!("B2 credentials required");
        }
        let endpoint = format!("https://s3.{region}.backblazeb2.com");
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()?;
        let read_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            bucket,
            region,
            access_key,
            secret_key,
            endpoint,
            client,
            read_client,
            tracker: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
    pub fn from_env() -> Result<Self> {
        let required = |name| std::env::var(name).map_err(|_| anyhow!("Missing {name}"));
        Self::new(
            required("B2_MEDIA_BUCKET")?,
            required("B2_REGION")?,
            required("B2_ACCESS_KEY_ID")?,
            required("B2_SECRET_ACCESS_KEY")?,
        )
    }
    pub async fn initialize_coordination(&self, database_url: &str) -> Result<()> {
        let client = crate::coordination::connect(database_url).await?;
        client
            .batch_execute(include_str!("multipart.sql"))
            .await
            .map_err(|_| anyhow!("B2 multipart coordination initialization failed"))?;
        *self.tracker.lock().await = Some(client);
        Ok(())
    }
    pub async fn attach_existing_coordination(&self, database_url: &str) -> Result<()> {
        let client = crate::coordination::connect(database_url).await?;
        let exists: bool = client
            .query_one(
                "SELECT to_regclass('b2_multipart_intents') IS NOT NULL",
                &[],
            )
            .await?
            .get(0);
        if exists {
            *self.tracker.lock().await = Some(client);
        }
        Ok(())
    }
    pub async fn ensure_coordination(&self, database_url: &str) -> Result<()> {
        let attached = self.tracker.lock().await.is_some();
        if !attached {
            self.initialize_coordination(database_url).await?;
        }
        Ok(())
    }
    async fn track(&self, key: &str, upload_id: &str, remove: bool) -> Result<()> {
        let guard = self.tracker.lock().await;
        if let Some(client) = guard.as_ref() {
            if remove {
                client.execute("DELETE FROM b2_multipart_intents WHERE bucket=$1 AND object_key=$2 AND upload_id=$3",&[&self.bucket,&key,&upload_id]).await.map_err(|_|anyhow!("B2 multipart state update failed"))?;
            } else {
                client.execute("INSERT INTO b2_multipart_intents(bucket,object_key,upload_id,lease_expires) VALUES($1,$2,$3,now()+interval '10 minutes') ON CONFLICT(bucket,object_key,upload_id) DO UPDATE SET lease_expires=EXCLUDED.lease_expires",&[&self.bucket,&key,&upload_id]).await.map_err(|_|anyhow!("B2 multipart state update failed"))?;
            }
        }
        Ok(())
    }
    pub async fn sweep_stale_multipart(&self, limit: u32) -> Result<u64> {
        let rows = {
            let guard = self.tracker.lock().await;
            let Some(client) = guard.as_ref() else {
                return Ok(0);
            };
            client.query("WITH candidates AS (SELECT bucket,object_key,upload_id FROM b2_multipart_intents WHERE bucket=$1 AND lease_expires<now() ORDER BY lease_expires LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE b2_multipart_intents i SET lease_expires=now()+interval '10 minutes' FROM candidates c WHERE i.bucket=c.bucket AND i.object_key=c.object_key AND i.upload_id=c.upload_id RETURNING i.object_key,i.upload_id",&[&self.bucket,&i64::from(limit.min(100))]).await.map_err(|_|anyhow!("B2 multipart cleanup discovery failed"))?
        };
        let mut removed = 0;
        for row in rows {
            let key: String = row.get(0);
            let id: String = row.get(1);
            let response = self
                .request_query(
                    Method::DELETE,
                    &key,
                    &format!("uploadId={}", encode(&id)),
                    Bytes::new(),
                    None,
                    None,
                )
                .await?;
            if response.status() != StatusCode::NOT_FOUND {
                require_success(&response)?;
            }
            self.track(&key, &id, true).await?;
            removed += 1;
        }
        Ok(removed)
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    fn reference(&self, key: &str, version: String) -> ObjectRef {
        ObjectRef {
            provider: StorageProvider::B2,
            bucket: self.bucket.clone(),
            key: key.into(),
            version: Some(version),
        }
    }
    async fn request(
        &self,
        method: Method,
        key: &str,
        version: Option<&str>,
        body: Bytes,
        content_type: Option<&str>,
        owner: Option<&str>,
    ) -> Result<Response> {
        let query = version
            .map(|v| format!("versionId={}", encode(v)))
            .unwrap_or_default();
        self.request_query(method, key, &query, body, content_type, owner)
            .await
    }
    async fn request_query(
        &self,
        method: Method,
        key: &str,
        query: &str,
        body: Bytes,
        content_type: Option<&str>,
        owner: Option<&str>,
    ) -> Result<Response> {
        if key.is_empty() || key.split('/').any(|s| s == "." || s == "..") {
            bail!("Invalid B2 object key");
        }
        let path = format!(
            "/{}/{}",
            encode(&self.bucket),
            key.split('/').map(encode).collect::<Vec<_>>().join("/")
        );
        let url = format!(
            "{}{path}{}",
            self.endpoint,
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        let now = OffsetDateTime::now_utc();
        let date = format!(
            "{:04}{:02}{:02}",
            now.year(),
            u8::from(now.month()),
            now.day()
        );
        let timestamp = format!(
            "{date}T{:02}{:02}{:02}Z",
            now.hour(),
            now.minute(),
            now.second()
        );
        let host = url::Url::parse(&self.endpoint)?.authority().to_string();
        let payload_hash = hex::encode(Sha256::digest(&body));
        let mut headers = BTreeMap::new();
        headers.insert("host", host);
        headers.insert("x-amz-content-sha256", payload_hash.clone());
        headers.insert("x-amz-date", timestamp.clone());
        if let Some(mime) = content_type {
            headers.insert("content-type", mime.into());
        }
        if let Some(owner) = owner {
            headers.insert("x-amz-meta-owner", owner.into());
        }
        let (canonical, signed_headers) =
            canonical_request(method.as_str(), &path, query, &headers, &payload_hash);
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let signature = s3_signature(
            &self.secret_key,
            &self.region,
            &date,
            &timestamp,
            &canonical,
        );
        let authorization=format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",self.access_key);
        let client = if method == Method::GET {
            &self.read_client
        } else {
            &self.client
        };
        let mut req = client
            .request(method, url)
            .header("authorization", authorization);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        // GET bodies use a per-chunk stall deadline, not a whole-object deadline.
        // Headers and control requests still have a bounded response wait.
        tokio::time::timeout(Duration::from_secs(120), req.body(body).send())
            .await
            .map_err(|_| anyhow!("B2 response headers timed out"))?
            .map_err(|_| anyhow!("B2 transport failure; write outcome may require reconciliation"))
    }
    pub async fn stat(&self, key: &str) -> Result<Option<StoredObject>> {
        self.stat_at(key, None).await
    }
    pub(super) async fn stat_reference(
        &self,
        reference: &ObjectRef,
    ) -> Result<Option<StoredObject>> {
        self.validate_ref(reference)?;
        if reference.version.as_deref() == Some("null") {
            bail!("Invalid B2 immutable version");
        }
        self.stat_at(&reference.key, reference.version.as_deref())
            .await
    }
    async fn stat_at(
        &self,
        key: &str,
        expected_version: Option<&str>,
    ) -> Result<Option<StoredObject>> {
        let response = self
            .request(
                Method::HEAD,
                key,
                expected_version,
                Bytes::new(),
                None,
                None,
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        require_success(&response)?;
        if expected_version.is_some() && response.status() != StatusCode::OK {
            bail!("Invalid B2 metadata status");
        }
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let version = header("x-amz-version-id")
            .filter(|v| !v.is_empty() && v != "null")
            .ok_or_else(|| anyhow!("B2 response omitted immutable version"))?;
        if expected_version.is_some_and(|expected| expected != version) {
            bail!("B2 metadata version mismatch");
        }
        let size = header("content-length")
            .ok_or_else(|| anyhow!("B2 response omitted content length"))?
            .parse()
            .map_err(|_| anyhow!("B2 response has invalid content length"))?;
        Ok(Some(StoredObject {
            reference: self.reference(key, version),
            size,
            content_type: header("content-type")
                .unwrap_or_else(|| "application/octet-stream".into()),
            owner: header("x-amz-meta-owner"),
        }))
    }
    pub async fn read(&self, reference: &ObjectRef) -> Result<Bytes> {
        self.validate_ref(reference)?;
        let response = self
            .request(
                Method::GET,
                &reference.key,
                reference.version.as_deref(),
                Bytes::new(),
                None,
                None,
            )
            .await?;
        require_success(&response)?;
        if response
            .headers()
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
            != reference.version.as_deref()
        {
            bail!("B2 response version mismatch");
        }
        use futures::TryStreamExt;
        body_stream(response, Duration::from_secs(30))
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .map(Bytes::from)
    }
    pub async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<(ObjectRef, bool)> {
        self.put_inner(key, bytes, content_type, owner, None).await
    }
    async fn put_inner(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
        owner: Option<&str>,
        admission: Option<&super::primary::Admission>,
    ) -> Result<(ObjectRef, bool)> {
        if let Some(o) = self.stat(key).await? {
            if let Some(a) = admission {
                a.acknowledged(&o.reference).await?;
            }
            self.verify(&o.reference, &bytes).await?;
            if let Some(a) = admission {
                a.verified(&o.reference).await?;
            }
            return Ok((o.reference, false));
        }
        if let Some(a) = admission {
            a.issued(StorageProvider::B2).await?;
        }
        let result = self
            .request(
                Method::PUT,
                key,
                None,
                bytes.clone(),
                Some(content_type),
                owner,
            )
            .await;
        let reference = match result {
            Ok(response) if response.status().is_success() => {
                let version = response
                    .headers()
                    .get("x-amz-version-id")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty() && *v != "null")
                    .ok_or_else(|| anyhow!("B2 write outcome unresolved: version missing"))?;
                self.reference(key, version.into())
            }
            Ok(response) => {
                require_success(&response)?;
                unreachable!()
            }
            Err(_) if admission.is_some() => {
                bail!("B2 write outcome unresolved; retain pending admission")
            }
            Err(_) => {
                let existing = self
                    .stat(key)
                    .await?
                    .ok_or_else(|| anyhow!("B2 write outcome unresolved"))?;
                self.verify(&existing.reference, &bytes).await?;
                return Ok((existing.reference, false));
            }
        };
        if let Some(a) = admission {
            a.acknowledged(&reference).await?;
        }
        self.verify(&reference, &bytes).await?;
        if let Some(a) = admission {
            a.verified(&reference).await?;
        }
        Ok((reference, true))
    }
    pub(super) async fn verify(&self, reference: &ObjectRef, bytes: &Bytes) -> Result<()> {
        use futures::StreamExt;
        self.validate_ref(reference)?;
        let response = self
            .request(
                Method::GET,
                &reference.key,
                reference.version.as_deref(),
                Bytes::new(),
                None,
                None,
            )
            .await?;
        require_success(&response)?;
        if response
            .headers()
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
            != reference.version.as_deref()
        {
            bail!("B2 response version mismatch");
        }
        if response.content_length() != Some(bytes.len() as u64) {
            bail!("B2 stored media length mismatch");
        }
        let mut stream = body_stream(response, Duration::from_secs(30));
        let mut total = 0usize;
        let mut hash = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("B2 verification body failed"))?;
            total = total
                .checked_add(chunk.len())
                .ok_or_else(|| anyhow!("B2 length overflow"))?;
            if total > bytes.len() {
                bail!("B2 stored media length mismatch");
            }
            hash.update(&chunk);
        }
        if total != bytes.len() || hash.finalize() != Sha256::digest(bytes) {
            bail!("B2 stored media integrity verification failed");
        }
        Ok(())
    }
    fn validate_ref(&self, reference: &ObjectRef) -> Result<()> {
        if reference.provider != StorageProvider::B2
            || reference.bucket != self.bucket
            || reference.version.as_deref().is_none_or(str::is_empty)
            || reference.version.as_deref() == Some("null")
        {
            bail!("Invalid B2 object reference");
        }
        Ok(())
    }
    pub async fn delete(&self, reference: &ObjectRef) -> Result<()> {
        self.validate_ref(reference)?;
        let response = self
            .request(
                Method::DELETE,
                &reference.key,
                reference.version.as_deref(),
                Bytes::new(),
                None,
                None,
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        require_success(&response)
    }
    pub async fn stream(
        &self,
        reference: &ObjectRef,
    ) -> Result<futures::stream::BoxStream<'static, Result<Bytes>>> {
        self.validate_ref(reference)?;
        let response = self
            .request(
                Method::GET,
                &reference.key,
                reference.version.as_deref(),
                Bytes::new(),
                None,
                None,
            )
            .await?;
        require_success(&response)?;
        if response
            .headers()
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
            != reference.version.as_deref()
        {
            bail!("B2 response version mismatch");
        }
        Ok(body_stream(response, Duration::from_secs(30)))
    }
    pub async fn put_file(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: &str,
        owner: Option<&str>,
    ) -> Result<(ObjectRef, bool)> {
        self.put_file_inner(key, path, content_type, owner, None)
            .await
    }
    pub(super) async fn put_file_admitted(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: &str,
        owner: Option<&str>,
        admission: &super::primary::Admission,
    ) -> Result<(ObjectRef, bool)> {
        self.put_file_inner(key, path, content_type, owner, Some(admission))
            .await
    }
    async fn put_file_inner(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: &str,
        owner: Option<&str>,
        admission: Option<&super::primary::Admission>,
    ) -> Result<(ObjectRef, bool)> {
        use tokio::io::AsyncReadExt;
        let size = tokio::fs::metadata(path).await?.len();
        if let Some(existing) = self.stat(key).await? {
            if let Some(a) = admission {
                a.acknowledged(&existing.reference).await?;
            }
            super::verify_stream(self.stream(&existing.reference).await?, path).await?;
            if let Some(a) = admission {
                a.verified(&existing.reference).await?;
            }
            return Ok((existing.reference, false));
        }
        const PART: usize = 8 * 1024 * 1024;
        if size <= PART as u64 {
            return self
                .put_inner(
                    key,
                    Bytes::from(tokio::fs::read(path).await?),
                    content_type,
                    owner,
                    admission,
                )
                .await;
        }
        if size > 10000 * PART as u64 {
            bail!("B2 upload exceeds 10000-part limit");
        }
        if let Some(a) = admission {
            a.issued(StorageProvider::B2).await?;
        }
        let response = self
            .request_query(
                Method::POST,
                key,
                "uploads=",
                Bytes::new(),
                Some(content_type),
                owner,
            )
            .await?;
        require_success(&response)?;
        let xml = control_body(response).await?;
        let upload_id = xml_field(&xml, "UploadId")?;
        if let Some(a) = admission {
            a.multipart(&upload_id).await?;
        }
        let upload_query = format!("uploadId={}", encode(&upload_id));
        if admission.is_none() {
            if let Err(error) = self.track(key, &upload_id, false).await {
                let _ = self
                    .request_query(Method::DELETE, key, &upload_query, Bytes::new(), None, None)
                    .await;
                return Err(error);
            }
        }
        let result = async {
            let mut file = tokio::fs::File::open(path).await?;
            let mut parts = String::from("<CompleteMultipartUpload>");
            let mut total = 0u64;
            let mut number = 1;
            loop {
                let mut part = vec![0u8; PART];
                let mut length = 0;
                while length < PART {
                    let read = file.read(&mut part[length..]).await?;
                    if read == 0 {
                        break;
                    }
                    length += read;
                }
                if length == 0 {
                    break;
                }
                part.truncate(length);
                total += length as u64;
                if admission.is_none() {
                    self.track(key, &upload_id, false).await?;
                }
                let query = format!("partNumber={number}&{upload_query}");
                let response = self
                    .request_query(Method::PUT, key, &query, Bytes::from(part), None, None)
                    .await?;
                require_success(&response)?;
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| anyhow!("B2 part omitted ETag"))?;
                parts.push_str(&format!(
                    "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                    escape_xml(etag)
                ));
                number += 1;
            }
            if total != size {
                bail!("Upload source changed while reading");
            }
            parts.push_str("</CompleteMultipartUpload>");
            let response = self
                .request_query(
                    Method::POST,
                    key,
                    &upload_query,
                    Bytes::from(parts),
                    Some("application/xml"),
                    None,
                )
                .await?;
            require_success(&response)?;
            let version = response
                .headers()
                .get("x-amz-version-id")
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty() && *v != "null")
                .map(str::to_owned);
            let acknowledged = version
                .as_ref()
                .map(|version| self.reference(key, version.clone()));
            if let (Some(a), Some(reference)) = (admission, acknowledged.as_ref()) {
                a.acknowledged(reference).await?;
            }
            let body = control_body(response).await?;
            // Some S3-compatible services return an XML Error in a 200 response.
            let document = roxmltree::Document::parse(&body)
                .map_err(|_| anyhow!("Malformed B2 completion response"))?;
            if document.root_element().tag_name().name() != "CompleteMultipartUploadResult" {
                bail!("B2 multipart completion failed");
            }
            let reference = self.reference(
                key,
                version.ok_or_else(|| anyhow!("B2 completion omitted immutable version"))?,
            );
            super::verify_stream(self.stream(&reference).await?, path).await?;
            if let Some(a) = admission {
                a.verified(&reference).await?;
            }
            Ok((reference, true))
        }
        .await;
        if result.is_err() {
            // Aborting an already-completed upload cannot remove the resulting
            // object version. A later intent reconciliation verifies that object.
            if let Ok(response) = self
                .request_query(Method::DELETE, key, &upload_query, Bytes::new(), None, None)
                .await
            {
                if admission.is_none()
                    && (response.status().is_success()
                        || response.status() == StatusCode::NOT_FOUND)
                {
                    let _ = self.track(key, &upload_id, true).await;
                }
            }
        } else if admission.is_none() {
            self.track(key, &upload_id, true).await?;
        }
        result
    }
    #[cfg(test)]
    pub(crate) fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }
}
fn require_success(response: &Response) -> Result<()> {
    if !response.status().is_success() {
        bail!("B2 request failed with HTTP {}", response.status().as_u16());
    }
    Ok(())
}

fn body_stream(
    response: Response,
    stall: Duration,
) -> futures::stream::BoxStream<'static, Result<Bytes>> {
    use futures::StreamExt;
    futures::stream::try_unfold(response.bytes_stream(), move |mut stream| async move {
        match tokio::time::timeout(stall, stream.next()).await {
            Ok(Some(chunk)) => Ok(Some((
                chunk.map_err(|_| anyhow!("B2 body read failed"))?,
                stream,
            ))),
            Ok(None) => Ok(None),
            Err(_) => Err(anyhow!("B2 body stalled")),
        }
    })
    .boxed()
}
fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &BTreeMap<&str, String>,
    payload_hash: &str,
) -> (String, String) {
    let canonical_headers = headers
        .iter()
        .map(|(k, v)| {
            format!(
                "{k}:{}\n",
                v.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        })
        .collect::<String>();
    let signed_headers = headers.keys().copied().collect::<Vec<_>>().join(";");
    (
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"),
        signed_headers,
    )
}
fn s3_signature(
    secret_key: &str,
    region: &str,
    date: &str,
    timestamp: &str,
    canonical: &str,
) -> String {
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let k_date = hmac(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    hex::encode(hmac(&k_signing, string_to_sign.as_bytes()))
}
fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
fn encode(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

async fn control_body(response: Response) -> Result<String> {
    use futures::StreamExt;
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow!("B2 control body read failed"))?;
        if body.len() + chunk.len() > 1024 * 1024 {
            bail!("B2 control response too large");
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|_| anyhow!("Invalid B2 control response"))
}
fn xml_field(xml: &str, name: &str) -> Result<String> {
    let document =
        roxmltree::Document::parse(xml).map_err(|_| anyhow!("Malformed B2 control response"))?;
    let mut found = document
        .descendants()
        .filter(|n| n.is_element() && n.tag_name().name() == name);
    let value = found
        .next()
        .and_then(|n| n.text())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("B2 control response missing field"))?;
    if found.next().is_some() {
        bail!("Ambiguous B2 control response");
    }
    Ok(value.into())
}
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod signing_tests {
    use super::*;
    #[tokio::test]
    async fn read_stall_deadline_resets_on_progress() {
        use axum::{body::Body, routing::get, Router};
        use futures::TryStreamExt;
        let app = Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(futures::stream::unfold(0, |index| async move {
                    if index == 6 {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some((
                        Ok::<_, std::convert::Infallible>(Bytes::from_static(b"x")),
                        index + 1,
                    ))
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = reqwest::Client::new().get(endpoint).send().await.unwrap();
        let bytes: Vec<Bytes> = body_stream(response, Duration::from_millis(500))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(bytes.iter().map(Bytes::len).sum::<usize>(), 6);
        server.abort();
    }

    #[tokio::test]
    async fn read_stall_deadline_rejects_a_stopped_body() {
        use axum::{body::Body, routing::get, Router};
        use futures::TryStreamExt;
        let app = Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(futures::stream::once(async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok::<_, std::convert::Infallible>(Bytes::from_static(b"late"))
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = reqwest::Client::new().get(endpoint).send().await.unwrap();
        let error = body_stream(response, Duration::from_millis(500))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "B2 body stalled");
        server.abort();
    }
    #[test]
    fn aws_published_s3_get_signature_matches() {
        // AWS's published GET Object known-answer vector, using its public example key:
        // https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html#example-signature-calculations
        let payload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = BTreeMap::from([
            ("host", "examplebucket.s3.amazonaws.com".to_string()),
            ("range", "bytes=0-9".to_string()),
            ("x-amz-content-sha256", payload.to_string()),
            ("x-amz-date", "20130524T000000Z".to_string()),
        ]);
        let (canonical, signed) = canonical_request("GET", "/test.txt", "", &headers, payload);
        assert_eq!(signed, "host;range;x-amz-content-sha256;x-amz-date");
        assert_eq!(
            hex::encode(Sha256::digest(canonical.as_bytes())),
            "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
        );
        assert_eq!(
            s3_signature(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "us-east-1",
                "20130524",
                "20130524T000000Z",
                &canonical
            ),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        assert_eq!(encode("a b/+"), "a%20b%2F%2B");
    }
}
