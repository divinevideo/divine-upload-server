//! PostgreSQL transport shared by resumable state and the processing outbox.
use anyhow::{anyhow, bail, Result};
use std::str::FromStr;
use tokio_postgres::{config::SslMode, Client, Config};

fn tls_connector(ca_file: Option<&std::path::Path>) -> Result<native_tls::TlsConnector> {
    use std::io::Read;
    let mut builder = native_tls::TlsConnector::builder();
    if let Some(path) = ca_file {
        let file = std::fs::File::open(path)
            .map_err(|_| anyhow!("coordination CA file could not be read"))?;
        if !file
            .metadata()
            .map_err(|_| anyhow!("coordination CA file could not be read"))?
            .is_file()
        {
            bail!("invalid coordination CA file");
        }
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| anyhow!("coordination CA file could not be read"))?;
        if bytes.is_empty() || bytes.len() > 1024 * 1024 {
            bail!("invalid coordination CA bundle");
        }
        let mut remaining = std::str::from_utf8(&bytes)
            .map_err(|_| anyhow!("invalid coordination CA bundle"))?
            .trim();
        if remaining.is_empty() {
            bail!("invalid coordination CA bundle");
        }
        const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
        const END: &str = "-----END CERTIFICATE-----";
        while !remaining.is_empty() {
            if !remaining.starts_with(BEGIN) {
                bail!("invalid coordination CA bundle");
            }
            let end = remaining
                .find(END)
                .ok_or_else(|| anyhow!("invalid coordination CA bundle"))?
                + END.len();
            let certificate = native_tls::Certificate::from_pem(remaining[..end].as_bytes())
                .map_err(|_| anyhow!("invalid coordination CA certificate"))?;
            builder.add_root_certificate(certificate);
            remaining = remaining[end..].trim();
        }
    }
    // Keep OS roots, chain verification and hostname verification enabled.
    builder
        .build()
        .map_err(|_| anyhow!("coordination TLS initialization failed"))
}

#[cfg(test)]
#[path = "coordination_tests.rs"]
mod tests;

pub async fn connect(database_url: &str) -> Result<Client> {
    let ca_file = std::env::var_os("COORDINATION_DATABASE_CA_FILE").map(std::path::PathBuf::from);
    let config = database_configuration(database_url, ca_file.is_some())?;
    let tls = tls_connector(ca_file.as_deref())?;
    let (client, connection) = config
        .connect(postgres_native_tls::MakeTlsConnector::new(tls))
        .await
        .map_err(|_| anyhow!("coordination database connection failed"))?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!(
                "coordination database connection closed; restarting to restore durable workers"
            );
            #[cfg(not(test))]
            std::process::exit(1);
        }
    });
    Ok(client)
}

fn database_configuration(database_url: &str, ca_configured: bool) -> Result<Config> {
    let mut config = Config::from_str(database_url)
        .map_err(|_| anyhow!("invalid coordination database configuration"))?;
    // Prefer must not silently send credentials without TLS after a server refusal.
    if config.get_ssl_mode() == SslMode::Prefer {
        config.ssl_mode(SslMode::Require);
    }
    if ca_configured && config.get_ssl_mode() == SslMode::Disable {
        bail!("coordination CA configuration requires TLS");
    }
    if config.get_ssl_mode() == SslMode::Disable
        && !config.get_hosts().iter().all(|host| match host {
            tokio_postgres::config::Host::Tcp(host) => {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            }
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => true,
        })
    {
        bail!("unencrypted coordination database connections require a local endpoint");
    }
    Ok(config)
}
