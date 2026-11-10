//! PostgreSQL transport shared by resumable state and the processing outbox.
use anyhow::{anyhow, bail, Result};
use std::str::FromStr;
use tokio_postgres::{config::SslMode, Client, Config};

pub async fn connect(database_url: &str) -> Result<Client> {
    let mut config = Config::from_str(database_url)
        .map_err(|_| anyhow!("invalid coordination database configuration"))?;
    // Prefer must not silently send credentials without TLS after a server refusal.
    if config.get_ssl_mode() == SslMode::Prefer {
        config.ssl_mode(SslMode::Require);
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
    let tls = native_tls::TlsConnector::builder()
        .build()
        .map_err(|_| anyhow!("coordination TLS initialization failed"))?;
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
