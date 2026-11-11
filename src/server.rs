use anyhow::Result;
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::{conn::auto::Builder, graceful::GracefulShutdown},
};
use std::{future::Future, time::Duration};
use tokio::net::TcpListener;
use tower::Service;
use tracing::{error, info};

pub async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = Result<()>>,
    drain_timeout: Duration,
) -> Result<()> {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            biased;
            result = &mut shutdown => {
                result?;
                break;
            },
            accepted = listener.accept() => accepted?.0,
        };
        let app = app.clone();
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let builder = Builder::new(TokioExecutor::new());
            let connection = builder.serve_connection(
                TokioIo::new(stream),
                hyper::service::service_fn(move |request| {
                    let mut app = app.clone();
                    async move { app.call(request).await }
                }),
            );
            if let Err(error) = watcher.watch(connection).await {
                error!("Connection error: {}", error);
            }
        });
    }
    drop(listener);
    info!("Draining {} upload connections", graceful.count());
    tokio::time::timeout(drain_timeout, graceful.shutdown()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::put};
    use std::sync::Arc;
    use tokio::sync::{oneshot, Notify};

    #[derive(Clone, Default)]
    struct Upload {
        started: Arc<Notify>,
        complete: Arc<Notify>,
    }

    async fn upload(State(upload): State<Upload>) -> &'static str {
        upload.started.notify_one();
        upload.complete.notified().await;
        "stored"
    }

    #[tokio::test]
    async fn shutdown_drains_uploads_on_http1_and_http2() {
        for http2 in [false, true] {
            let state = Upload::default();
            let app = Router::new()
                .route("/upload", put(upload))
                .with_state(state.clone());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, shutdown_requested) = oneshot::channel();
            let server = tokio::spawn(serve(
                listener,
                app,
                async { shutdown_requested.await.map_err(Into::into) },
                Duration::from_secs(5),
            ));
            let client = reqwest::Client::builder();
            let client = if http2 {
                client.http2_prior_knowledge()
            } else {
                client
            };
            let request = tokio::spawn(async move {
                client
                    .build()
                    .unwrap()
                    .put(format!("http://{address}/upload"))
                    .send()
                    .await
                    .unwrap()
            });
            tokio::time::timeout(Duration::from_secs(5), state.started.notified())
                .await
                .unwrap();
            shutdown.send(()).unwrap();
            tokio::task::yield_now().await;
            assert!(!server.is_finished(), "shutdown must wait for the upload");
            state.complete.notify_one();
            let response = request.await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(response.text().await.unwrap(), "stored");
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
    }

    #[tokio::test]
    async fn shutdown_has_a_bounded_drain_deadline() {
        let state = Upload::default();
        let app = Router::new()
            .route("/upload", put(upload))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_requested) = oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            app,
            async { shutdown_requested.await.map_err(Into::into) },
            Duration::ZERO,
        ));
        let request = tokio::spawn(async move {
            reqwest::Client::new()
                .put(format!("http://{address}/upload"))
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), state.started.notified())
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        state.complete.notify_one();
        request.await.unwrap().unwrap();
    }
}
