pub mod action;
pub mod auth;
pub mod circuit_breaker;
pub mod config;
pub mod db;
pub use db as db_operation;
pub mod dtos;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod models;
pub mod rule;
pub(crate) mod schema;
pub mod tracing;
pub mod validation;
pub mod workers;

use diesel::{ConnectionError, ConnectionResult};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::ManagerConfig;
use diesel_async::pooled_connection::bb8::PooledConnection;
use diesel_async::pooled_connection::bb8::{self, Pool};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use rustls::ClientConfig;
use rustls_platform_verifier::ConfigVerifierExt;

pub type DbPool = bb8::Pool<AsyncPgConnection>;

pub type Conn<'a> = PooledConnection<'a, AsyncPgConnection>;

/// Initialize database connection pool based on `DATABASE_URL` environment variable.
///
/// See more: <https://docs.rs/diesel/latest/diesel/r2d2/index.html>.
pub async fn initialize_db_pool(pool_config: &config::PoolConfig) -> DbPool {
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(establish_connection);
    let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(db_url, config);

    Pool::builder()
        .max_size(pool_config.max_size)
        .min_idle(Some(pool_config.min_idle))
        .max_lifetime(Some(pool_config.max_lifetime))
        .idle_timeout(Some(pool_config.idle_timeout))
        .connection_timeout(pool_config.connection_timeout)
        .build(mgr)
        .await
        .expect("failed to get pool")
}

fn establish_connection(config: &'_ str) -> BoxFuture<'_, ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = ClientConfig::with_platform_verifier()
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);
        let (client, conn) = tokio_postgres::connect(config, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        AsyncPgConnection::try_from_client_and_connection(client, conn).await
    };
    fut.boxed()
}
