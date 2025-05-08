use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use clickhouse::Client;
use futures::Future;
use eyre::WrapErr;
use crate::models::NormalizedEvent;

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url:      String,
    pub port:     String,
    pub user:     String,
    pub database: String,
    pub password: String,
}

impl ClickHouseConfig {
    fn get_env_var(key: &str) -> eyre::Result<String> {
        std::env::var(key).wrap_err(format!("Clickhouse config: {key} environment variable is not set"))
    }

    pub fn from_env() -> eyre::Result<Self> {
        Ok(Self {
            url:      Self::get_env_var("CLICKHOUSE_URL")?,
            port:     Self::get_env_var("CLICKHOUSE_PORT")?,
            user:     Self::get_env_var("CLICKHOUSE_USER")?,
            database: Self::get_env_var("CLICKHOUSE_DATABASE")?,
            password: Self::get_env_var("CLICKHOUSE_PASS")?,
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.url, self.port)
    }
}
#[derive(Clone)]
pub struct ClickHouseService {
    client: Client,
}

impl ClickHouseService {
    pub fn new(config: ClickHouseConfig) -> Self {
        let client = Client::default()
            .with_url(config.url())
            .with_user(config.user)
            .with_password(config.password)
            .with_database(config.database);
        Self { client }
    }

    async fn write_batch(&self, events: Vec<NormalizedEvent>) -> eyre::Result<()> {
        let mut trade_inserter = self
            .client
            .inserter("normalized_trades")?
            .with_max_rows(100)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        let mut quote_inserter = self
            .client
            .inserter("normalized_quotes")?
            .with_max_rows(100)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        for event in events {
            match event {
                NormalizedEvent::Trade(trade) => {
                    trade_inserter.write(&trade)?;
                    trade_inserter.commit().await?;
                }
                NormalizedEvent::Quote(quote) => {
                    quote_inserter.write(&quote)?;
                    quote_inserter.commit().await?;
                }
            }
        }
        trade_inserter.end().await?;
        quote_inserter.end().await?;
        Ok(())
    }
}


impl tower::Service<Vec<NormalizedEvent>> for ClickHouseService {
    type Response = ();
    type Error    = eyre::Error;
    type Future   = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, batch: Vec<NormalizedEvent>) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            // if write_batch fails, this returns Err(eyre::Error)
            svc.write_batch(batch).await
              .map_err(|e| eyre::eyre!("clickhouse write failed: {}", e))?;
            // on success:
            Ok(())
        })
    }
}