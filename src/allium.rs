use crate::clickhouse::{ClickHouseConfig, ClickHouseService};
use crate::config::AlliumConfig;
use crate::models::ClickhouseMessage;
use chrono::{DateTime, Utc};
use clickhouse::Row;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

mod allium_serde {
    use chrono::{DateTime, Utc};
    use serde::Deserialize;

    pub fn deserialize_period<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Parse naive datetime and assume UTC
        let naive = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S")
            .map_err(serde::de::Error::custom)?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    }

    pub fn serialize_volume_usd<S>(volume: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match volume {
            Some(value) => serializer.serialize_f64(*value),
            None => serializer.serialize_f64(0.0),
        }
    }
}

#[derive(Serialize, Deserialize, Row, Debug, Clone)]
pub struct DexVolume {
    #[serde(
        deserialize_with = "allium_serde::deserialize_period",
        serialize_with = "clickhouse::serde::chrono::datetime64::millis::serialize"
    )]
    period: DateTime<Utc>,
    project: String,
    #[serde(serialize_with = "allium_serde::serialize_volume_usd")]
    volume_usd: Option<f64>,
    recipient: u64,
}

#[derive(Serialize, Deserialize)]
pub struct AsyncRunResponse {
    #[serde(rename = "run_id")]
    run_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct StatusResponse {
    run_id: String,
    query_id: String,
    status: String,
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    name: String,
    data_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct Meta {
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize)]
pub struct ResultResponse {
    sql: String,
    data: Vec<DexVolume>,
    meta: Meta,
    queried_at: DateTime<Utc>,
}

pub struct AlliumClient {
    base_url: &'static str,
    client: Client,
    dex_volume_query_id: String,
}

impl AlliumClient {
    pub fn new(
        base_url: &'static str,
        api_key: String,
        dex_volume_query_id: String,
    ) -> eyre::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-API-Key", api_key.parse()?);
        headers.insert("Content-Type", "application/json".parse()?);
        let client = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            base_url,
            client,
            dex_volume_query_id,
        })
    }

    pub fn from_config(config: &AlliumConfig) -> eyre::Result<Self> {
        Self::new(
            "https://api.allium.so",
            config
                .api_key
                .clone()
                .ok_or(eyre::eyre!("Allium API key is required"))?,
            config
                .dex_volume_query_id
                .clone()
                .ok_or(eyre::eyre!("Allium query ID is required"))?,
        )
    }

    pub async fn query_dex_volumes(&self, limit: Option<usize>) -> eyre::Result<Vec<DexVolume>> {
        let base_url = &self.base_url;
        let query_id = &self.dex_volume_query_id;
        let url = format!("{base_url}/api/v1/explorer/queries/{query_id}/run-async");
        let response = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "parameters": {},
                "run_config": {
                  "limit": limit
                }
            }))
            .send()
            .await?;
        let run_id = response.json::<AsyncRunResponse>().await?.run_id;

        // Poll for completion
        loop {
            let status_url = format!("{base_url}/api/v1/explorer/query-runs/{run_id}");
            let response = self.client.get(status_url).send().await?;
            let status = response.json::<StatusResponse>().await?;
            if status.status == "success" {
                tracing::trace!(?run_id, "Query completed");
                break;
            }
            tracing::trace!(?run_id, "Waiting for query to complete...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        // Get results
        let results_url = format!("{base_url}/api/v1/explorer/query-runs/{run_id}/results");
        let response = self.client.get(results_url).send().await?;
        let result = response.json::<ResultResponse>().await?;
        Ok(result.data)
    }
}

pub async fn backfill_dex_volumes(
    api_key: String,
    query_id: String,
    limit: Option<usize>,
) -> eyre::Result<()> {
    let client = AlliumClient::new("https://api.allium.so", api_key, query_id)?;
    tracing::info!("Querying dex volumes. Limit: {:?}", limit);
    let volumes = client.query_dex_volumes(limit).await?;
    tracing::info!(
        "Writing dex volumes to ClickHouse. Count: {:?}",
        volumes.len()
    );
    let clickhouse = ClickHouseService::new(ClickHouseConfig::from_env()?);
    clickhouse.write_dex_volumes(volumes.as_ref()).await?;
    Ok(())
}

pub async fn fetch_dex_volumes_task(
    config: AlliumConfig,
    msg_tx: mpsc::UnboundedSender<ClickhouseMessage>,
) -> eyre::Result<()> {
    let client = AlliumClient::from_config(&config)?;
    loop {
        let volumes = client.query_dex_volumes(None).await?;
        tracing::trace!("Fetched {} dex volumes", volumes.len());
        for volume in volumes {
            msg_tx.send(ClickhouseMessage::DexVolume(volume))?;
        }
        tracing::trace!("Sleeping for 1 hour");
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;

    #[test]
    fn test_dex_volume_deserialization() {
        let json_data = r#"{"period": "2025-06-28T06:00:00", "project": "odos", "volume_usd": 61561.348599726654, "recipient": 47}"#;
        let dex_volume: DexVolume = serde_json::from_str(json_data).unwrap();

        assert_eq!(dex_volume.project, "odos");
        assert_eq!(dex_volume.volume_usd, Some(61561.348599726654));
        assert_eq!(dex_volume.recipient, 47);

        // Verify the datetime was parsed correctly
        let expected_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            chrono::NaiveDateTime::parse_from_str("2025-06-28T06:00:00", "%Y-%m-%dT%H:%M:%S")
                .unwrap(),
            Utc,
        );
        assert_eq!(dex_volume.period, expected_dt);
    }

    #[test]
    fn test_dex_volume_serialization_with_none_volume() {
        use chrono::{DateTime, Utc};

        let dex_volume = DexVolume {
            period: DateTime::from_naive_utc_and_offset(
                chrono::NaiveDateTime::parse_from_str("2025-06-28T06:00:00", "%Y-%m-%dT%H:%M:%S")
                    .unwrap(),
                Utc,
            ),
            project: "test_project".to_string(),
            volume_usd: None,
            recipient: 123,
        };

        let json = serde_json::to_string(&dex_volume).unwrap();
        // Should serialize None volume_usd as 0.0
        assert!(json.contains("\"volume_usd\":0.0"));
    }

    #[test]
    fn test_dex_volume_serialization_with_some_volume() {
        use chrono::{DateTime, Utc};

        let dex_volume = DexVolume {
            period: DateTime::from_naive_utc_and_offset(
                chrono::NaiveDateTime::parse_from_str("2025-06-28T06:00:00", "%Y-%m-%dT%H:%M:%S")
                    .unwrap(),
                Utc,
            ),
            project: "test_project".to_string(),
            volume_usd: Some(1234.56),
            recipient: 123,
        };

        let json = serde_json::to_string(&dex_volume).unwrap();
        // Should serialize Some volume_usd as the actual value
        assert!(json.contains("\"volume_usd\":1234.56"));
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_dex_volumes() {
        dotenv().ok();
        let api_key = std::env::var("ALLIUM_API_KEY").unwrap();
        let dex_volume_query_id = std::env::var("ALLIUM_DEX_VOLUME_QUERY_ID").unwrap();
        let client =
            AlliumClient::new("https://api.allium.so", api_key, dex_volume_query_id).unwrap();
        let volumes = client.query_dex_volumes(Some(100)).await.unwrap();
        println!("{:?}", volumes);
        assert!(!volumes.is_empty());
        assert_eq!(volumes.len(), 100);
    }
}
