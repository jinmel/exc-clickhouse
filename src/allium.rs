use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use clickhouse::Row;
use crate::clickhouse::{ClickHouseConfig, ClickHouseService};

fn deserialize_period<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    // Parse naive datetime and assume UTC
    let naive = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S")
        .map_err(serde::de::Error::custom)?;
    Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
}

#[derive(Serialize, Deserialize, Row)]
pub struct DexVolume {
    #[serde(deserialize_with = "deserialize_period", serialize_with = "clickhouse::serde::chrono::datetime64::millis::serialize")]
    period: DateTime<Utc>,
    project: String,
    volume_usd: f64,
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
    base_url: String,
    client: Client,
    query_id: String,
}

impl AlliumClient {
    pub fn new(base_url: String, api_key: String, query_id: String) -> eyre::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-API-Key", api_key.parse()?);
        headers.insert("Content-Type", "application/json".parse()?);
        let client = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            base_url,
            client,
            query_id,
        })
    }

    pub async fn query_dex_volumes(&self, limit: Option<usize>) -> eyre::Result<Vec<DexVolume>> {
        let base_url = &self.base_url;
        let query_id = &self.query_id;
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
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }

        // Get results
        let results_url = format!("{base_url}/api/v1/explorer/query-runs/{run_id}/results");
        let response = self.client.get(results_url).send().await?;
        let result = response.json::<ResultResponse>().await?;
        Ok(result.data)
    }
}


pub async fn backfill_dex_volumes(limit: Option<usize>) -> eyre::Result<()> {
    let client = AlliumClient::new(
        "https://api.allium.so".to_string(),
        "r75AuiiUUM_W1F_WiyUpYlSUjlS9tf84wxhn5ndJ7zkBNSXwWp8Z_KPJMS2tuqzJYtqXVTNlpSONfXqh3jjL0A".to_string(),
        "88FDvtgyUzthWmr4ZCM4".to_string(),
    )?;
    tracing::info!("Querying dex volumes. Limit: {:?}", limit);
    let volumes = client.query_dex_volumes(limit).await?;
    tracing::info!("Writing dex volumes to ClickHouse. Count: {:?}", volumes.len());
    let clickhouse = ClickHouseService::new(ClickHouseConfig::from_env()?);
    clickhouse.write_dex_volumes(volumes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dex_volume_deserialization() {
        let json_data = r#"{"period": "2025-06-28T06:00:00", "project": "odos", "volume_usd": 61561.348599726654, "recipient": 47}"#;
        let dex_volume: DexVolume = serde_json::from_str(json_data).unwrap();

        assert_eq!(dex_volume.project, "odos");
        assert_eq!(dex_volume.volume_usd, 61561.348599726654);
        assert_eq!(dex_volume.recipient, 47);

        // Verify the datetime was parsed correctly
        let expected_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            chrono::NaiveDateTime::parse_from_str("2025-06-28T06:00:00", "%Y-%m-%dT%H:%M:%S")
                .unwrap(),
            Utc,
        );
        assert_eq!(dex_volume.period, expected_dt);
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_dex_volumes() {
        let client = AlliumClient::new(
            "https://api.allium.so".to_string(),
            "r75AuiiUUM_W1F_WiyUpYlSUjlS9tf84wxhn5ndJ7zkBNSXwWp8Z_KPJMS2tuqzJYtqXVTNlpSONfXqh3jjL0A".to_string(),
            "88FDvtgyUzthWmr4ZCM4".to_string(),
        ).unwrap();

        let volumes = client.query_dex_volumes(Some(100)).await.unwrap();
        assert!(!volumes.is_empty());
        assert_eq!(volumes.len(), 100);
    }
}
