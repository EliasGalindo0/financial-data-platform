use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RelayConfig {
    pub database_url: String,
    pub kafka_brokers: String,
    pub batch_size: i64,
    pub poll_interval_ms: u64,
    pub log_level: String,
}

impl RelayConfig {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("RELAY").separator("__"))
            .set_default("batch_size", 100i64)?
            .set_default("poll_interval_ms", 200u64)?
            .set_default("log_level", "info")?
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
