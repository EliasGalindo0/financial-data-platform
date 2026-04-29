use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub database_url: String,
    pub kafka_brokers: String,
    pub kafka_group_id: String,
    pub kafka_topic_transactions: String,
    #[allow(dead_code)]
    pub kafka_topic_dlq: String,
    pub db_max_connections: u32,
    pub worker_concurrency: usize,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub log_level: String,
    pub worker_id_prefix: String,
}

impl WorkerConfig {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::Environment::with_prefix("WORKER").separator("__"))
            .set_default("db_max_connections", 10u32)?
            .set_default("worker_concurrency", 4u64)?
            .set_default("max_retries", 5u32)?
            .set_default("retry_backoff_ms", 1000u64)?
            .set_default("log_level", "info")?
            .set_default("worker_id_prefix", "worker")?
            .set_default("kafka_topic_transactions", "financial.transactions.v1")?
            .set_default("kafka_topic_dlq", "financial.transactions.dlq")?
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
