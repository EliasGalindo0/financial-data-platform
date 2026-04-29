/// Fraud / risk scoring — pluggable rules engine.
///
/// In production this would call an ML model service or a rules engine
/// (e.g., Stripe Radar, custom ONNX model).
/// Here we show the integration point with a simple rule set.
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;
use std::time::Duration;

pub struct FraudResult {
    pub is_blocked: bool,
    pub reason: String,
    pub risk_score: Decimal, // 0.0000 – 1.0000
}

pub struct FraudChecker;

impl FraudChecker {
    pub async fn check(transaction_id: Uuid, amount: Decimal) -> FraudResult {
        if shared::fault::should_fail("fraud.hang", Some(transaction_id)) {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        // Rule 1: Block transactions over $100,000 without manual review
        if amount > dec!(100_000) {
            return FraudResult {
                is_blocked: true,
                reason: "amount_exceeds_auto_approve_limit".into(),
                risk_score: dec!(0.9500),
            };
        }

        // Rule 2: Round-number large transactions are higher risk
        let risk_score = if amount > dec!(10_000) {
            dec!(0.4000)
        } else if amount > dec!(1_000) {
            dec!(0.1500)
        } else {
            dec!(0.0100)
        };

        FraudResult {
            is_blocked: false,
            reason: String::new(),
            risk_score,
        }
    }
}
