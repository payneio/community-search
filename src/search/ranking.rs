use crate::db::ranking_config::RankingConfig;

// ── Types ─────────────────────────────────────────────────────────────────────

pub struct ScoreInputs<'a> {
    pub base_relevance: f32,
    pub source: &'a str,
    pub domain: &'a str,
    pub doc_timestamp_secs: i64,
    pub now_secs: i64,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Compute the final ranking score for a document.
///
/// `final_score = base_relevance * source_weight * freshness_factor * domain_boost`
pub fn score(inputs: &ScoreInputs, cfg: &RankingConfig) -> f32 {
    let source_weight = cfg
        .source_weights
        .get(inputs.source)
        .copied()
        .unwrap_or(1.0) as f32;

    let domain_boost = cfg.domain_boosts.get(inputs.domain).copied().unwrap_or(1.0) as f32;

    let freshness = freshness_factor(
        inputs.doc_timestamp_secs,
        inputs.now_secs,
        cfg.freshness_half_life_days,
    );

    inputs.base_relevance * source_weight * freshness * domain_boost
}

/// Compute the freshness factor using exponential half-life decay.
///
/// Returns `0.5 ^ (age_days / half_life_days)`.
/// Future timestamps (doc_ts > now) are clamped to 1.0.
/// When `half_life_days <= 0` the factor is always 1.0 (no decay).
pub fn freshness_factor(doc_ts: i64, now: i64, half_life_days: f64) -> f32 {
    if half_life_days <= 0.0 {
        return 1.0;
    }

    let age_secs = (now - doc_ts).max(0) as f64;
    let age_days = age_secs / 86_400.0;

    0.5_f64.powf(age_days / half_life_days) as f32
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn cfg_no_decay() -> RankingConfig {
        RankingConfig {
            collection_id: 0,
            freshness_half_life_days: 0.0,
            source_weights: HashMap::new(),
            domain_boosts: HashMap::new(),
        }
    }

    fn cfg_with_half_life(hl: f64) -> RankingConfig {
        RankingConfig {
            collection_id: 0,
            freshness_half_life_days: hl,
            source_weights: HashMap::new(),
            domain_boosts: HashMap::new(),
        }
    }

    #[test]
    fn freshness_is_one_for_now() {
        let factor = freshness_factor(1_000, 1_000, 90.0);
        assert!((factor - 1.0).abs() < 1e-6, "expected 1.0, got {factor}");
    }

    #[test]
    fn freshness_is_half_at_half_life() {
        // doc is exactly one half-life old
        let now = 90 * 86_400_i64;
        let factor = freshness_factor(0, now, 90.0);
        assert!((factor - 0.5).abs() < 1e-6, "expected 0.5, got {factor}");
    }

    #[test]
    fn freshness_is_quarter_at_two_half_lives() {
        let now = 180 * 86_400_i64;
        let factor = freshness_factor(0, now, 90.0);
        assert!((factor - 0.25).abs() < 1e-6, "expected 0.25, got {factor}");
    }

    #[test]
    fn future_timestamps_dont_explode() {
        // doc_ts > now → age clamped to 0 → factor must be 1.0
        let factor = freshness_factor(9_999_999, 1_000, 90.0);
        assert!(
            (factor - 1.0).abs() < 1e-6,
            "expected 1.0 for future doc, got {factor}"
        );
    }

    #[test]
    fn unknown_source_uses_weight_one() {
        let cfg = cfg_no_decay();
        let inputs = ScoreInputs {
            base_relevance: 3.7,
            source: "unknown-source",
            domain: "example.com",
            doc_timestamp_secs: 0,
            now_secs: 0,
        };
        let result = score(&inputs, &cfg);
        assert!(
            (result - 3.7).abs() < 1e-5,
            "expected score == base_relevance (3.7), got {result}"
        );
    }

    #[test]
    fn domain_boost_multiplies_score() {
        let mut cfg = cfg_no_decay();
        cfg.domain_boosts.insert("trusted.org".to_string(), 3.0);

        let inputs = ScoreInputs {
            base_relevance: 2.0,
            source: "local",
            domain: "trusted.org",
            doc_timestamp_secs: 0,
            now_secs: 0,
        };
        let result = score(&inputs, &cfg);
        // 2.0 * 1.0 (src) * 1.0 (freshness, no decay) * 3.0 = 6.0
        assert!((result - 6.0).abs() < 1e-5, "expected 6.0, got {result}");
    }

    #[test]
    fn full_formula_composes() {
        // final = 4.0 * source(0.5) * freshness(0.5 at half-life) * domain(2.0) = 2.0
        let mut cfg = cfg_with_half_life(90.0);
        cfg.source_weights.insert("peer".to_string(), 0.5);
        cfg.domain_boosts.insert("example.com".to_string(), 2.0);

        let now = 90 * 86_400_i64; // exactly one half-life
        let inputs = ScoreInputs {
            base_relevance: 4.0,
            source: "peer",
            domain: "example.com",
            doc_timestamp_secs: 0,
            now_secs: now,
        };
        let result = score(&inputs, &cfg);
        assert!((result - 2.0).abs() < 1e-5, "expected 2.0, got {result}");
    }
}
